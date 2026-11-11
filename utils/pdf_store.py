"""One stored PDF per distinct bytes (oxjob #2023).

Every writer to R2 `openalex-pdfs` (the TaxiCab harvester, the arXiv and PMC sweeps) claims the file's MD5 in the
DynamoDB table `pdf-md5` (md5 -> file id) before storing. When we already hold those bytes and the file is still in
the bucket, the writer reuses that file's id and stores nothing; Parse PDFs then links the reused file to the new link
(`link_reused_files`). The harvester has its own copy of this rule (taxicab `harvest.py`).

A writer that cannot reach the table stores the file as before: a DynamoDB outage never stops a harvest.
"""
import datetime
import hashlib
import uuid

BUCKET = "openalex-pdfs"
TABLE = "pdf-md5"
# a claim this recent whose file is not in the bucket yet is another writer's upload in flight, not a lost file
IN_FLIGHT = datetime.timedelta(minutes=10)


def md5_table(dbutils, scope="pdf-md5"):
    """The pdf-md5 table with the scope's AWS key, or None when the scope is missing (store as before)."""
    import boto3
    try:
        key_id = dbutils.secrets.get(scope, "aws_access_key_id")
        secret = dbutils.secrets.get(scope, "aws_secret_access_key")
    except Exception as e:
        print(f"pdf-md5: no key in secret scope {scope!r} ({type(e).__name__}); storing every PDF as before")
        return None
    return boto3.resource("dynamodb", region_name="us-east-1", aws_access_key_id=key_id,
                          aws_secret_access_key=secret).Table(TABLE)


def store_pdf(r2, table, body, metadata):
    """Store `body` as openalex-pdfs/<id>.pdf unless we already hold the same bytes.

    Returns (id, reused). `metadata` is the object metadata without `id`; `table` is md5_table() (None: no check).
    """
    pdf_id = str(uuid.uuid4())
    if table is not None:
        try:
            held_id = _claim(table, r2, hashlib.md5(body).hexdigest(), pdf_id)
            if held_id:
                return held_id, True
        except Exception as e:
            print(f"pdf-md5 lookup failed, storing {pdf_id} as before: {type(e).__name__}: {e}")
    r2.put_object(Bucket=BUCKET, Key=f"{pdf_id}.pdf", Body=body, Metadata={**metadata, "id": pdf_id})
    return pdf_id, False


def _claim(table, r2, md5, pdf_id):
    """Claim `md5` for `pdf_id`. Returns the id of the file we already hold with these bytes, or None: store pdf_id."""
    now = datetime.datetime.now(datetime.timezone.utc)
    item = {"md5": md5, "id": pdf_id, "created_date": now.isoformat()}
    conflict = table.meta.client.exceptions.ConditionalCheckFailedException
    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(md5)",
                       ReturnValuesOnConditionCheckFailure="ALL_OLD")
        return None
    except conflict as e:
        held = e.response.get("Item", {})
    held_id = held.get("id", {}).get("S")
    if held_id and _exists(r2, held_id):
        return held_id
    held_at = held.get("created_date", {}).get("S")
    if held_at and now - datetime.datetime.fromisoformat(held_at) < IN_FLIGHT:
        return None  # another writer is uploading these bytes right now: keep its claim, store our own copy
    try:  # the held file left the bucket (moved out, or its upload failed): this one takes its place
        table.put_item(Item=item, ConditionExpression="id = :held", ExpressionAttributeValues={":held": held_id})
    except conflict:
        pass  # another writer took it over first
    return None


def _exists(r2, pdf_id):
    try:
        r2.head_object(Bucket=BUCKET, Key=f"{pdf_id}.pdf")
        return True
    except r2.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
