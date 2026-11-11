"""Tests for utils/pdf_store.py: one stored PDF per distinct bytes (oxjob #2023)."""
import datetime
import os
import sys

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from utils import pdf_store  # noqa: E402

DDB = boto3.client("dynamodb", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x")
PDF = b"%PDF-1.7 " + b"x" * 200


class FakeR2:
    exceptions = type("E", (), {"ClientError": ClientError})

    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, Metadata):
        self.objects[Key] = (Body, Metadata)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}


class FakeTable:
    meta = type("M", (), {"client": DDB})

    def __init__(self, fail=False):
        self.items, self.fail = {}, fail

    def put_item(self, Item, ConditionExpression, ExpressionAttributeValues=None, **kw):
        if self.fail:
            raise RuntimeError("dynamodb down")
        held = self.items.get(Item["md5"])
        ok = held is None if "attribute_not_exists" in ConditionExpression else (
            held is not None and held["id"] == ExpressionAttributeValues[":held"])
        if not ok:
            resp = {"Error": {"Code": "ConditionalCheckFailedException"}}
            if held:
                resp["Item"] = {k: {"S": v} for k, v in held.items()}
            raise DDB.exceptions.ConditionalCheckFailedException(resp, "PutItem")
        self.items[Item["md5"]] = Item


def test_new_bytes_are_stored_and_claimed():
    r2, t = FakeR2(), FakeTable()
    pdf_id, reused = pdf_store.store_pdf(r2, t, PDF, {"url": "a"})
    assert not reused and f"{pdf_id}.pdf" in r2.objects
    assert [i["id"] for i in t.items.values()] == [pdf_id]
    assert r2.objects[f"{pdf_id}.pdf"][1]["id"] == pdf_id


def test_same_bytes_reuse_the_file_and_store_nothing():
    r2, t = FakeR2(), FakeTable()
    first, _ = pdf_store.store_pdf(r2, t, PDF, {"url": "a"})
    again, reused = pdf_store.store_pdf(r2, t, PDF, {"url": "b"})
    assert reused and again == first and len(r2.objects) == 1


def test_new_bytes_from_the_same_link_are_a_new_file():
    r2, t = FakeR2(), FakeTable()
    first, _ = pdf_store.store_pdf(r2, t, PDF, {"url": "a"})
    second, reused = pdf_store.store_pdf(r2, t, PDF + b"v2", {"url": "a"})
    assert not reused and second != first and len(r2.objects) == 2


def test_file_moved_out_is_replaced():
    r2, t = FakeR2(), FakeTable()
    first, _ = pdf_store.store_pdf(r2, t, PDF, {"url": "a"})
    del r2.objects[f"{first}.pdf"]
    old = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=40)).isoformat()
    next(iter(t.items.values()))["created_date"] = old
    second, reused = pdf_store.store_pdf(r2, t, PDF, {"url": "a"})
    assert not reused and second != first and f"{second}.pdf" in r2.objects
    assert next(iter(t.items.values()))["id"] == second


def test_upload_in_flight_keeps_its_claim():
    r2, t = FakeR2(), FakeTable()
    first, _ = pdf_store.store_pdf(r2, t, PDF, {"url": "a"})
    del r2.objects[f"{first}.pdf"]  # claimed a moment ago, upload not finished
    second, reused = pdf_store.store_pdf(r2, t, PDF, {"url": "b"})
    assert not reused and second != first
    assert next(iter(t.items.values()))["id"] == first


def test_no_table_or_table_down_stores_as_before():
    for t in (None, FakeTable(fail=True)):
        r2 = FakeR2()
        pdf_id, reused = pdf_store.store_pdf(r2, t, PDF, {"url": "a"})
        assert not reused and f"{pdf_id}.pdf" in r2.objects
