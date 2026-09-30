"""D2 capture contracts; no warehouse clients or side effects."""
import re
from stable_award_ids import sha

REUSED = ('crossref','datacite','crossref_records','datacite_records','funder_map','normalization')


def described_body(rows):
    """Extract the Body field, preserving SQL internals and one final newline.

    DESCRIBE output has presentation padding; the operator's .body.sql files
    contain the field value, stripped of that padding, with one final newline.
    Unknown layouts fail closed instead of hashing an entire description.
    """
    bodies=[]
    for row in rows:
        values=list(row)
        if len(values)==2 and str(values[0]).strip().rstrip(':').lower()=='body':
            bodies.append(str(values[1]))
        elif len(values)==1:
            match=re.fullmatch(r'\s*Body:\s*(.*)',str(values[0]),re.S|re.I)
            if match:bodies.append(match.group(1))
    if len(bodies)!=1:raise ValueError('DESCRIBE_BODY_LAYOUT_UNVERIFIED')
    return bodies[0].strip()+'\n'


def verify_body(rows, expected):
    body=described_body(rows)
    if sha(body)!=expected:raise ValueError('LIVE_UDF_BODY_CHANGED')
    return body


def resolved_version(value, latest):
    if value=='current':return int(latest)
    if type(value) is not int or value<0:raise ValueError('UNPINNED_CAPTURE_INPUT')
    return value
