"""SQL normalization and fingerprinting."""

import hashlib
import re

import sqlparse
from sqlparse import tokens as T


def normalize_sql(query: str) -> str:
    """Normalize SQL query for fingerprinting.

    Normalization steps:
    1. Parse with sqlparse
    2. Remove comments
    3. Normalize whitespace
    4. Replace string literals with placeholder
    5. Replace numeric literals with placeholder
    6. Lowercase keywords

    The result is a canonical form that groups equivalent queries together.
    """
    # Parse the SQL
    parsed = sqlparse.parse(query)
    if not parsed:
        return query.strip()

    statement = parsed[0]

    # Process tokens
    normalized_tokens = []
    for token in statement.flatten():
        # Skip comments
        if token.ttype in (T.Comment.Single, T.Comment.Multiline):
            continue

        # Skip whitespace (we'll add our own)
        if token.ttype in (T.Whitespace, T.Newline):
            continue

        # Replace string literals
        if token.ttype in (T.String.Single, T.String.Symbol, T.Literal.String.Single):
            normalized_tokens.append("?")
            continue

        # Replace numeric literals
        if token.ttype in (T.Number.Integer, T.Number.Float, T.Literal.Number.Integer, T.Literal.Number.Float):
            normalized_tokens.append("?")
            continue

        # Lowercase keywords
        if token.ttype in (T.Keyword, T.Keyword.DML, T.Keyword.DDL):
            normalized_tokens.append(token.value.upper())
            continue

        # Keep everything else as-is
        normalized_tokens.append(token.value)

    # Join with single spaces
    result = " ".join(normalized_tokens)

    # Clean up extra spaces around punctuation
    result = re.sub(r"\s*([(),])\s*", r"\1 ", result)
    result = re.sub(r"\s+", " ", result)

    return result.strip()


def fingerprint_sql(query: str) -> str:
    """Generate a fingerprint (hash) for a SQL query.

    Returns a SHA256 hex digest of the normalized query.
    Queries that are logically equivalent will have the same fingerprint.
    """
    normalized = normalize_sql(query)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
