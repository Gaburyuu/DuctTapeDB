from typing import Any


def validate_id(id: int):
    """Validate that the ID is a positive integer."""
    if not isinstance(id, int) or id <= 0:
        raise ValueError("ID must be a positive integer.")


def validate_document(document: dict[Any, Any]):
    """Validate that the document is a non-empty dictionary."""
    if not isinstance(document, dict) or not document:
        raise ValueError("Document must be a non-empty dictionary.")
    if "id" not in document:
        raise ValueError("Document must contain an 'id' key.")


def validate_key_value(key: str, value: Any):
    """Validate key-value pair for search operations."""
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Key must be a non-empty string.")
    if value is None:
        raise ValueError("Value cannot be None.")
