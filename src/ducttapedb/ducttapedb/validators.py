from typing import Any


def validate_id(id: int):
    """Validate that the ID is a positive integer."""
    if not isinstance(id, int) or id <= 0:
        raise ValueError("ID must be a positive integer.")


def validate_document(document: dict[Any, Any]):
    """Validate that the document is properly structured.

    Args:
        document (dict[Any, Any]): The document to validate.

    Raises:
        ValueError: If the document is not valid.
    """
    if not isinstance(document, dict) or not document:
        raise ValueError("Document must be a non-empty dictionary.")

    # Check if an ID is present and valid
    if "id" in document:
        if not isinstance(document["id"], int):
            raise ValueError("Document 'id' must be an integer if provided.")
        if "data" not in document:
            raise ValueError("Documents with an 'id' must also include a 'data' field.")
        if "id" in document["data"]:
            raise ValueError("The 'data' field must not contain an 'id' key.")

    # For documents without an ID, no extra validation is needed


def validate_key_value(key: str, value: Any):
    """Validate key-value pair for search operations."""
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Key must be a non-empty string.")
    if value is None:
        raise ValueError("Value cannot be None.")
