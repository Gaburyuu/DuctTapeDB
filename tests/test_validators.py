import pytest
from src import validators

validate_document = validators.validate_document


def test_validate_document_with_id_in_data():
    """Test that a document with 'id' in both the document and 'data' raises a ValueError."""
    doc = {"id": 1, "data": {"id": 2, "name": "Dragon"}}

    with pytest.raises(
        ValueError, match="The 'data' field must not contain an 'id' key."
    ):
        validate_document(doc)


def test_validate_document_missing_data_with_id():
    """Test that a document with an 'id' but missing 'data' raises a ValueError."""
    doc = {"id": 1}

    with pytest.raises(
        ValueError, match="Documents with an 'id' must also include a 'data' field."
    ):
        validate_document(doc)


def test_validate_document_non_integer_id():
    """Test that a document with a non-integer 'id' raises a ValueError."""
    doc = {"id": "string_id", "data": {"name": "Slime"}}

    with pytest.raises(
        ValueError, match="Document 'id' must be an integer if provided."
    ):
        validate_document(doc)


def test_validate_document_empty_document():
    """Test that an empty document raises a ValueError."""
    doc = {}

    with pytest.raises(ValueError, match="Document must be a non-empty dictionary."):
        validate_document(doc)


def test_validate_document_success_no_id():
    """Test that a valid document without an 'id' passes validation."""
    doc = {"name": "Metal Slime", "level": 10}

    # No exception is raised for a valid document
    validate_document(doc)


def test_validate_document_success_with_id():
    """Test that a valid document with an 'id' passes validation."""
    doc = {"id": 1, "data": {"name": "Slime", "level": 5}}

    # No exception is raised for a valid document
    validate_document(doc)
