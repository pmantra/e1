"""Tests for the eligibility query type definitions."""

from typing import Union, get_args, get_origin

from app.eligibility.query_framework.types import (
    Member2Result,
    MemberResponseType,
    MemberResult,
    MemberResultType,
    MemberType,
    MemberVersionedResult,
    MultipleRecordType,
    SingleRecordType,
)
from db import model


class TestTypeDefinitions:
    """Test that type aliases are defined and available."""

    def test_type_aliases_exist(self):
        """Test that all expected type aliases are defined."""
        # Simply check that the types are defined and importable
        assert MemberType is not None
        assert SingleRecordType is not None
        assert MultipleRecordType is not None
        assert MemberResult is not None
        assert MemberResultType is not None
        assert MemberVersionedResult is not None
        assert Member2Result is not None
        assert MemberResponseType is not None

    def test_member_response_type(self):
        """Test basic structure of MemberResponseType."""

        assert get_origin(MemberResponseType) is Union
        args = get_args(MemberResponseType)
        assert len(args) == 2
        assert model.MemberResponse in args
        assert get_origin(args[1]) is list
        assert get_args(args[1])[0] == model.MemberResponse
