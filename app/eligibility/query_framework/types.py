"""
Common type definitions for eligibility functionality.
"""

from typing import List, Type, Union

from db import model

# Base member type (either version)
MemberType = Union[model.MemberVersioned, model.Member2]
SingleRecordType = Union[model.MemberVersioned, model.Member2]
MultipleRecordType = Union[List[model.MemberVersioned], List[model.Member2]]

# Member result type (either single member or list)
MemberResult = Union[MemberType, List[MemberType]]

# For type checking with expected_type parameter
MemberVersionedResult = Union[model.MemberVersioned, List[model.MemberVersioned]]
Member2Result = Union[model.Member2, List[model.Member2]]
MemberResultType = Union[Type[SingleRecordType], Type[MultipleRecordType]]

# For function return types
MemberResponseType = Union[model.MemberResponse, List[model.MemberResponse]]
