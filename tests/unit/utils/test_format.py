from typing import Any

import pytest

from app.utils.format import sanitize_json_input


class TestFormat:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="input,expected",
        argvalues=[
            ("not-a-json-but-just-output-it", "not-a-json-but-just-output-it"),
            (set([1, 2, 3]), b"[1,2,3]"),
            (
                {"One": 1, "Two": 2, "Three": "triple"},
                b'{"One":1,"Two":2,"Three":"triple"}',
            ),
        ],
        ids=["plain_string", "set", "any_object"],
    )
    def test_sanitize_json_input(input: Any, expected: str):
        # When
        output = sanitize_json_input(input)

        # Then:
        assert output == expected
