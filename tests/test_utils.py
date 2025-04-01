import random
import string


def generate_random_string(
    length,
    include_lower_case_char=True,
    include_upper_case_char=True,
    include_digit=True,
) -> str:
    if length <= 0:
        raise ValueError("Invalid string length")

    if (
        not include_lower_case_char
        and not include_upper_case_char
        and not include_digit
    ):
        raise ValueError(
            "Cannot generate a random string because no characters allowed"
        )

    candidate_characters = ""
    if include_lower_case_char:
        candidate_characters = candidate_characters + string.ascii_lowercase
    if include_upper_case_char:
        candidate_characters = candidate_characters + string.ascii_uppercase
    if include_digit:
        candidate_characters = candidate_characters + string.digits

    return "".join(random.choices(candidate_characters, k=length))


def generate_random_int(length: int) -> int:
    return int(generate_random_string(length, False, False, True))
