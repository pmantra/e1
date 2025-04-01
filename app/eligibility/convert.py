"""Eligibility Converters

These functions are specialized to operate on a single value within an Eligibility Record.
"""
from __future__ import annotations

import functools
import re
from datetime import date, datetime
from typing import Optional, Union

import pendulum
import pycountry

DATE_UNKNOWN = pendulum.date(1, 1, 1)
DEFAULT_DATE_OF_BIRTH = pendulum.date(1900, 1, 1)
# Regex based on RFC 3696
EMAIL_PATTERN = re.compile(
    r"(?=^.{1,64}@)^(?P<username>[\w!#$%&'*+\/=?`{|}~^-]+(?:\.[\w!#$%&'*+\/=?`{|}~^-]+)*)(?=@.{4,255}$)@(?P<domain>(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,6})$"
)

# https://www.regextutorial.org/regex-for-numbers-and-ranges.php
# Matches (M)M[-, ,/,.](D)D[-, ,/,.](YY)YY
# E.g.: 1/01/88, 01/1/1988, 1/01/88, 01/01/88, 1/1/1988...
_COMMON_DATE_PATTERN = re.compile(
    r"^"
    # Any number between 1-12, with an optional `0` for single-digit.
    r"(?P<month>0?[1-9]|1[0-2])[\s/.-]"
    # Any number between 1-31, with an optional `0` for single-digit.
    r"(?P<day>0?[1-9]|[12][0-9]|3[01])[\s/.-]"
    # Any 2 or 4 digit number
    r"(?P<year>((?:\d{2}){1,2}))"
    # Ignore the time portion if it's there.
    r".*"
    r"$",
)
_max_year = pendulum.today().year
_century = _max_year - (_max_year % 100)
_tens = _max_year - _century
_last_century = _century - 100


@functools.lru_cache(maxsize=500_000)
def to_date(
    o: Union[str, date, datetime],
    *,
    parse=pendulum.parse,
    default=DATE_UNKNOWN,
    # Byte-code hacks to avoid global lookups.
    # Only necessary for hot loops.
    __date=date,
    __pdate=pendulum.date,
    __datetime=datetime,
    __pattern=_COMMON_DATE_PATTERN,
    __century=_century,
    __last_century=_last_century,
    __tens=_tens,
) -> date | None:
    otype = o.__class__
    if issubclass(otype, __datetime):
        return o.date()
    if issubclass(otype, __date):
        return o
    try:
        # Yay! We have a known format. Skip our fallback.
        if match := __pattern.match(o):
            group = match.group
            year = int(group("year"))

            # Test to see if our value is the default (case where date is 0001-01-01 representing unknown date)
            if (
                __pdate(
                    month=int(group("month")),
                    day=int(group("day")),
                    year=year,
                )
                == DATE_UNKNOWN
                and group("year") == "0001"
            ):
                return None

            # Handle a two-digit year - try to select the correct century.
            if year < 100:
                century = __last_century if year > __tens else __century
                year += century
            # Return a pendulum.Date instance.
            return __pdate(
                month=int(group("month")),
                day=int(group("day")),
                year=year,
            )
        # Use a more relaxed parser if we have to.
        parsed = parse(o, strict=False, exact=True)

        # Test to see if our value is the default (case where date is 0001-01-01 representing unknown date)
        if parsed == __pdate(1, 1, 1):
            return None

        # Make sure we're returning a date object.
        if isinstance(parsed, __datetime):
            return __pdate(parsed.year, parsed.month, parsed.day)
        return parsed
    except (ValueError, TypeError):
        return default


# NOTE: These converters aren't DRY, but we're optimizing for callstack and speed.


_BOOLS = frozenset((True, "true", 1, "1", "y", "yes"))


def to_bool(
    o: Union[int, str, bool],
    *,
    flags=_BOOLS,
    __str=str,
):
    return o in flags or __str(o).lower() in flags


_BENEFICIARY_FLAGS = (
    frozenset(
        {
            "dependents",
            "ee+children",
            "employee+child",
            "employee+child(ren)",
            "employee+child(ren)+domesticpartner",
            "employee+child(ren)+domesticpartner+dpchild(ren)",
            "employee+children",
            "employee+children+dpchildren",
            "employee+dependent",
            "employee+dependent(s)",
            "employee+dependents",
            "employee+domesticpartner",
            "employee+domesticpartner+children",
            "employee+spouse",
            "family",
            "you+child",
            "you+child(ren)",
            "you+children",
            "you+family",
            "you+spouse/dp",
        }
    )
    | _BOOLS
)


def to_beneficiaries_enabled(
    o: Union[int, str, bool],
    *,
    flags=_BENEFICIARY_FLAGS,
    __str=str,
) -> bool:
    return o in flags or __str(o).lower().replace(" ", "") in flags


_PREGNANCY_SEXES = frozenset({"female", "f", "fe"})


def to_can_get_pregnant(
    o: Union[int, str, bool],
    *,
    flags=_PREGNANCY_SEXES,
    __str=str,
) -> bool:
    return o in flags or __str(o).lower() in flags


STATE_UNKNOWN = "<unknown state>"
COUNTRY_DEFAULT = "USA"


@functools.lru_cache(maxsize=100_000)
def _get_country(
    *,
    __get=pycountry.countries.get,
    **kwargs: str,
) -> pycountry.db.Data:
    return __get(**kwargs)


@functools.lru_cache(maxsize=100_000)
def to_state_code(
    o: Optional[str],
    *,
    search=pycountry.subdivisions.lookup,
    default=STATE_UNKNOWN,
    country_code=COUNTRY_DEFAULT,
    _get_country=_get_country,
) -> str:
    """Search for a state within the provided country-code."""
    country = _get_country(alpha_3=country_code) or _get_country(
        alpha_3=COUNTRY_DEFAULT
    )
    if o is None or country is None:
        return default
    o = o.strip()
    if not o or len(o) < 2:
        return default
    if len(o) < 3:
        o = f"{country.alpha_2}-{o}"
    try:
        state = search(o)
        return state.code.rsplit("-", maxsplit=1)[-1]
    except LookupError:
        return default


COUNTRY_UNKNOWN = "<unknown country>"


@functools.lru_cache(maxsize=100_000)
def to_country_code(
    o: Optional[str],
    *,
    search=pycountry.countries.lookup,
    default=COUNTRY_UNKNOWN,
) -> str:
    """Search for a country code given a fuzzy string."""
    if o is None:
        return default
    o = o.strip()
    if not o or len(o) < 2:
        return default
    try:
        return search(o).alpha_3
    except LookupError:
        return default


def _warmup_caches():
    us = to_country_code("US")
    country = _get_country(alpha_3=us)
    to_country_code("USA")
    to_country_code("United States")
    to_country_code("United States of America")
    for subdiv in pycountry.subdivisions.get(country_code=country.alpha_2):
        to_state_code(subdiv.code)
        to_state_code(subdiv.code.split("-")[-1])
        to_state_code(subdiv.name)


_warmup_caches()
