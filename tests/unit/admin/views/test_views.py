from typing import Optional
from unittest import mock

import flask
import pytest

from app.admin.views import views


@pytest.fixture(autouse=True)
def reset_global_original_base_url():
    views._original_base_url = None


@pytest.mark.parametrize(
    argnames="referrer,expected_base_url",
    argvalues=[
        ("http://this.should.work/i/think/", "http://this.should.work"),
        ("https://thisshouldalsowork/and/why/not.html", "https://thisshouldalsowork"),
        ("ftp://this.should.not/work", None),
        (None, None),
    ],
    ids=["http", "https", "ftp-none", "none-none"],
)
def test_get_referrer_base_url(
    referrer: Optional[str], expected_base_url: Optional[str]
):
    test_flask = flask.Flask(__name__)
    test_flask.testing = True
    with test_flask.test_request_context(), mock.patch.object(
        views, "request"
    ) as mock_request:
        # Given
        mock_request.referrer = referrer
        # When
        returned_base_url = views.get_referrer_base_url()
        # Then
        assert returned_base_url == expected_base_url


def test_get_referrer_base_url_only_called_once():
    with mock.patch(
        "app.admin.views.views.get_referrer_base_url"
    ) as mock_get_referrer_base_url:
        # Given
        mock_get_referrer_base_url.return_value = "meh"
        # When
        views.add_original_base_url(url="bleh")
        views.add_original_base_url(url="heh")
        views.add_original_base_url(url="eh")
        # Then
        assert mock_get_referrer_base_url.call_count == 1


@pytest.mark.parametrize(
    argnames="input_url,referrer_base_url,expected_output_url",
    argvalues=[
        ("/i/think/", "http://this.should.work", "http://this.should.work/i/think/"),
        (
            "/and/again.js",
            "https://thisshouldalsowork",
            "https://thisshouldalsowork/and/again.js",
        ),
        ("/and/no/nones/", None, "/and/no/nones/"),
        ("http://all.set/thanks", "http://all.your.base", "http://all.set/thanks"),
        ("https://all.set/thanks", "http://all.your.base", "https://all.set/thanks"),
    ],
    ids=[
        "http",
        "https",
        "none-no_change",
        "http_all_set-no_change",
        "https_all_set-no_change",
    ],
)
def test_add_original_base_url(
    input_url: str, referrer_base_url: Optional[str], expected_output_url: str
):
    with mock.patch(
        "app.admin.views.views.get_referrer_base_url"
    ) as mock_get_referrer_base_url:
        # Given
        mock_get_referrer_base_url.return_value = referrer_base_url
        # When
        returned_url = views.add_original_base_url(url=input_url)
        # Then
        assert returned_url == expected_output_url
