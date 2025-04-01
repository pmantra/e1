from unittest import mock

import pytest
from werkzeug import exceptions

from bin.commands import admin
from sqlalchemy.exc import PendingRollbackError


def test_internal_error_handling_pending_rollback_error():
    error = PendingRollbackError()
    with pytest.raises(exceptions.InternalServerError), mock.patch(
        "bin.commands.admin.rollback_errored_sessions"
    ) as mock_rollback:
        admin.internal_error(error)

    mock_rollback.assert_called()
