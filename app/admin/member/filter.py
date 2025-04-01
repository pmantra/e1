import datetime

from flask_admin.contrib.sqla import filters as sqla_filters

import sqlalchemy
from db.sqlalchemy.models.member_versioned import MemberVersioned


class ActiveE9yRecordFilter(sqla_filters.BaseSQLAFilter):
    """Filter results by active/inactive E9y records"""

    def apply(self, query, value, alias=None):
        if value != "false":
            return query.filter(
                MemberVersioned.effective_range.contains(datetime.date.today())
            )
        else:
            return query.filter(
                sqlalchemy.not_(
                    MemberVersioned.effective_range.contains(datetime.date.today())
                )
            )

    # readable operation name. This appears in the middle filter line drop-down
    def operation(self):
        return "="
