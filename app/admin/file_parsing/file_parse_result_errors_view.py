from flask_admin.contrib.sqla import ModelView


class FileParseErrorView(ModelView):
    can_create = False
    can_edit = False
    can_view_details = True
    can_delete = False
    can_export = True
    can_set_page_size = True
    page_size = 50
    column_filters = ("organization_id", "file_id")


class FileParseResultsView(ModelView):
    can_create = False
    can_edit = False
    can_view_details = True
    can_delete = False
    can_export = True
    can_set_page_size = True
    page_size = 50
    column_filters = (
        "organization_id",
        "file_id",
        "first_name",
        "last_name",
        "unique_corp_id",
    )
