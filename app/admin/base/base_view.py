import flask_admin
from flask_admin import model
from flask_admin.contrib import sqla


class ModelViewWithSessionRollback(sqla.ModelView):
    def handle_exception(self, e):
        self.session.rollback()
        return super().handle_exception(e)

    def handle_user_exception(self, e):
        self.session.rollback()
        return super().handle_user_exception(e)

    def handle_view_exception(self, e):
        self.session.rollback()
        return super().handle_view_exception(e)


class BaseModelViewWithSessionRollback(model.BaseModelView):
    def handle_exception(self, e):
        self.session.rollback()
        return super().handle_exception(e)

    def handle_user_exception(self, e):
        self.session.rollback()
        return super().handle_user_exception(e)

    def handle_view_exception(self, e):
        self.session.rollback()
        return super().handle_view_exception(e)


class BaseViewWithSessionRollback(flask_admin.BaseView):
    def handle_exception(self, e):
        self.session.rollback()
        return super().handle_exception(e)

    def handle_user_exception(self, e):
        self.session.rollback()
        return super().handle_user_exception(e)

    def handle_view_exception(self, e):
        self.session.rollback()
        return super().handle_view_exception(e)
