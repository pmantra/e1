import contextlib
from dataclasses import fields
from typing import Type

import asyncpg

from app.admin.base import base_view, filter
from db.clients import client, postgres_connector
from db.flask import synchronize


class ServiceProtocolModelView(base_view.BaseModelViewWithSessionRollback):
    can_view_details = True
    can_edit = False
    can_delete = False
    can_create = False
    can_export = True
    can_set_page_size = True
    default_sort_field = None
    default_sort_desc = False

    def __init__(
        self,
        service_cls: Type[client.ServiceProtocol] = None,
        name=None,
        category=None,
        endpoint=None,
        url=None,
        static_folder=None,
        menu_class_name=None,
        menu_icon_type=None,
        menu_icon_value=None,
    ):
        self.service_cls = service_cls
        self.pk = service_cls().client.pk
        super().__init__(
            service_cls.model,
            name=name,
            category=category,
            endpoint=endpoint,
            url=url,
            static_folder=static_folder,
            menu_class_name=menu_class_name,
            menu_icon_type=menu_icon_type,
            menu_icon_value=menu_icon_value,
        )

    def get_pk_value(self, model):
        return getattr(model, self.pk)

    def scaffold_list_columns(self):
        return [f.name for f in fields(self.model)]

    def scaffold_sortable_columns(self):
        pass

    def scaffold_form(self):
        pass

    def scaffold_list_form(self, widget=None, validators=None):
        pass

    @contextlib.asynccontextmanager
    async def service(self):
        connectors = postgres_connector.cached_connectors()
        service = self.service_cls(connector=connectors["main"])
        await service.client.connector.initialize()
        yield service

    @synchronize
    async def get_list(
        self, page, sort_field, sort_desc, search, filters, page_size=None
    ):
        effective_sort_field, effective_sort_desc = sort_field, sort_desc
        if sort_field is None and self.default_sort_field is not None:
            effective_sort_field = self.default_sort_field
            effective_sort_desc = self.default_sort_desc
        kwargs = {}
        if filters and self._filters:
            flt: filter.NoOpFilter
            for i, name, value in filters:
                flt = self._filters[i]
                kwargs[name] = flt.clean(value)

        cursor: asyncpg.connection.cursor.Cursor
        pagesize = page_size or self.page_size
        s: client.ServiceProtocol
        async with self.service() as s:
            async with s.select_cursor(
                sort_field=effective_sort_field, sort_desc=effective_sort_desc, **kwargs
            ) as (total, cursor):
                if page and pagesize:
                    await cursor.forward(pagesize * page)
                rows = await cursor.fetch(pagesize)
            return total, rows

    @synchronize
    async def get_one(self, id):
        async with self.service() as s:
            return await s.get(int(id))

    @synchronize
    def create_model(self, form):
        ...

    @synchronize
    def update_model(self, form, model):
        ...

    @synchronize
    async def delete_model(self, model):
        async with self.service() as s:
            return await s.delete(self.get_pk_value(model))

    def _create_ajax_loader(self, name, options):
        pass

    def _validate_form_class(self, ruleset, form_class, remove_missing=True):
        pass
