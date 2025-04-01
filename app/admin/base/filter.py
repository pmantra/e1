from flask_admin.babel import lazy_gettext
from flask_admin.model import filters

"""
Note: These filters only work with ServiceProtocolModelView. Also, "work" here means that
it will run a comparison check to see if the stored value is equal to the supplied value.
Regardless of the class names, all filters here will do an equality check due to the 
implementation of ServiceProtocolModelView.get_list(). When used with standard Views,
the filters will cause an error because NoOpFilter.apply() does not return anything (and 
therefore returns None), which will eventually cause an AttributeError when downstream 
code tries to make a call on the NoneType. For standard views, use the filters in 
flask_admin.contrib.sqla.filters instead. 
"""


class NoOpFilter(filters.BaseFilter):
    def apply(self, query, value):
        ...


# Common filters
class FilterEqual(NoOpFilter):
    def operation(self):
        return lazy_gettext("equals")


class FilterNotEqual(NoOpFilter):
    def operation(self):
        return lazy_gettext("not equal")


class FilterLike(NoOpFilter):
    def operation(self):
        return lazy_gettext("contains")


class FilterNotLike(NoOpFilter):
    def operation(self):
        return lazy_gettext("not contains")


class FilterGreater(NoOpFilter):
    def operation(self):
        return lazy_gettext("greater than")


class FilterSmaller(NoOpFilter):
    def operation(self):
        return lazy_gettext("smaller than")


class FilterEmpty(NoOpFilter, filters.BaseBooleanFilter):
    def operation(self):
        return lazy_gettext("empty")


class FilterInList(NoOpFilter):
    def __init__(self, name, options=None, data_type=None):
        super().__init__(name, options, data_type="select2-tags")

    def clean(self, value):
        return [v.strip() for v in value.split(",") if v.strip()]

    def operation(self):
        return lazy_gettext("in list")


class FilterNotInList(FilterInList):
    def operation(self):
        return lazy_gettext("not in list")


# Customized type filters
class BooleanEqualFilter(FilterEqual, filters.BaseBooleanFilter):
    pass


class BooleanNotEqualFilter(FilterNotEqual, filters.BaseBooleanFilter):
    pass


class IntEqualFilter(FilterEqual, filters.BaseIntFilter):
    pass


class IntNotEqualFilter(FilterNotEqual, filters.BaseIntFilter):
    pass


class IntGreaterFilter(FilterGreater, filters.BaseIntFilter):
    pass


class IntSmallerFilter(FilterSmaller, filters.BaseIntFilter):
    pass


class IntInListFilter(filters.BaseIntListFilter, FilterInList):
    pass


class IntNotInListFilter(filters.BaseIntListFilter, FilterNotInList):
    pass


class FloatEqualFilter(FilterEqual, filters.BaseFloatFilter):
    pass


class FloatNotEqualFilter(FilterNotEqual, filters.BaseFloatFilter):
    pass


class FloatGreaterFilter(FilterGreater, filters.BaseFloatFilter):
    pass


class FloatSmallerFilter(FilterSmaller, filters.BaseFloatFilter):
    pass


class FloatInListFilter(filters.BaseFloatListFilter, FilterInList):
    pass


class FloatNotInListFilter(filters.BaseFloatListFilter, FilterNotInList):
    pass


class DateEqualFilter(FilterEqual, filters.BaseDateFilter):
    pass


class DateNotEqualFilter(FilterNotEqual, filters.BaseDateFilter):
    pass


class DateGreaterFilter(FilterGreater, filters.BaseDateFilter):
    pass


class DateSmallerFilter(FilterSmaller, filters.BaseDateFilter):
    pass


class DateBetweenFilter(NoOpFilter, filters.BaseDateBetweenFilter):
    def __init__(self, name, options=None, data_type=None):
        super().__init__(name, options, data_type="daterangepicker")


class DateNotBetweenFilter(DateBetweenFilter):
    def operation(self):
        return lazy_gettext("not between")


class DateTimeEqualFilter(FilterEqual, filters.BaseDateTimeFilter):
    pass


class DateTimeNotEqualFilter(FilterNotEqual, filters.BaseDateTimeFilter):
    pass


class DateTimeGreaterFilter(FilterGreater, filters.BaseDateTimeFilter):
    pass


class DateTimeSmallerFilter(FilterSmaller, filters.BaseDateTimeFilter):
    pass


class DateTimeBetweenFilter(NoOpFilter, filters.BaseDateTimeBetweenFilter):
    def __init__(self, name, options=None, data_type=None):
        super().__init__(name, options, data_type="datetimerangepicker")


class DateTimeNotBetweenFilter(DateTimeBetweenFilter):
    def operation(self):
        return lazy_gettext("not between")


# Base SQLA filter field converter
class FilterConverter(filters.BaseFilterConverter):
    strings = (
        FilterLike,
        FilterNotLike,
        FilterEqual,
        FilterNotEqual,
        FilterEmpty,
        FilterInList,
        FilterNotInList,
    )
    string_key_filters = (
        FilterEqual,
        FilterNotEqual,
        FilterEmpty,
        FilterInList,
        FilterNotInList,
    )
    int_filters = (
        IntEqualFilter,
        IntNotEqualFilter,
        IntGreaterFilter,
        IntSmallerFilter,
        FilterEmpty,
        IntInListFilter,
        IntNotInListFilter,
    )
    float_filters = (
        FloatEqualFilter,
        FloatNotEqualFilter,
        FloatGreaterFilter,
        FloatSmallerFilter,
        FilterEmpty,
        FloatInListFilter,
        FloatNotInListFilter,
    )
    bool_filters = (BooleanEqualFilter, BooleanNotEqualFilter)
    date_filters = (
        DateEqualFilter,
        DateNotEqualFilter,
        DateGreaterFilter,
        DateSmallerFilter,
        DateBetweenFilter,
        DateNotBetweenFilter,
        FilterEmpty,
    )
    datetime_filters = (
        DateTimeEqualFilter,
        DateTimeNotEqualFilter,
        DateTimeGreaterFilter,
        DateTimeSmallerFilter,
        DateTimeBetweenFilter,
        DateTimeNotBetweenFilter,
        FilterEmpty,
    )

    def convert(self, type_name, name, **kwargs):
        filter_name = type_name.lower()

        if filter_name in self.converters:
            return self.converters[filter_name](name, **kwargs)

        return None

    @filters.convert("str")
    def conv_string(self, name, **kwargs):
        return [f(name, **kwargs) for f in self.strings]

    @filters.convert("UUID")
    def conv_string_keys(self, name, **kwargs):
        return [f(name, **kwargs) for f in self.string_key_filters]

    @filters.convert("bool")
    def conv_bool(self, name, **kwargs):
        return [f(name, **kwargs) for f in self.bool_filters]

    @filters.convert("int")
    def conv_int(self, name, **kwargs):
        return [f(name, **kwargs) for f in self.int_filters]

    @filters.convert("float", "Decimal")
    def conv_float(self, name, **kwargs):
        return [f(name, **kwargs) for f in self.float_filters]

    @filters.convert("date")
    def conv_date(self, name, **kwargs):
        return [f(name, **kwargs) for f in self.date_filters]

    @filters.convert("datetime")
    def conv_datetime(self, name, **kwargs):
        return [f(name, **kwargs) for f in self.datetime_filters]
