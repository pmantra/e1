from typing import Optional

from maven import feature_flags

from app.eligibility import constants as e9y_constants
from config import settings
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

Base = automap_base()


def init_engine(*, db_settings: settings.DB, max_execution_seconds: Optional[int]):
    # max_execution_seconds: Sets the maximum allowed duration of any statement in seconds
    # will configure postgres statement_timeout (in ms) as max_execution_seconds * 1000
    # refer: https://postgresqlco.nf/doc/en/param/statement_timeout/

    # db_port: int = (
    #     db_settings.read_port if read_only and not local_dev else db_settings.main_port
    # )
    # isolation_level: str = "REPEATABLE_READ" if read_only else "AUTOCOMMIT"

    isolation_level = "AUTOCOMMIT"
    named_arg = {
        "isolation_level": isolation_level,
        "pool_pre_ping": True,
        "pool_recycle": 3600,
    }
    if max_execution_seconds:
        named_arg["connect_args"] = {
            "options": f"-c statement_timeout={max_execution_seconds * 1000}"
        }

    the_db_host = db_settings.host
    the_db_port = db_settings.main_port

    use_new_db = feature_flags.bool_variation(
        e9y_constants.E9yFeatureFlag.RELEASE_ELIGIBILITY_DATABASE_INSTANCE_SWITCH,
        default=False,
    )
    if use_new_db:
        the_db_host = db_settings.host
        the_db_port = the_db_port + 4

    return create_engine(
        f"{db_settings.scheme}+psycopg2://{db_settings.user}:{db_settings.password}@{the_db_host}:{str(the_db_port)}/{db_settings.db}",
        **named_arg,
    )


# This is used to help prevent SQL alchemy from trying to recreate relationships between tables based on the schema it loads from PSQL
# these relationships are already defined, so this helps quiet some warnings SQLAlch throws
# https://stackoverflow.com/questions/67158435/sqlalchemy-1-4-warnings-on-overlapping-relationships-with-a-many-to-many-relatio
def generate_relationships(
    base, direction, return_fn, attrname, local_cls, referred_cls, **kw
):
    return None


def get_session_maker(max_execution_seconds: Optional[int] = None):

    """
    max_execution_seconds: Sets the maximum allowed duration of any statement in seconds
        if None, no limit on statement execution time
    Return a session maker

    Usage:
        Session = get_session_maker()

        with Session.begin() as session:
            session.add(some_object)
            session.add(some_other_object)
        # commits transaction, closes session

    """
    db_settings = settings.DB()
    engine = init_engine(
        db_settings=db_settings, max_execution_seconds=max_execution_seconds
    )
    Base.prepare(engine, reflect=True)
    session_maker = sessionmaker(bind=engine, expire_on_commit=False)
    return session_maker


Session = get_session_maker()
