from sqlalchemy import create_engine, event
from sqlalchemy.engine.base import Engine


def get_engine(db_url: str) -> Engine:
    """"""
    engine = create_engine(db_url)
    event.listen(
        engine,
        'connect',
        lambda e, _: e.execute('pragma foreign_keys=on')
    )
    return engine

