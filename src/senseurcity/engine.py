from sqlalchemy import create_engine, event
from sqlalchemy.engine.base import Engine
from sqlalchemy import schema


def get_engine(
    db_url: str,
    schema_name: str = "measurement",
) -> Engine:
    """"""
    engine = create_engine(db_url)
    if db_url[:6] == "sqlite":
        engine = engine.execution_options(
            schema_translate_map = {
                "measurement": None
            }
        )
        event.listen(
            engine,
            'connect',
            lambda e, _: e.execute('pragma foreign_keys=on')
        )
    elif db_url[:10] == "postgresql":
        if schema_name != "measurement":
            engine = engine.execution_options(
                schema_translate_map = {
                    "measurement": schema_name
                }
            )
        with engine.connect() as conn:
            conn.execute(schema.CreateSchema(schema_name))
            conn.commit()
    return engine

