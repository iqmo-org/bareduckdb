import logging
from pathlib import Path

import bareduckdb

logging.basicConfig(level=getattr(logging, "DEBUG", logging.DEBUG), format="[%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

data_path = Path(".")

for r in data_path.rglob("data/**/create.sql"):
    cr = r.relative_to("data")

    data_name = print(cr.parent)
    statement = print(cr.name)

    with bareduckdb.connect() as conn:
        query = r.read_text()
        logger.info(f"Creating {data_name=} with {query=}")
        conn.execute(query)
