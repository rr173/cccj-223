import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import bootstrap  # noqa

from factasm import FactAssembler
from factasm.db import destroy_database
from factasm.sources import register_batch
from tests.fixtures_builder import (
    build_fixtures, CUSTOMER_BATCH, CONTRACT_BATCH, TXN_BATCH,
)

work = ROOT / "run_smoke"
destroy_database(work / "wh.duckdb")
import shutil
shutil.rmtree(work, ignore_errors=True)
fixdir = work / "incoming"
paths = build_fixtures(fixdir)

for kind, bid, key in [
    ("customer", CUSTOMER_BATCH, "customers"),
    ("contract", CONTRACT_BATCH, "contracts"),
    ("transaction", TXN_BATCH, "transactions"),
]:
    print(register_batch(work / "wh.duckdb", batch_id=bid, source_kind=kind, path=paths[key]))

asm = FactAssembler(work / "wh.duckdb")
res = asm.assemble(
    customer_batch=CUSTOMER_BATCH,
    contract_batch=CONTRACT_BATCH,
    transaction_batch=TXN_BATCH,
)
print("STATUS", res.status, "facts", res.fact_count, "quar", res.quarantine_count)
for f in res.facts:
    print(f["txn_row_id"], f["customer_generation_id"], f["contract_generation_id"])
for q in res.quarantine:
    print("Q", q["txn_row_id"], q["reason_code"])
