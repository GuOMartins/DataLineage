# utils/openlineage_client.py
import os, uuid, requests
from datetime import datetime, timezone

URL  = os.environ.get("OPENLINEAGE_URL",       "http://localhost:5000")
NS   = os.environ.get("OPENLINEAGE_NAMESPACE", "datalineage_olist")
EP   = f"{URL}/api/v1/lineage"


def _ds(name):
    return {"namespace": NS, "name": name}


def emit(job, tipo, inputs=None, outputs=None, facets=None):
    ins  = [_ds(i) if isinstance(i, str) else i for i in (inputs  or [])]
    outs = [_ds(o) if isinstance(o, str) else o for o in (outputs or [])]
    if tipo == "COMPLETE" and (not ins or not outs):
        print(f"  LINEAGE INCOMPLETO [{job}] in={len(ins)} out={len(outs)}")
    ev = {
        "eventType":  tipo,
        "eventTime":  datetime.now(timezone.utc).isoformat(),
        "run":        {"runId": str(uuid.uuid4()), "facets": facets or {}},
        "job":        {"namespace": NS, "name": job},
        "inputs":     ins,
        "outputs":    outs,
        "producer":   "https://github.com/GustavoMartins23/DataLineage",
        "schemaURL":  "https://openlineage.io/spec/1-0-5/OpenLineage.json",
    }
    try:
        r = requests.post(EP, json=ev,
                          headers={"Content-Type": "application/json"}, timeout=5)
        return r.status_code in [200, 201]
    except Exception as e:
        print(f"  OpenLineage offline: {e}")
        return False


def start(job, inputs=None, outputs=None):
    return emit(job, "START", inputs, outputs)


def done(job, inputs, outputs, stats=None):
    facets = {}
    if stats:
        facets["dataQualityMetrics"] = {
            "_producer": "datalineage-olist",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsFacet.json",
            **stats,
        }
    return emit(job, "COMPLETE", inputs, outputs, facets)


def fail(job, inputs=None, outputs=None, erro=""):
    return emit(job, "FAIL", inputs, outputs,
                {"errorMessage": {"_producer": "datalineage-olist", "message": str(erro)}})
