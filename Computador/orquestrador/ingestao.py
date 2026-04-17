# orquestrador/ingestao.py — CSV → raw_*
import os, sys, duckdb, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.openlineage_client import start, done, fail

R       = os.environ.get("PROJECT_ROOT", ".")
RAW     = os.path.join(R, "dados", "raw")
DB      = os.path.join(R, "dados", "olist.duckdb")

TABELAS = {
    "olist_orders_dataset.csv":               "orders",
    "olist_customers_dataset.csv":            "customers",
    "olist_order_items_dataset.csv":          "order_items",
    "olist_products_dataset.csv":             "products",
    "olist_sellers_dataset.csv":              "sellers",
    "olist_order_payments_dataset.csv":       "order_payments",
    "olist_order_reviews_dataset.csv":        "order_reviews",
    "olist_geolocation_dataset.csv":          "geolocation",
    "product_category_name_translation.csv":  "product_category_translation"
}


def _carregar(conn, csv, tabela):
    inp = f"file://dados/raw/{csv}"
    out = f"duckdb://olist/raw_{tabela}"
    job = f"ingestao_{tabela}"
    start(job, inputs=[inp], outputs=[out])
    try:
        df   = pd.read_csv(os.path.join(RAW, csv), low_memory=False)
        orig = len(df)
        df   = df.drop_duplicates()
        df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
        for col in [c for c in df.columns if "date" in c or "timestamp" in c]:
            try: df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception: pass
        conn.execute(f"DROP TABLE IF EXISTS raw_{tabela}")
        conn.execute(f"CREATE TABLE raw_{tabela} AS SELECT * FROM df")
        n = conn.execute(f"SELECT COUNT(*) FROM raw_{tabela}").fetchone()[0]
        done(job, inputs=[inp], outputs=[out],
             stats={"rowCount": n, "cols": len(df.columns), "dupRemov": orig - n})
        print(f"  raw_{tabela}: {n:,} linhas")
        return n
    except Exception as e:
        fail(job, inputs=[inp], outputs=[out], erro=str(e))
        raise


def executar_ingestao():
    print("=" * 55)
    print("INGESTAO CSV → raw_*")
    print("=" * 55)
    conn = duckdb.connect(DB)
    res  = {}
    for csv, tab in TABELAS.items():
        if os.path.exists(os.path.join(RAW, csv)):
            try: res[tab] = _carregar(conn, csv, tab)
            except Exception as e: print(f"  ERRO {tab}: {e}")
        else:
            print(f"  NAO ENCONTRADO: {csv}")
    conn.close()
    print(f"\n{len(res)} tabelas raw carregadas")
    return res


if __name__ == "__main__": executar_ingestao()
