# validacoes/validar_dados.py
# GE API 0.18+: context.sources (nao data_sources)
# Fallback duplo: validated_* SEMPRE criada
# Salva resultados em lineage/ge_resultados.json
import os, sys, json, duckdb
import great_expectations as gx
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.openlineage_client import start, done, fail

R     = os.environ.get("PROJECT_ROOT", ".")
DB    = os.path.join(R, "dados", "olist.duckdb")
GE_F  = os.path.join(R, "lineage", "ge_resultados.json")

TABELAS = [
    "orders", "customers", "order_items", "products",
    "sellers", "order_payments", "order_reviews",
    "geolocation", "product_category_translation",
]

TABELAS_GE = {"orders", "order_payments", "products"}

# Acumulador de resultados GE (persiste entre chamadas)
_GE_ROWS = []


def _exp_orders(v):
    v.expect_column_values_to_not_be_null("order_id")
    v.expect_column_values_to_be_unique("order_id")
    v.expect_column_values_to_not_be_null("customer_id")
    v.expect_column_values_to_be_in_set(
        "order_status",
        ["delivered","shipped","processing","canceled",
         "invoiced","unavailable","created","approved"])
    v.expect_column_values_to_not_be_null("order_purchase_timestamp", mostly=0.90)
    return v


def _exp_payments(v):
    v.expect_column_values_to_not_be_null("order_id")
    v.expect_column_values_to_be_between(
        "payment_value", min_value=0.01, max_value=100000, mostly=0.99)
    v.expect_column_values_to_be_in_set(
        "payment_type",
        ["credit_card","boleto","voucher","debit_card","not_defined"])
    return v


def _exp_products(v):
    v.expect_column_values_to_not_be_null("product_id")
    v.expect_column_values_to_be_unique("product_id")
    v.expect_column_values_to_be_between(
        "product_weight_g", min_value=1, max_value=100000, mostly=0.95)
    return v


FN = {"orders": _exp_orders, "order_payments": _exp_payments, "products": _exp_products}


def _cria_validated(conn, tab):
    conn.execute(f"DROP TABLE IF EXISTS validated_{tab}")
    conn.execute(f"CREATE TABLE validated_{tab} AS SELECT * FROM raw_{tab}")
    return conn.execute(f"SELECT COUNT(*) FROM validated_{tab}").fetchone()[0]


def _salvar_ge(rows):
    os.makedirs(os.path.dirname(GE_F), exist_ok=True)
    with open(GE_F, "w") as f: json.dump(rows, f, indent=2, default=str)


def validar(tab):
    inp = f"duckdb://olist/raw_{tab}"
    out = f"duckdb://olist/validated_{tab}"
    job = f"validacao_{tab}"
    print(f"  Validando: {tab}")
    start(job, inputs=[inp], outputs=[out])
    conn = duckdb.connect(DB)
    rows_ge = []
    try:
        df = conn.execute(f"SELECT * FROM raw_{tab}").df()
        if tab in TABELAS_GE:
            ctx  = gx.get_context(mode="ephemeral")
            ds   = ctx.sources.add_pandas(f"ds_{tab}")
            ast_ = ds.add_dataframe_asset(f"asset_{tab}")
            br   = ast_.build_batch_request(dataframe=df)
            v    = ctx.get_validator(batch_request=br)
            v    = FN[tab](v)
            res  = v.validate()
            for r in res.results:
                col = r.expectation_config.kwargs.get("column", "—")
                row = {"tabela": tab, "coluna": col,
                       "expectativa": r.expectation_config.expectation_type,
                       "passou": bool(r.success)}
                rows_ge.append(row)
                _GE_ROWS.append(row)
            n_ok = sum(1 for r in res.results if r.success)
            print(f"    GE: {n_ok}/{len(res.results)} OK")
            stats = {"geTotal": len(res.results), "gePassaram": n_ok}
        else:
            stats = {}
        n = _cria_validated(conn, tab)
        conn.close()
        _salvar_ge(_GE_ROWS)
        done(job, inputs=[inp], outputs=[out], stats={**stats, "rowCount": n})
        print(f"    OK validated_{tab}: {n:,} linhas")
        return {"tab": tab, "n": n, "ge_ok": True, "rows_ge": rows_ge}
    except Exception as e:
        print(f"    FALLBACK {tab}: {e}")
        try:
            n = _cria_validated(conn, tab)
            conn.close()
            _salvar_ge(_GE_ROWS)
            done(job, inputs=[inp], outputs=[out], stats={"rowCount": n, "fallback": True})
            print(f"    OK validated_{tab} (fallback): {n:,} linhas")
            return {"tab": tab, "n": n, "ge_ok": False, "rows_ge": rows_ge}
        except Exception as e2:
            conn.close()
            fail(job, inputs=[inp], outputs=[out], erro=str(e2))
            return {"tab": tab, "n": 0, "ge_ok": False, "rows_ge": []}


def executar_validacoes():
    print("=" * 55)
    print("VALIDACAO raw_* → validated_* (fallback garantido)")
    print("=" * 55)
    resultados = [validar(t) for t in TABELAS]
    ok = sum(1 for r in resultados if r["n"] > 0)
    print(f"\n{ok}/{len(TABELAS)} tabelas validated_* criadas")
    return resultados


if __name__ == "__main__": executar_validacoes()
