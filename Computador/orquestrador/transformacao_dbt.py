# orquestrador/transformacao_dbt.py — SQL puro DuckDB
import os, sys, duckdb
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.openlineage_client import start, done, fail

R  = os.environ.get("PROJECT_ROOT", ".")
DB = os.path.join(R, "dados", "olist.duckdb")

_STG_O  = "SELECT o.order_id, o.customer_id, o.order_status, TRY_CAST(o.order_purchase_timestamp AS TIMESTAMP) AS purchased_at, TRY_CAST(o.order_approved_at AS TIMESTAMP) AS approved_at, TRY_CAST(o.order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_at, TRY_CAST(o.order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at, DATE_DIFF('day',   TRY_CAST(o.order_purchase_timestamp AS TIMESTAMP),   TRY_CAST(o.order_delivered_customer_date AS TIMESTAMP)) AS dias_ate_entrega FROM {SRC_O} o WHERE o.order_id IS NOT NULL"
_STG_C  = 'WITH l AS (SELECT customer_id, customer_unique_id, LOWER(TRIM(customer_city)) AS cidade, UPPER(TRIM(customer_state)) AS estado FROM raw_customers WHERE customer_id IS NOT NULL), d AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn FROM l) SELECT customer_id, customer_unique_id, cidade, estado FROM d WHERE rn = 1'
_STG_I  = 'SELECT order_id, order_item_id, product_id, seller_id, CASE WHEN price > 0 THEN price ELSE NULL END AS preco, CASE WHEN freight_value >= 0 THEN freight_value ELSE NULL END AS frete, CASE WHEN price > 0 AND freight_value >= 0      THEN ROUND(price + freight_value, 2) ELSE NULL END AS valor_total FROM raw_order_items WHERE order_id IS NOT NULL AND product_id IS NOT NULL'
_STG_P  = 'SELECT order_id, payment_sequential, payment_type, payment_installments AS parcelas, ROUND(payment_value, 2) AS valor_pago FROM {SRC_P} WHERE order_id IS NOT NULL AND payment_value > 0'
_FACT   = 'WITH itens AS (  SELECT order_id, COUNT(*) AS qtd_itens,  COUNT(DISTINCT product_id) AS qtd_produtos,  COUNT(DISTINCT seller_id) AS qtd_vendedores,  SUM(COALESCE(preco,0)) AS subtotal,  SUM(COALESCE(frete,0)) AS total_frete,  SUM(COALESCE(valor_total,0)) AS valor_total_itens  FROM stg_order_items GROUP BY order_id), pags AS (  SELECT order_id, SUM(valor_pago) AS total_pago,  COUNT(*) AS qtd_pags, MAX(parcelas) AS max_parcelas,  FIRST(payment_type ORDER BY valor_pago DESC) AS tipo_pagamento  FROM stg_order_payments GROUP BY order_id) SELECT o.order_id, o.customer_id, c.cidade AS cidade_cliente, c.estado AS estado_cliente, o.order_status, o.purchased_at, o.approved_at, o.delivered_customer_at, o.estimated_delivery_at, o.dias_ate_entrega, CASE WHEN o.delivered_customer_at IS NULL THEN NULL      WHEN o.estimated_delivery_at IS NULL THEN NULL      WHEN o.delivered_customer_at <= o.estimated_delivery_at THEN TRUE      ELSE FALSE END AS entregue_no_prazo, COALESCE(i.qtd_itens, 0) AS qtd_itens, COALESCE(i.qtd_produtos, 0) AS qtd_produtos, COALESCE(i.qtd_vendedores, 0) AS qtd_vendedores, ROUND(COALESCE(i.subtotal, 0), 2) AS subtotal, ROUND(COALESCE(i.total_frete, 0), 2) AS total_frete, ROUND(COALESCE(p.total_pago, 0), 2) AS total_pago, p.qtd_pags, p.max_parcelas, p.tipo_pagamento, CURRENT_TIMESTAMP AS processado_em FROM stg_orders o LEFT JOIN stg_customers c ON o.customer_id = c.customer_id LEFT JOIN itens i ON o.order_id = i.order_id LEFT JOIN pags p ON o.order_id = p.order_id'
_MART   = "WITH base AS (  SELECT * FROM fact_orders  WHERE order_status = 'delivered' AND purchased_at IS NOT NULL) SELECT DATE_TRUNC('month', purchased_at) AS mes, STRFTIME(purchased_at, '%Y-%m') AS mes_ano, COUNT(DISTINCT order_id) AS total_pedidos, COUNT(DISTINCT customer_id) AS clientes_unicos, ROUND(SUM(total_pago), 2) AS receita_total, ROUND(AVG(total_pago), 2) AS ticket_medio, ROUND(SUM(total_frete), 2) AS total_frete, SUM(qtd_itens) AS total_itens, ROUND(AVG(qtd_itens), 2) AS media_itens, ROUND(   100.0 * COUNT(CASE WHEN entregue_no_prazo = TRUE THEN 1 END)   / NULLIF(COUNT(CASE WHEN entregue_no_prazo IS NOT NULL THEN 1 END), 0) , 2) AS pct_entrega_no_prazo, ROUND(AVG(dias_ate_entrega), 1) AS media_dias_entrega, COUNT(CASE WHEN tipo_pagamento = 'credit_card' THEN 1 END) AS pag_cartao, COUNT(CASE WHEN tipo_pagamento = 'boleto'      THEN 1 END) AS pag_boleto FROM base GROUP BY 1, 2 ORDER BY 1"


def _ex(conn, tab, inp, out, sql):
    job = f"dbt_{tab}"
    start(job, inputs=inp, outputs=out)
    conn.execute(f"DROP TABLE IF EXISTS {tab}")
    conn.execute(f"CREATE TABLE {tab} AS {sql}")
    n = conn.execute(f"SELECT COUNT(*) FROM {tab}").fetchone()[0]
    done(job, inputs=inp, outputs=out, stats={"rowCount": n})
    print(f"  {tab}: {n:,}")
    return n


def _src(conn, validada, raw):
    tabs = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    return validada if validada in tabs else raw


def executar_dbt():
    print("=" * 55)
    print("TRANSFORMACAO SQL → stg_* → fact → mart")
    print("=" * 55)
    conn = duckdb.connect(DB)
    src_o = _src(conn, "validated_orders",         "raw_orders")
    src_p = _src(conn, "validated_order_payments", "raw_order_payments")
    _ex(conn, "stg_orders",
        [f"duckdb://olist/{src_o}"], ["duckdb://olist/stg_orders"],
        _STG_O.format(SRC_O=src_o))
    _ex(conn, "stg_customers",
        ["duckdb://olist/raw_customers"], ["duckdb://olist/stg_customers"],
        _STG_C)
    _ex(conn, "stg_order_items",
        ["duckdb://olist/raw_order_items"], ["duckdb://olist/stg_order_items"],
        _STG_I)
    _ex(conn, "stg_order_payments",
        [f"duckdb://olist/{src_p}"], ["duckdb://olist/stg_order_payments"],
        _STG_P.format(SRC_P=src_p))
    _ex(conn, "fact_orders",
        ["duckdb://olist/stg_orders","duckdb://olist/stg_customers",
         "duckdb://olist/stg_order_items","duckdb://olist/stg_order_payments"],
        ["duckdb://olist/fact_orders"], _FACT)
    _ex(conn, "mart_kpis_mensais",
        ["duckdb://olist/fact_orders"], ["duckdb://olist/mart_kpis_mensais"],
        _MART)
    tabs = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    conn.close()
    print(f"\nTransformacao concluida: {len(tabs)} tabelas")
    return tabs


if __name__ == "__main__": executar_dbt()
