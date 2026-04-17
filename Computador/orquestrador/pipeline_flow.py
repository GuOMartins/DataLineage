# orquestrador/pipeline_flow.py
from prefect import flow, task, get_run_logger
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.openlineage_client    import start as ol_start, done as ol_done, fail as ol_fail
from orquestrador.ingestao       import executar_ingestao
from validacoes.validar_dados    import executar_validacoes
from orquestrador.transformacao_dbt import executar_dbt


@task(name="Ingestao CSV→raw", retries=2, retry_delay_seconds=30)
def task_ingestao():
    lg = get_run_logger()
    lg.info("Ingestao: CSV → raw_*")
    r = executar_ingestao()
    lg.info(f"{len(r)} tabelas raw")
    return r


@task(name="Validacao raw→validated")
def task_validacao():
    lg = get_run_logger()
    lg.info("Validacao: raw_* → validated_* (fallback ativo)")
    r = executar_validacoes()
    lg.info(f"{len(r)} validated_* criadas")
    return r


@task(name="Transformacao SQL→analytics")
def task_dbt():
    lg = get_run_logger()
    lg.info("Transformacao: validated_* → stg → fact → mart_kpis")
    tabs = executar_dbt()
    lg.info(f"Transformacao OK: {len(tabs)} tabelas")
    return tabs


@flow(name="DataLineage v5 — Olist",
      description="CSV→raw→validated→stg→fact→mart_kpis (OpenLineage)")
def pipeline_completo():
    lg = get_run_logger()
    lg.info("Pipeline v5 iniciado")
    ol_start("pipeline_v5",
             inputs=["file://dados/raw/*"],
             outputs=["duckdb://olist/mart_kpis_mensais"])
    try:
        r1 = task_ingestao()
        r2 = task_validacao(wait_for=[r1])
        r3 = task_dbt(wait_for=[r2])
        ol_done("pipeline_v5",
                inputs=["file://dados/raw/*"],
                outputs=["duckdb://olist/mart_kpis_mensais"],
                stats={"status": "sucesso"})
        lg.info("Pipeline v5 concluido")
        return {"ing": r1, "val": r2, "dbt": r3}
    except Exception as e:
        ol_fail("pipeline_v5",
                inputs=["file://dados/raw/*"],
                outputs=["duckdb://olist/mart_kpis_mensais"],
                erro=str(e))
        lg.error(f"Pipeline falhou: {e}")
        raise


if __name__ == "__main__": pipeline_completo()
