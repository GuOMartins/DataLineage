# 🔗 DataLineage v5 — Pipeline End-to-End com OpenLineage

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Prefect](https://img.shields.io/badge/Orquestração-Prefect-1A237E?style=for-the-badge&logo=prefect&logoColor=white)
![dbt](https://img.shields.io/badge/Transform-dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![DuckDB](https://img.shields.io/badge/DB-DuckDB-FFC107?style=for-the-badge&logo=duckdb&logoColor=black)
![OpenLineage](https://img.shields.io/badge/Lineage-OpenLineage-4CAF50?style=for-the-badge&logo=linux-foundation&logoColor=white)
![Plotly](https://img.shields.io/badge/Charts-Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)
![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Google Colab](https://img.shields.io/badge/Colab-Google-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-gustavoomartins-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://linkedin.com/in/gustavoomartins)
[![GitHub](https://img.shields.io/badge/GitHub-GuOMartins-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/GuOMartins)

**Rastreabilidade completa ponta a ponta: CSV → raw → validated → stg → fact → mart\_kpis,
com grafo interativo Plotly e dashboard profissional de 6 abas.**

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/GuOMartins/DataLineage/blob/main/notebooks/datalineage_v5_colab.ipynb)

</div>

---

## 🎬 Demo rápida

<div align="center">
  <img src="assets/gifs/dashboard_navegacao.gif" alt="Navegação pelo Dashboard DataLineage v5" width="90%">
  <p><em>Dashboard com 6 abas interativas: grafo de lineage, KPIs, qualidade, análise temporal, distribuições e diagnóstico</em></p>
</div>

---

## 📌 Sobre o Projeto

Em ambientes de dados reais, é muito comum não saber de onde vieram os dados de uma tabela, quais transformações foram aplicadas ou quando e por que um dado mudou. Esse problema é chamado de **falta de linhagem de dados** — e é exatamente o que este projeto resolve.

O **DataLineage v5** é um pipeline completo de engenharia de dados que:

1. **Ingere** os dados reais do e-commerce brasileiro (Olist) em um banco analítico local
2. **Valida** a qualidade com Great Expectations e persiste em tabelas `validated_*`
3. **Transforma** em camadas organizadas com SQL versionado
4. **Rastreia** o caminho completo de cada dado com eventos OpenLineage
5. **Visualiza** o grafo real de linhagem em um dashboard Plotly interativo

> 💡 O padrão OpenLineage é usado por Netflix, LinkedIn e Datadog para governança de dados em escala. Este projeto torna essa tecnologia acessível com ferramentas 100% open source.

---

## 🏗️ Arquitetura

<div align="center">
  <img src="assets/arquitetura/diagrama_alto_nivel.png" alt="Diagrama de alto nível — DataLineage v5" width="85%">
  <p><em>Visão de alto nível do pipeline</em></p>
</div>

### Fluxo completo dos dados

```
📁 CSV (Kaggle/Olist) · 9 arquivos · ~100k pedidos reais (2016-2018)
     │
     ▼  [Ingestão — Prefect task_ingestao]
🥉 raw_*  (9 tabelas DuckDB · eventos OpenLineage: CSV → raw_*)
     │
     ▼  [Validação — Great Expectations · fallback garantido]
🥈 validated_*  (9 tabelas · resultados em lineage/ge_resultados.json)
     │
     ▼  [Transformação SQL — Prefect task_dbt]
🥇 stg_orders · stg_customers · stg_order_items · stg_order_payments
     │
     ▼  [JOIN convergente — 4 streams → 1 tabela]
⭐ fact_orders  (com coluna entregue_no_prazo)
     │
     ▼  [Agregação mensal]
🏆 mart_kpis_mensais
     │
     ▼  [OpenLineage Events → lineage/events/]
📊 Dashboard Streamlit + Plotly · 6 abas interativas
```

<div align="center">
  <img src="assets/arquitetura/diagrama_tecnico.png" alt="Diagrama técnico detalhado" width="85%">
  <p><em>Componentes técnicos e integrações</em></p>
</div>

### Orquestração Prefect (dependências explícitas)

```
task_ingestao  →  task_validacao  →  task_dbt
   (wait_for)         (wait_for)
```

---

## 🗺️ Grafo de Linhagem Interativo

<div align="center">
  <img src="assets/grafo/grafo_ponta_a_ponta.png" alt="Grafo de Lineage Ponta a Ponta" width="90%">
  <p><em>Grafo renderizado com plotly.graph_objects — nós = datasets, arestas = transformações, cores = camadas</em></p>
</div>

<div align="center">
  <img src="assets/gifs/grafo_interativo.gif" alt="Grafo interativo — zoom e hover" width="75%">
  <p><em>Interatividade: hover nos nós, zoom, detalhes das transformações</em></p>
</div>

### Diagnóstico de Integridade do Lineage

<div align="center">
  <img src="assets/diagnostico/diag_barras_linhagem.png" alt="Diagnóstico de Linhagem" width="80%">
  <p><em>Todos os 10 jobs com inputs e outputs definidos — grafo 100% íntegro</em></p>
</div>

```
Job                    │ Inputs │ Outputs │ Status
───────────────────────┼────────┼─────────┼──────────
ingestao_orders        │   1    │    1    │  OK
validacao_orders       │   1    │    1    │  OK  ← GE real
validacao_payments     │   1    │    1    │  OK  ← fallback
dbt_stg_orders         │   1    │    1    │  OK
dbt_fact_orders        │   4    │    1    │  OK  ← JOIN 4 stg
dbt_mart_kpis_mensais  │   1    │    1    │  OK

10/10 jobs com lineage completo!
```

---

## 📊 Dashboard Interativo

### KPIs Mensais

<div align="center">
  <img src="assets/gifs/drill_down.gif" alt="KPIs interativos com drill-down" width="85%">
  <p><em>Filtros por período, estatísticas descritivas (média, mediana, desvio), trendline e exportação CSV</em></p>
</div>

<table>
  <tr>
    <td align="center">
      <img src="assets/kpis/kpi_total_pedidos.png" width="200"><br>
      <sub>Total de Pedidos</sub>
    </td>
    <td align="center">
      <img src="assets/kpis/kpi_receita.png" width="200"><br>
      <sub>Receita Total</sub>
    </td>
    <td align="center">
      <img src="assets/kpis/kpi_ticket_medio.png" width="200"><br>
      <sub>Ticket Médio</sub>
    </td>
  </tr>
  <tr>
    <td align="center">
      <img src="assets/kpis/kpi_entrega_prazo.png" width="200"><br>
      <sub>% Entrega no Prazo</sub>
    </td>
    <td align="center">
      <img src="assets/kpis/kpi_unicos.png" width="200"><br>
      <sub>Clientes Únicos</sub>
    </td>
    <td align="center">
      <img src="assets/kpis/kpi_total_itens.png" width="200"><br>
      <sub>Total de Itens</sub>
    </td>
  </tr>
</table>

### Análise Temporal e Distribuições

<div align="center">
  <img src="assets/analise/analise_temporal_pedidos_receita.png" alt="Análise Temporal" width="80%">
  <p><em>Evolução mensal de pedidos e receita com gráfico de área</em></p>
</div>

<table>
  <tr>
    <td align="center">
      <img src="assets/analise/analise_temporal_sazonalidade.png" width="280"><br>
      <sub>Sazonalidade por mês do ano</sub>
    </td>
    <td align="center">
      <img src="assets/analise/distribuicoes_valor_pedido.png" width="280"><br>
      <sub>Distribuição do valor do pedido</sub>
    </td>
  </tr>
  <tr>
    <td align="center">
      <img src="assets/analise/distribuicoes_tempo_entrega.png" width="280"><br>
      <sub>Distribuição do tempo de entrega</sub>
    </td>
    <td align="center">
      <img src="assets/analise/boxplot_tipo_pagamento.png" width="280"><br>
      <sub>Valor por tipo de pagamento</sub>
    </td>
  </tr>
</table>

<div align="center">
  <img src="assets/analise/porc_entrega_estado.png" alt="% Entrega no Prazo por Estado" width="75%">
  <p><em>% de entrega no prazo por estado — barras horizontais com escala vermelho→verde</em></p>
</div>

### Qualidade dos Dados (Great Expectations)

<div align="center">
  <img src="assets/qualidade/ge_taxa_sucesso.png" alt="Taxa de Sucesso GE por Tabela" width="80%">
  <p><em>Taxa de sucesso das expectativas por tabela — falhas destacadas em vermelho</em></p>
</div>

<div align="center">
  <img src="assets/qualidade/ge_tabela_sucesso.png" alt="Tabela detalhada de resultados GE" width="75%">
  <p><em>Detalhamento por expectativa: tabela, coluna, tipo e resultado</em></p>
</div>

---

## 🛠️ Stack Tecnológica

| Camada | Tecnologia | Por que foi escolhida |
|--------|-----------|----------------------|
| **Ingestão** | pandas + DuckDB | DuckDB lê CSV direto para SQL, sem servidor |
| **Validação** | Great Expectations 0.18+ | `context.sources`, fallback duplo, gera `validated_*` |
| **Transformação** | SQL puro DuckDB | Compatibilidade máxima, zero dependência externa |
| **Orquestração** | Prefect 2 | `wait_for` garante ordem explícita entre tasks |
| **Linhagem** | OpenLineage | Padrão Linux Foundation — inputs e outputs reais |
| **Metadata Store** | Marquez / Mock local | API REST; Mock salva JSON sem Docker |
| **Visualização** | Streamlit + Plotly | Grafo interativo, filtros, drill-down, estatísticas |
| **Ambiente** | Google Colab / VS Code / Jupyter | 100% open source, sem custo |

---

## 🧠 Decisões Técnicas

**Por que `validated_*` tem OUTPUT no OpenLineage?** Sem esse output, a validação é um beco sem saída no grafo — dados entram mas nada sai. O Marquez não conecta o que vem depois. Cada validação salva `validated_{tabela}` no DuckDB e emite um evento COMPLETE com `outputs=[validated_*]`.

**Por que o slider usa strings YYYY-MM?** `st.slider()` com `datetime64` causa `AttributeError: 'int' has no attribute 'to_pydatetime'` em certas versões do Streamlit. Strings `YYYY-MM` eliminam esse bug sem perda de funcionalidade.

**Por que trendline com numpy?** `px.line(trendline="ols")` requer `statsmodels`, ausente no Colab. A trendline é calculada com `numpy.polyfit()` — mesmo visual, zero dependência extra.

**Por que `ast.parse()` antes de escrever `.py`?** Arquivos gerados via strings no notebook podiam ter erros silenciosos. `ast.parse()` antes de escrever o arquivo garante que qualquer erro de sintaxe falhe com número de linha exato.

---

## 🖥️ Compatibilidade por Ambiente

| Componente | Google Colab | VS Code / Jupyter local |
|---|---|---|
| **Marquez** | Mock embutido (JSON em `lineage/events/`) | Docker Compose (`docker/`) |
| **Streamlit** | Via ngrok (link público automático) | `localhost:8501` |
| **Dataset** | `kagglehub` automático | `~/.kaggle/kaggle.json` |
| **Restart após Célula 1?** | ✅ Sim — `Runtime → Restart runtime` | ❌ Não necessário |

---

## 📁 Estrutura do Projeto

```
DataLineage/
├── notebooks/
│   └── datalineage_v5_colab.ipynb
├── orquestrador/
│   ├── ingestao.py
│   ├── transformacao_dbt.py
│   └── pipeline_flow.py
├── validacoes/
│   └── validar_dados.py
├── visualizacao/
│   └── dashboard.py
├── utils/
│   └── openlineage_client.py
├── assets/
│   ├── arquitetura/
│   ├── grafo/
│   ├── kpis/
│   ├── analise/
│   ├── qualidade/
│   ├── diagnostico/
│   └── gifs/
├── docker/
│   └── docker-compose.yml
├── dados/raw/          # CSVs Olist (não versionados)
├── lineage/events/     # Eventos JSON (não versionados)
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

---

## 🚀 Como Rodar

### ▶️ Opção 1 — Google Colab (recomendado)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/GuOMartins/DataLineage/blob/main/notebooks/datalineage_v5_colab.ipynb)

1. Execute a **Célula 1** → `Runtime → Restart runtime`
2. Execute as **Células 2 a 12** em ordem
3. Execute a **Célula 13** (pipeline completo)
4. Execute a **Célula 15** (dashboard via ngrok)

> Você precisará de um token gratuito do [ngrok.com](https://ngrok.com).

### 💻 Opção 2 — Local

```bash
git clone https://github.com/GuOMartins/DataLineage.git
cd DataLineage
python -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate
pip install --upgrade pip && pip install -r requirements.txt

# Configure ~/.kaggle/kaggle.json com suas credenciais Kaggle
# Depois:
cd docker && docker compose up -d && cd ..
export PROJECT_ROOT=$(pwd) OPENLINEAGE_URL=http://localhost:5000

python orquestrador/pipeline_flow.py
PROJECT_ROOT=$(pwd) streamlit run visualizacao/dashboard.py
```

---

## 📦 Dependências

```
prefect>=2.16,<3.0        great-expectations>=0.18,<1.0
duckdb>=0.10              openlineage-python>=1.8
streamlit>=1.30           plotly>=5.18
pandas>=2.0               numpy>=1.24,<2.0
kagglehub>=0.2            pyngrok
```

---

## 🗺️ Roadmap

- [x] Pipeline end-to-end: CSV → raw → validated → stg → fact → mart
- [x] Todo job com INPUT e OUTPUT real no OpenLineage
- [x] Great Expectations 0.18+ com fallback duplo
- [x] Dashboard Plotly com 6 abas interativas
- [x] Grafo de lineage com nós e arestas por camada
- [x] Compatibilidade Colab, VS Code, Jupyter
- [ ] Integração com Marquez real (Docker Compose)
- [ ] Alertas via Slack quando GE falha
- [ ] Deploy no Prefect Cloud
- [ ] Integração com AWS S3 + Athena
- [ ] Testes unitários com pytest

---

## ⚠️ Limitações

- Marquez Mock salva localmente — use Docker Compose para o grafo no Marquez UI
- DuckDB adequado até ~10 GB — para mais, considere BigQuery ou Redshift
- Evite Python 3.13 — algumas bibliotecas ainda sem wheels
- Projeto educacional — produção requer testes de carga e conformidade LGPD

---

## 🤝 Como Contribuir

```bash
git checkout -b feature/minha-melhoria
git commit -m "feat: integração com Marquez real"
git push origin feature/minha-melhoria
# Abra um Pull Request
```

---

## 📄 Licença

MIT — veja [`LICENSE`](LICENSE).

---

## 👤 Autor

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Conectar-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/gustavoomartins)
[![GitHub](https://img.shields.io/badge/GitHub-Seguir-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/GuOMartins)

<div align="center">
  <sub>⭐ Se este projeto foi útil, deixe uma estrela — ajuda muito!</sub>
</div>
