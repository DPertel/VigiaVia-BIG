# Roteiro do relatório PDF (ordem exigida pela disciplina)

> Monte o PDF **nesta ordem**. Cada print precisa de legenda e estar legível (prints ilegíveis/sem legenda: até −1,0).
> Enviar um PDF com o link do repositório no Teams até **19/11, 23h55** (um integrante). Repositório público e no ar durante a correção.

## 01 · Integrantes
| Integrante | RM |
|---|---|
| Deivison Pertel | 550803 |
| Eduardo Akira Murata | 98713 |
| Wesley Souza de Oliveira | 97874 |
(Nome completo ou RM ausente: −0,5. Confira os nomes completos com a secretaria/Teams.)

## 02 · O produto em uma frase e as perguntas que o MVP responde
**VigiaVia** detecta buracos com câmeras e entrega à zeladoria um relato único, georreferenciado e priorizado por buraco.
Perguntas: (1) Qual é a fila da zeladoria? (2) Onde estão os pontos mais críticos? (3) A chuva antecede mais relatos?
(4) As detecções do modelo são confiáveis? (5) Quantos chamados duplicados o produto evita?

## 03 · Diagrama do fluxo de dados (fonte → MVP)
Use o diagrama do README (Mermaid) renderizado ou redesenhe: Open-Meteo + relatos → Airflow → `chegada/` → Volume → Bronze → Prata → Ouro → fila da zeladoria.

## 04 · Tabela de fontes
| Fonte | Origem | Formato | Frequência | Observação |
|---|---|---|---|---|
| Clima | API Open-Meteo (externa, real) | JSON (listas horárias) | diária (@daily) | sem chave |
| Relatos VigiaVia | simulados no formato do MVP | JSON (array de objetos) | diária | `origem_dado = demonstracao`; justificativa em `dags/vigiavia_fontes.py` |

## 05 · Requisitos 01 a 09, na ordem (um print + explicação curta cada)
| Req. | O que mostrar no print |
|---|---|
| 01 | Resposta da API Open-Meteo no navegador/Postman + exemplo de relato |
| 02 | Código do DAG + arquivo gerado em `chegada/` com a data no nome |
| 03 | Visão Graph (2 tarefas paralelas + conferir) e a tela de Variables (sem chaves no código) |
| 04 | Volume do Databricks com os arquivos do lote 1 e do lote 2 |
| 05 | Notebook `01_bronze` com contagens (ver item 07) |
| 06 | Notebook `02_prata`: tabela `prata_qualidade_log` e `SELECT COUNT(*) vs COUNT(DISTINCT chave)` |
| 07 | Tabelas `fato_relato`, `dim_*` e as 5 consultas (ver item 08) |
| 08 | `fato_relato` sem lat/lon e sem `id_relator` + `vw_mvp_fila_zeladoria` + a tela do MVP |
| 09 | Página do repositório no GitHub com README |

## 06 · Prints do Airflow
Visão **Graph** e **Grid** com execuções concluídas (todas as tarefas verdes, vários dias via catchup).

## 07 · Contagens da Bronze
Tabela: `bronze_clima` e `bronze_relatos` — (a) após o lote 1; (b) após o lote 2; (c) após rodar de novo sem arquivos novos.
Esperado: (b) > (a) só pelos arquivos novos; (c) = (b).

## 08 · As 5 consultas SQL
`databricks/04_consultas.sql`: resultado em tabela + 2–3 linhas de interpretação **com os números reais da sua execução** (C5 é a de impacto).

## 09 · Conclusão técnica e link do repositório
Decisões (Auto Loader, MERGE, hash com sal, célula de 110 m), limitações (relatos simulados; célula ≠ deduplicação exata do MVP;
Free Edition sem conexão direta com o Airflow → upload em lotes) e `https://github.com/<usuario>/<repo>`.
