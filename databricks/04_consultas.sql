-- 5 consultas SQL da Ouro que respondem perguntas do MVP (requisito 07). Cada uma: pergunta → SQL → como interpretar.
-- Cole o resultado e a interpretação (com os SEUS números) no relatório, item 08.

-- ======================================================================================================
-- C1 · Qual é a fila da zeladoria hoje? (tela "Fila da zeladoria")
-- Quantos buracos únicos existem, por severidade e status.
-- ======================================================================================================
SELECT f.severidade, f.status, COUNT(*) AS relatos, COUNT(DISTINCT f.id_local) AS buracos_unicos
FROM workspace.gs.fato_relato f
GROUP BY f.severidade, f.status
ORDER BY CASE f.severidade WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END, f.status;
-- Interpretação: mostra onde está o acúmulo. Muitos "alta + pendente" = prioridade imediata para a subprefeitura.

-- ======================================================================================================
-- C2 · Onde estão os pontos mais críticos? (mapa de calor / priorização)
-- Top 10 locais por número de relatos e severidade máxima.
-- ======================================================================================================
SELECT l.via, l.lat_aprox, l.lon_aprox, COUNT(*) AS relatos, COUNT(DISTINCT f.id_relator_hash) AS relatores_distintos,
       MAX(CASE f.severidade WHEN 'alta' THEN 3 WHEN 'media' THEN 2 ELSE 1 END) AS severidade_max
FROM workspace.gs.fato_relato f JOIN workspace.gs.dim_local l USING (id_local)
WHERE f.status <> 'descartado'
GROUP BY l.via, l.lat_aprox, l.lon_aprox
ORDER BY severidade_max DESC, relatos DESC
LIMIT 10;
-- Interpretação: vários relatores distintos no mesmo local reforçam a confiança de que o buraco existe.

-- ======================================================================================================
-- C3 · A chuva antecede mais relatos? (cruza a fonte externa Open-Meteo com os relatos)
-- Relatos por dia, chuva do dia e chuva do dia anterior.
-- ======================================================================================================
SELECT d.data, c.chuva_mm, LAG(c.chuva_mm) OVER (ORDER BY d.data) AS chuva_dia_anterior_mm,
       COUNT(f.id_relato) AS relatos_novos, ROUND(AVG(c.temp_media_c), 1) AS temp_media_c
FROM workspace.gs.dim_data d
JOIN workspace.gs.fato_clima_dia c ON c.sk_data = d.sk_data
LEFT JOIN workspace.gs.fato_relato f ON f.sk_data = d.sk_data
GROUP BY d.data, c.chuva_mm
ORDER BY d.data;
-- Interpretação: compare relatos_novos com chuva_dia_anterior_mm. Com poucos dias, é indício, não prova: diga isso no relatório.
-- ATENÇÃO: se usar os relatos simulados, eles NÃO dependem da chuva (a relação esperada não aparece, e está tudo bem).

-- ======================================================================================================
-- C4 · As detecções do modelo são confiáveis? (precisão confirmada pela zeladoria)
-- Dos relatos já decididos por um agente, quantos foram confirmados, por origem (detector x cidadão).
-- ======================================================================================================
SELECT origem,
       COUNT(*) AS decididos,
       SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) AS confirmados,
       ROUND(100.0 * SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) / COUNT(*), 1) AS precisao_pct,
       ROUND(AVG(confianca), 2) AS confianca_media
FROM workspace.gs.fato_relato
WHERE status IN ('confirmado', 'descartado')
GROUP BY origem;
-- Interpretação: é o indicador "precisão das detecções confirmadas pela zeladoria" da ficha técnica.

-- ======================================================================================================
-- C5 · IMPACTO: quantos chamados duplicados o VigiaVia evita?  (indicador "% de chamados duplicados evitados")
-- Relatos brutos x buracos únicos (1 relato por célula de ~110 m) e tempo até a decisão da zeladoria.
-- ======================================================================================================
WITH base AS (
  SELECT COUNT(*) AS relatos_brutos, COUNT(DISTINCT id_local) AS buracos_unicos FROM workspace.gs.fato_relato
),
tempo AS (
  SELECT ROUND(AVG(horas_ate_atualizacao), 1) AS horas_ate_decisao
  FROM workspace.gs.fato_relato WHERE status IN ('confirmado', 'descartado')
)
SELECT b.relatos_brutos, b.buracos_unicos,
       b.relatos_brutos - b.buracos_unicos AS duplicados_evitados,
       ROUND(100.0 * (b.relatos_brutos - b.buracos_unicos) / b.relatos_brutos, 1) AS duplicados_evitados_pct,
       t.horas_ate_decisao
FROM base b CROSS JOIN tempo t;
-- Interpretação: cada relato além do primeiro numa mesma célula seria um chamado duplicado na fila manual. Liga-se aos ODS 11.2/11.3.
-- Limite: a célula de ~110 m é uma aproximação do "mesmo buraco" (a deduplicação do MVP pode usar outro critério).
