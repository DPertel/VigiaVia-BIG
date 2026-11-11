-- Databricks (Free Edition) | Setup único. Execute em um notebook SQL ou no SQL Editor.
-- Catálogo "workspace" já existe no Free Edition.
CREATE SCHEMA IF NOT EXISTS workspace.gs COMMENT 'Global Solution - VigiaVia';

-- Volume onde os arquivos da pasta chegada/ são enviados pela interface (Catalog > workspace > gs > chegada > Upload).
CREATE VOLUME IF NOT EXISTS workspace.gs.chegada;

-- Estrutura de pastas esperada no Volume (crie pela interface ao subir os lotes):
--   /Volumes/workspace/gs/chegada/clima/    clima_2026-11-01.json ...
--   /Volumes/workspace/gs/chegada/relatos/  relatos_2026-11-01.json ...
--   /Volumes/workspace/gs/chegada/_controle/  (schema e checkpoints do Auto Loader - criado automaticamente)
-- Lote 1: arquivos dos dias 01 a 03.  Lote 2: arquivos dos dias 04 e 05 (requisito 04).
