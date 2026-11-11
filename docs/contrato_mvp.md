# Contrato Ouro → MVP (requisito 08)

**Tabela da Ouro consumida pelo MVP:** `workspace.gs.vw_mvp_fila_zeladoria` (1 linha por buraco / célula de ~110 m).
**Entrega:** `fila_zeladoria.json` no Volume `/Volumes/workspace/gs/chegada/saida/` (gerado no fim do notebook `03_ouro.py`).
**Tela que exibe esses dados:** *Fila da zeladoria* do VigiaVia (agentes das subprefeituras) — https://ca-vigiavia-prod.happywave-0bf5b4c3.brazilsouth.azurecontainerapps.io

| Campo | Tipo | Significado | Dado pessoal? |
|---|---|---|---|
| `id_local` | string | célula `lat3_lon3` (3 casas decimais) | não (localização do buraco, ~110 m) |
| `via` | string | trecho/via | não |
| `bairro` | string | bairro do piloto | não |
| `lat_aprox`, `lon_aprox` | double | centro aproximado da célula | não |
| `relatos` | int | relatos recebidos para o buraco | não |
| `relatores_distintos` | int | quantos aparelhos diferentes relataram (contagem de hashes) | não — só contagem |
| `severidade_max` | int | 1 baixa · 2 média · 3 alta | não |
| `ultimo_relato_em` | timestamp | data/hora do relato mais recente | não |
| `status_prioritario` | string | `pendente` antes de `confirmado` | não |

**Regras:** relatos `descartado` ficam fora; ordenação por `severidade_max` e `relatos`; o JSON **não** contém `id_relator`
(nem em hash) nem coordenada exata.

**Cobertura de LGPD:** minimização (sem imagem, sem identificador direto), pseudonimização (`sha2(sal + id_relator)` na
`dim_relator`/`fato_relato`, sal fora do repositório) e generalização espacial (3 casas decimais).
