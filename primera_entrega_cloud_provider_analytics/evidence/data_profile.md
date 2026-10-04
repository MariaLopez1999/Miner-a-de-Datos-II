# Evidencia mínima de lectura y exploración

## Estructura encontrada

```text
README.txt
datalake/landing/
├── customers_orgs.csv
├── users.csv
├── resources.csv
├── support_tickets.csv
├── marketing_touches.csv
├── nps_surveys.csv
├── billing_monthly.csv
└── usage_events_stream/
    ├── events_part_0000.jsonl
    ├── ...
    └── events_part_0119.jsonl
```

## Conteos

| Fuente | Registros |
|---|---:|
| customers_orgs | 80 |
| users | 800 |
| resources | 400 |
| support_tickets | 1.000 |
| marketing_touches | 1.500 |
| nps_surveys | 92 |
| billing_monthly | 240 |
| usage events | 43.200 |
| archivos JSONL | 120 |

## Hallazgos de calidad

- `customers_orgs.nps_score`: 11 nulos; rango observado -38 a 101.
- `users.last_login`: 139 nulos.
- `resources.tags_json`: 83 nulos.
- `support_tickets.resolved_at`: 240 nulos.
- `support_tickets.csat`: 254 nulos; rango observado 0 a 7.
- `nps_surveys.nps_score`: 19 nulos; rango observado -16 a 68.
- `nps_surveys.comment`: 10 nulos.
- `billing_monthly.credits`: 137 nulos.
- Billing: 160 USD, 51 ARS y 29 EUR; meses 2025-06, 2025-07 y 2025-08.
- Eventos: 877 valores nulos, 2.075 unidades nulas.
- Eventos: 1.309 valores numéricos llegaron como string.
- Eventos: 216 costos negativos.
- Eventos: schema v1 = 10.800; schema v2 = 32.400.
- No se detectaron IDs de evento duplicados en el perfil inicial.
- No se detectaron organizaciones huérfanas en las fuentes tabulares que referencian `org_id`.

## Evolución de esquema

- v1: 2025-07-03 a 2025-07-17.
- v2: 2025-07-18 a 2025-08-31.
- v2 incorpora `carbon_kg` y, para el servicio GenAI, `genai_tokens`.

## Nota

Estos resultados corresponden al perfil inicial del dataset provisto. No reemplazan las validaciones que deberán ejecutarse en el pipeline de la segunda evaluación.
