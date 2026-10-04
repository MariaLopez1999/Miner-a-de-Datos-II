Cloud Provider Analytics — Primera entrega

## Alcance
Esta carpeta contiene la **Primera Evaluación Parcial: Diseño y fundación de datos** del proyecto integrador de Minería de Datos II (ISTEA, 2C 2026).

## Estructura
- `docs/diseno_primera_entrega.md`: documento principal.
- `docs/arquitectura_v1.mmd`: diagrama de arquitectura en Mermaid.
- `docs/matriz_requisitos.md`: trazabilidad requisito → componente → evidencia.
- `docs/DECISIONS.md`: decisiones, supuestos, trade-offs y cuestiones abiertas.
- `evidence/data_profile.md`: evidencia mínima de lectura y exploración del dataset.

## Datos analizados
El ZIP provisto contiene:
- 7 fuentes CSV en `datalake/landing/`.
- 120 archivos JSONL en `datalake/landing/usage_events_stream/`.
- 43.200 eventos de uso.
- Evolución de esquema: v1 hasta 2025-07-17 y v2 desde 2025-07-18.
- Inconsistencias, nulos, costos negativos y outliers intencionales.

## Decisión arquitectónica
Se propone un **patrón híbrido**:
- Batch: clientes/organizaciones, usuarios, recursos, soporte, marketing, NPS y facturación.
- Streaming: `usage_events_stream/*.jsonl` mediante Structured Streaming.
- Data Lake: Landing → Bronze → Silver → Gold, usando Parquet desde Bronze.
- Serving objetivo: Cassandra/AstraDB, modelado query-first.

## Qué queda para la segunda entrega
La implementación de PySpark, Structured Streaming, watermark, deduplicación, quarantine, Silver/Gold y Cassandra/AstraDB corresponde a la segunda evaluación.
md…]()
