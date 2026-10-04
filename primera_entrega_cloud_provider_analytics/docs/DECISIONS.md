# DECISIONS.md — Primera entrega

## D-001 — Patrón híbrido
**Decisión:** combinar batch y streaming.

**Motivo:** las fuentes de referencia y facturación tienen naturaleza batch, mientras que `usage_events_stream` fue diseñado para Structured Streaming y el caso exige near real-time.

**Alternativas descartadas:**
- Lambda completo: válido, pero introduce duplicación conceptual si se separan demasiado las lógicas.
- Kappa: no aporta una ventaja clara para los maestros y billing naturalmente batch.

## D-002 — Parquet como formato intermedio
**Decisión:** Bronze/Silver/Gold en Parquet.

**Motivo:** formato columnar adecuado para Spark, permite particionamiento y facilita lecturas analíticas.

## D-003 — Landing inmutable
**Decisión:** no modificar los archivos originales.

**Motivo:** la consigna exige raw inmutable y se necesita trazabilidad/reprocesamiento.

## D-004 — Particionar eventos por fecha
**Decisión:** usar `event_date` como partición primaria; `service` puede agregarse sólo si el volumen real lo justifica.

**Motivo:** evita particiones por claves de alta cardinalidad y permite pruning temporal.

## D-005 — Compatibilidad v1/v2 en Silver
**Decisión:** mantener un modelo conformado que soporte campos opcionales de v2.

**Motivo:** desde 2025-07-18 aparecen `carbon_kg` y `genai_tokens` para GenAI.

## D-006 — Calidad antes de Gold
**Decisión:** los problemas de calidad se tratan/flaggean antes de construir marts.

**Motivo:** Gold debe contener métricas de negocio consistentes; los registros inválidos deben poder auditarse mediante quarantine.

## D-007 — Cassandra/AstraDB query-first
**Decisión:** diseñar tablas desde las consultas requeridas.

**Motivo:** Cassandra prioriza patrones de consulta conocidos y no un modelo relacional genérico.

## Abiertas
- Política exacta de retención.
- Umbrales finales de outliers.
- Estrategia exacta de quarantine para cada fuente.
- Tamaño/volumen objetivo de producción.
- Consultas definitivas y claves de partición de Cassandra.
