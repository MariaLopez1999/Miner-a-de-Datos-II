# Matriz requisito → componente

| Requisito de primera entrega | Componente / decisión | Evidencia |
|---|---|---|
| Problema, usuarios y objetivos | Dominios FinOps, Soporte, Producto/Usage | `diseno_primera_entrega.md` §1 |
| 5V | Arquitectura orientada a escalabilidad, streaming y calidad | §2 |
| Inventario de fuentes | Perfil de 7 CSV + 120 JSONL | §3 + `evidence/data_profile.md` |
| Arquitectura de alto nivel | Fuentes → ingesta → DL → procesamiento → serving → consumo | `arquitectura_v1.mmd` |
| Patrón arquitectónico | Híbrido | §4 + `DECISIONS.md` |
| Data Lake | Landing/Bronze/Silver/Gold + Parquet | §5 |
| Batch | PySpark | §6 |
| Streaming | Structured Streaming | §6 |
| MapReduce | Map → Shuffle/Group → Reduce | §7 |
| Calidad | reglas, flags y quarantine | §5, §9 |
| Evolución de schema | compatibilidad v1/v2 en Silver | §5, §9 |
| Serving | Cassandra/AstraDB query-first | §4, §5 |
| Supuestos y riesgos | matriz de riesgos | §9 |
| Esfuerzo | roles y jornadas | §10 |
| Repositorio inicial | README + docs + evidence | estructura del repositorio |
