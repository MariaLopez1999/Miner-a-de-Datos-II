# Cloud Provider Analytics — Primera Evaluación Parcial
## Diseño y fundación de datos

**Asignatura:** Minería de Datos II — ISTEA  
**Proyecto:** Cloud Provider Analytics  
**Instancia:** Primera evaluación parcial  
**Alcance:** diseño, perfil de fuentes, arquitectura y fundación del Data Lake. No se implementa aún el pipeline end-to-end.

---

## 1. Interpretación del problema

El caso representa al área de datos de un proveedor de nube que debe integrar información de clientes, usuarios, recursos, facturación, soporte, marketing, NPS y eventos de uso para habilitar analítica en tres dominios: **FinOps, Soporte y Producto/Usage**.

La necesidad combina dos tiempos de procesamiento:

- **Near real-time:** uso, consumo y costos incrementales a partir de eventos.
- **Batch:** maestros, facturación y fuentes de referencia.

El desafío central no es solamente almacenar los datos, sino **conformarlos, controlar su calidad, resolver evolución de esquema y publicarlos en una capa de consumo consultable**.

### Usuarios y necesidades

| Usuario / dominio | Necesidad |
|---|---|
| FinOps | Costos, consumo, revenue, créditos, impuestos, anomalías y eficiencia por organización y servicio. |
| Soporte | Volumen de tickets, severidad, SLA y CSAT por organización y fecha. |
| Producto / Usage | Uso de servicios, requests, métricas operativas, tokens GenAI y carbono cuando estén disponibles. |
| Equipo de datos | Ingestar, transformar, controlar calidad, mantener trazabilidad y servir datos reproducibles. |

### Preguntas principales

1. ¿Cuánto consumo y costo tiene cada organización por servicio y día?
2. ¿Qué servicios concentran el costo y cómo evoluciona ese costo?
3. ¿Existen incrementos de costo anómalos que requieran investigación?
4. ¿Cómo evolucionan los tickets críticos y el incumplimiento de SLA?
5. ¿Cuál es el revenue mensual por organización luego de créditos, impuestos y conversión a USD?
6. ¿Cuánto uso de GenAI existe y cuál es su costo estimado cuando el dato está disponible?
7. ¿Podemos reprocesar datos sin duplicarlos y mantener trazabilidad hasta la fuente?

### Objetivos medibles de la fundación

- Mantener **100 % de las fuentes Landing sin modificación**.
- Poder distinguir y procesar por separado **batch y streaming**.
- Registrar en Bronze `ingest_ts` y `source_file`.
- Diseñar particiones y naming reproducibles para Parquet.
- Resolver explícitamente nulos, tipos ambiguos, outliers y evolución v1/v2.
- Dejar Gold preparado para los marts de FinOps, Soporte y Producto.
- Mantener trazabilidad fuente → zona → mart → serving.

---

## 2. ¿Por qué Big Data? Análisis de las 5V

La escala actual del dataset es sintética y acotada, por lo que no se debe afirmar que los archivos actuales sean masivos por volumen absoluto. La necesidad de una arquitectura Big Data surge principalmente de la **combinación de volumen potencial, velocidad, variedad, veracidad y valor**, y de la necesidad de que la solución pueda escalar.

| V | Evidencia del caso | Decisión |
|---|---|---|
| **Volumen** | 43.200 eventos en 120 archivos JSONL más múltiples fuentes tabulares. En producción, los eventos de uso crecerían continuamente. | Parquet particionado + procesamiento distribuido con Spark. |
| **Velocidad** | Los eventos se fragmentan para simular micro-lotes y requieren near real-time. | Structured Streaming, checkpoints, watermark y deduplicación. |
| **Variedad** | CSV y JSONL; maestros, tickets, billing, encuestas y eventos; múltiples servicios y monedas. | Esquemas explícitos, zonas del Data Lake y normalización en Silver. |
| **Veracidad** | Nulos, tipos ambiguos, costos negativos, outliers y evolución de esquema. | Reglas de calidad, flags, quarantine, casteos controlados y compatibilidad v1/v2. |
| **Valor** | FinOps, Soporte y Producto necesitan métricas accionables para costos, SLA, uso y eficiencia. | Gold por dominio + serving query-first en Cassandra/AstraDB. |

**Conclusión:** la arquitectura se justifica por el comportamiento del sistema y su escalabilidad, no por presentar el dataset de laboratorio como si ya tuviera escala productiva.

---

## 3. Inventario y perfil inicial de fuentes

| Fuente | Registros | Grano | Frecuencia / uso | Calidad / riesgos |
|---|---:|---|---|---|
| `customers_orgs.csv` | 80 | 1 fila por organización | Maestro / batch | NPS nulo y valores fuera del rango esperado; requiere validación. |
| `users.csv` | 800 | 1 fila por usuario | Maestro / batch | `last_login` nulo en 139 registros; fechas como texto. |
| `resources.csv` | 400 | 1 fila por recurso cloud | Maestro / batch | `tags_json` nulo en 83; JSON embebido dentro de CSV. |
| `support_tickets.csv` | 1.000 | 1 fila por ticket | Batch | `resolved_at` nulo en 240; CSAT nulo en 254 y valores atípicos. |
| `marketing_touches.csv` | 1.500 | 1 interacción por touch | Batch | Booleanos representados como texto; debe tipificarse. |
| `nps_surveys.csv` | 92 | 1 encuesta por organización/fecha | Batch | NPS nulo en 19 y comentarios nulos en 10. |
| `billing_monthly.csv` | 240 | 1 factura por organización/mes | Mensual / batch | 3 monedas; `credits` nulo en 137; requiere FX y normalización. |
| `usage_events_stream/*.jsonl` | 43.200 | 1 evento de uso | Streaming / micro-lotes | 877 valores nulos, 2.075 unidades nulas, 1.309 valores numéricos como string, 216 costos negativos; evolución v1/v2. |

### Evidencia de eventos

- 120 archivos JSONL.
- 43.200 eventos.
- Rango temporal: **2025-07-03 a 2025-08-31**.
- Schema v1: 10.800 eventos, hasta 2025-07-17.
- Schema v2: 32.400 eventos, desde 2025-07-18.
- En v2 aparecen `carbon_kg` y, para GenAI, `genai_tokens`.
- No se detectaron IDs de evento duplicados en la exploración inicial.
- Los costos incluyen 216 valores negativos; el máximo observado fue 317,4308 USD y el mínimo -154,4608 USD.

### Trazabilidad y relaciones

Las fuentes tabulares que referencian `org_id` no presentan organizaciones huérfanas respecto de `customers_orgs.csv` en la exploración inicial. Los identificadores principales de usuarios y recursos tampoco presentan duplicados en los archivos analizados.

Esto no reemplaza controles de calidad futuros: las mismas relaciones deben validarse en Bronze/Silver.

---

## 4. Arquitectura de alto nivel

### Patrón seleccionado: híbrido

Se selecciona un **patrón híbrido** porque el caso tiene dos naturalezas claramente distintas:

1. **Batch:** clientes, usuarios, recursos, soporte, marketing, NPS y facturación.
2. **Streaming:** eventos de uso fragmentados que requieren procesamiento incremental.

Se evita forzar todos los datos a streaming, lo que agregaría complejidad innecesaria a fuentes naturalmente batch. También se evita una solución exclusivamente batch porque perdería la capacidad near real-time solicitada para usage/costos incrementales.

### Flujo

**Fuentes → Ingesta → Data Lake → Procesamiento batch/streaming → Serving → Consumo**

Con capacidades transversales de:

**calidad + metadatos + linaje + seguridad + observabilidad + idempotencia + gobierno**

### Componentes propuestos

| Capa | Componente | Responsabilidad |
|---|---|---|
| Fuentes | CSV / JSONL | Datos de negocio y eventos. |
| Ingesta batch | PySpark | Lectura de CSV/JSON, esquemas explícitos y escritura Bronze. |
| Ingesta streaming | Spark Structured Streaming | JSONL incremental, watermark, checkpoint y dedupe. |
| Landing | Archivos originales | Raw inmutable. |
| Bronze | Parquet | Mismo grano de fuente, tipos explícitos, columnas técnicas. |
| Silver | Parquet | Normalización, joins, calidad, conformado y evolución v1/v2. |
| Gold | Parquet | Marts de negocio por dominio. |
| Serving | Cassandra/AstraDB | Persistencia query-first para consultas de consumo. |
| Consumo | Herramientas de visualización / analítica | Consultas y métricas de FinOps, Soporte y Producto. |

---

## 5. Diseño del Data Lake

### Landing

- Contiene exactamente los archivos originales.
- **Inmutable**.
- No se corrigen datos directamente.
- Debe conservarse el nombre del archivo como elemento de trazabilidad.

### Bronze

Responsabilidad: representar el mismo grano de la fuente con tipos explícitos.

Columnas técnicas mínimas:

- `ingest_ts`
- `source_file`

Para eventos:

- `event_id`
- `timestamp`
- `schema_version`
- atributos originales conformados.

Formato: **Parquet**.

Particionamiento inicial propuesto:

- Eventos: `event_date` y, si el volumen lo justifica, `service`.
- Fuentes batch temporales: por fecha/mes cuando corresponda.
- Maestros pequeños: evitar sobreparticionar.

### Silver

Responsabilidad:

- casteo de números, booleanos y fechas;
- normalización de regiones y servicios;
- tratamiento de nulos;
- detección/flag de outliers;
- joins con dimensiones;
- compatibilidad entre schema v1 y v2;
- deduplicación cuando corresponda;
- derivación de métricas como `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens` y `carbon_kg` cuando existan.

### Gold

Marts orientados al consumo:

- `org_daily_usage_by_service`
- `revenue_by_org_month`
- `cost_anomaly_mart`
- `tickets_by_org_date`
- `genai_tokens_by_org_date`

Los nombres son los propuestos por la consigna y deberán conservar el grano indicado al implementar.

### Naming

Convención propuesta:

`<zona>/<dominio>/<entidad>/dt=<YYYY-MM-DD>/`

Para particiones:

- usar claves de baja cardinalidad;
- evitar particionar por identificadores de alta cardinalidad como `event_id`;
- utilizar `event_date` como partición principal para eventos.

### Retención

Para esta etapa se propone:

- **Landing:** retención larga / definida por política de negocio, por ser fuente de reproceso.
- **Bronze:** conservar al menos el horizonte necesario para reprocesar y auditar.
- **Silver:** conservar el período requerido por los marts y backfills.
- **Gold:** conservar según necesidad analítica.

La duración exacta queda abierta porque el caso académico no define una política de retención concreta.

### Metadatos y promoción

Cada dataset debe documentar:

- nombre;
- dueño/responsable;
- descripción;
- grano;
- esquema;
- particiones;
- fecha de actualización;
- origen;
- reglas de calidad;
- nivel de sensibilidad/acceso.

Promoción:

**Landing → Bronze:** lectura exitosa + schema esperado.  
**Bronze → Silver:** tipos válidos + reglas mínimas de calidad + conformado.  
**Silver → Gold:** métricas de negocio consistentes + joins validados.  
**Gold → Serving:** mart estable y con modelo query-first.

---

## 6. Flujo batch y streaming

### Batch

1. Leer CSV desde Landing con schema explícito.
2. Registrar `source_file` e `ingest_ts`.
3. Castear tipos.
4. Escribir Bronze Parquet.
5. Aplicar reglas de calidad.
6. Conformar dimensiones/hechos en Silver.
7. Construir Gold por dominio.
8. Publicar los marts necesarios en Cassandra/AstraDB.

### Streaming

1. Leer `usage_events_stream/*.jsonl` como fuente de Structured Streaming.
2. Aplicar schema explícito.
3. Normalizar timestamp.
4. Usar watermark para eventos tardíos.
5. Deduplicar por `event_id`.
6. Enviar registros inválidos a quarantine.
7. Persistir Bronze Parquet con checkpoint.
8. Transformar a Silver y derivar métricas.
9. Agregar a Gold según ventanas/grano de negocio.
10. Servir los marts.

---

## 7. Flujo batch como MapReduce conceptual

Ejemplo: construir `org_daily_usage_by_service`.

### Map

Entrada: evento de uso limpio.

Clave:

`(org_id, usage_date, service)`

Valor:

`{cost_usd_increment, requests, cpu_hours, storage_gb_hours, genai_tokens, carbon_kg}`

El mapper convierte cada evento en un registro de métrica según su `metric`/`unit`.

### Shuffle / Group

Spark agrupa todos los registros con la misma clave:

`(org_id, usage_date, service)`

Esto permite distribuir el cálculo por organización, fecha y servicio.

### Reduce

Se agregan los valores:

- `SUM(cost_usd_increment)`
- `SUM(requests)`
- `SUM(cpu_hours)`
- `SUM(storage_gb_hours)`
- `SUM(genai_tokens)` cuando exista
- `SUM(carbon_kg)` cuando exista

Resultado:

`org_daily_usage_by_service`

### Equivalencia Spark

Conceptualmente:

`select → withColumn → groupBy(org_id, usage_date, service) → agg(sum(...)) → write Parquet`

Spark realiza internamente las etapas de distribución equivalentes a Map/Shuffle/Reduce.

---

## 8. Matriz 5V → arquitectura

| 5V / requisito | Decisión | Componente |
|---|---|---|
| Volumen | Formato columnar y procesamiento distribuido | Parquet + PySpark |
| Velocidad | Procesamiento incremental | Structured Streaming |
| Variedad | Esquemas por fuente y zonas | Landing/Bronze/Silver |
| Veracidad | Calidad y quarantine | Silver + reglas + Parquet quarantine |
| Valor | Marts por dominio | Gold + Cassandra/AstraDB |
| Evolución v1/v2 | Compatibilización de schema | Silver |
| Late data | Watermark | Structured Streaming |
| Duplicados | Dedupe/upsert | Streaming + Serving |
| Consultas | Modelo query-first | Cassandra/AstraDB |
| Trazabilidad | `source_file`, `ingest_ts`, metadatos | Bronze + gobierno |

---

## 9. Supuestos, riesgos y mitigaciones

| Riesgo / supuesto | Impacto | Mitigación |
|---|---|---|
| El volumen académico no representa producción | Puede subestimarse el diseño | Diseñar con escalabilidad y no con números absolutos del laboratorio. |
| Evolución de schema | Fallos de parsing o pérdida de campos | Versionar schema y compatibilizar v1/v2 en Silver. |
| Costos negativos | Métricas financieras incorrectas | Flag/quarantine y regla de negocio explícita. |
| Valores numéricos como texto | Errores de agregación | Cast controlado con fallback. |
| Eventos tardíos | Agregaciones incompletas | Watermark y política de late data. |
| Duplicados en reintentos | Sobreconteo | `event_id`, checkpoint y upserts. |
| Sobreparticionado | Muchos archivos pequeños / degradación | Particionar por fecha y controlar `coalesce/repartition`. |
| Cassandra modelada después de las consultas | Consultas lentas o imposibles | Diseñar query-first desde los marts. |
| Retención no definida | Costos y gobierno inciertos | Dejar política parametrizable y documentar decisión. |
| Entorno Colab | Persistencia/configuración limitada | Dataset de demo pequeño + instrucciones reproducibles. |

---

## 10. Estimación preliminar

Estimación para la fundación + primera implementación mínima posterior, no como compromiso contractual.

| Rol | Esfuerzo aproximado |
|---|---:|
| Data Engineer / Spark | 3–4 jornadas |
| Data Engineer / Streaming | 2–3 jornadas |
| Data / Analytics modeler | 1–2 jornadas |
| QA / Data Quality | 1 jornada |
| Documentación / arquitectura | 1 jornada |

Para la primera entrega concreta, el foco debe permanecer en diseño, perfilado, arquitectura, Data Lake, decisiones y evidencia mínima; la implementación profunda pertenece a la segunda evaluación.

---

## 11. Próximos pasos

1. Validar arquitectura y patrón híbrido con feedback docente.
2. Implementar batch a Bronze para al menos tres maestros.
3. Implementar Structured Streaming para los eventos.
4. Incorporar watermark, dedupe y checkpoint.
5. Construir Silver y reglas de calidad/quarantine.
6. Crear `org_daily_usage_by_service` en Gold.
7. Modelar Cassandra/AstraDB query-first.
8. Demostrar idempotencia y reproducibilidad.
9. Actualizar diagrama para reflejar la implementación real.

---

## 12. Checklist de primera entrega

- [x] Interpretación del caso y usuarios.
- [x] Objetivos y preguntas principales.
- [x] Justificación mediante 5V.
- [x] Inventario y perfil inicial de fuentes.
- [x] Arquitectura v1.
- [x] Patrón híbrido justificado.
- [x] Diseño Landing/Bronze/Silver/Gold.
- [x] Flujos batch y streaming.
- [x] Lógica MapReduce conceptual.
- [x] Matriz requisito-componente.
- [x] Supuestos, riesgos y mitigaciones.
- [x] Estimación preliminar.
- [x] README y evidencia mínima de exploración.

### Fuentes de esta entrega

- Consigna oficial del Proyecto Integrador — Minería de Datos II, ISTEA, 2C 2026.
- `cloud_provider_challenge_dataset_v1.zip`, README y archivos de Landing.
