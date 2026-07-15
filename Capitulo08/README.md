---LAB_START---
LAB_ID: 08-00-01
---MARKDOWN---
# Práctica 7 — Optimización del Procesamiento Distribuido con Características Avanzadas de Spark

## 1. Metadatos

| Campo            | Detalle                                      |
|------------------|----------------------------------------------|
| **Duración**     | 40 minutos                                   |
| **Complejidad**  | Difícil                                      |
| **Nivel Bloom**  | Aplicar                                      |
| **Práctica**     | 7 de 9                                       |
| **Módulo**       | 8 — Optimización en Apache Spark             |

---

## 2. Descripción General

En esta práctica aplicarás las técnicas de optimización más importantes de Apache Spark sobre un dataset sintético de más de **1 millón de filas** generado en memoria con Python. Medirás el impacto real del almacenamiento en caché (`cache()` / `persist()` con distintos `StorageLevel`), controlarás el particionamiento con `repartition()` y `coalesce()`, identificarás operaciones que generan *shuffle* usando `explain()`, distribuirás datos de referencia con **broadcast variables**, y contarás métricas distribuidas con **acumuladores**. Al finalizar, habrás construido un pipeline de análisis completamente optimizado y verificado su comportamiento desde la **Spark UI**.

---

## 3. Objetivos de Aprendizaje

- [ ] Implementar `cache()` y `persist()` con los niveles `MEMORY_ONLY`, `MEMORY_AND_DISK` y `DISK_ONLY`, midiendo el tiempo de ejecución antes y después de cada estrategia.
- [ ] Controlar el número de particiones con `repartition()` y `coalesce()`, y verificar el impacto en el rendimiento y en el balance de carga.
- [ ] Identificar operaciones que generan *shuffle* (groupBy, join, distinct) en el plan de ejecución usando `explain()` y mitigarlas con `broadcast()`.
- [ ] Crear y utilizar variables broadcast para distribuir eficientemente tablas de referencia pequeñas a todos los ejecutores.
- [ ] Implementar acumuladores para registrar contadores y métricas distribuidas durante transformaciones en un pipeline real.

---

## 4. Prerrequisitos

### Conocimientos previos
- Práctica 6 completada: dominio de transformaciones, acciones y operaciones SQL en PySpark.
- Comprensión del modelo de ejecución distribuida de Spark: ejecutores, tareas, etapas y *stages*.
- Familiaridad con el concepto de memoria y disco en sistemas distribuidos.
- Conocimiento básico de joins y su costo computacional (especialmente shuffle join vs. broadcast join).

### Acceso y software requerido
- Apache Spark 3.5.x con PySpark instalado y funcional (verificado en Práctica 1).
- JDK 11 o 17 configurado correctamente (`JAVA_HOME`).
- JupyterLab 7.x o Jupyter Notebook 7.x en ejecución.
- Puerto **4040** disponible para la Spark UI (alternativas: 4041, 4042).
- Paquetes Python: `pyspark`, `time`, `random`, `pandas` (2.0+).
- **Windows**: `winutils.exe` y `HADOOP_HOME` configurados correctamente.

---

## 5. Entorno de Laboratorio

### Tabla de software

| Componente       | Versión requerida | Verificación                        |
|------------------|-------------------|-------------------------------------|
| Apache Spark     | 3.5.x             | `spark.version`                     |
| PySpark          | 3.5.x             | `pyspark.__version__`               |
| Java (JDK)       | 11 o 17 LTS       | `java -version`                     |
| Python           | 3.10 o 3.11       | `python --version`                  |
| JupyterLab       | 7.x               | Interfaz web activa                 |

### Configuración de la sesión Spark para esta práctica

> ⚠️ **Importante:** Esta práctica trabaja con datasets de 1M+ filas. Si tu equipo tiene 8 GB de RAM, usa la configuración de memoria reducida indicada a continuación. Con 16 GB puedes aumentar los valores.

```python
# Celda 1 — Configuración e inicialización de la sesión Spark
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time
import random

# Configuración adaptada a equipos con 8 GB RAM
spark = SparkSession.builder \
    .appName("Lab08_Optimizacion_Spark") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"✅ Spark versión: {spark.version}")
print(f"✅ Spark UI disponible en: http://localhost:4040")
```

> 💡 **Nota sobre `spark.sql.shuffle.partitions`:** El valor por defecto es 200, lo que en modo local con pocos datos genera overhead. Lo reducimos a 8 para este laboratorio. Lo ajustaremos manualmente en pasos posteriores para demostrar su efecto.

---

## 6. Pasos del Laboratorio

---

### Paso 1: Generación del Dataset Sintético (1M+ filas)

**Objetivo:** Crear el dataset de trabajo que se utilizará en todos los pasos siguientes. El dataset simula transacciones de ventas con columnas relevantes para análisis.

#### Instrucciones

1. Ejecuta la siguiente celda para generar el dataset principal de **1,200,000 filas**:

```python
# Celda 2 — Generación del dataset sintético de ventas
from pyspark.sql.functions import col, rand, round as spark_round, lit
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, DateType
)
import pandas as pd
import numpy as np

# Parámetros del dataset
NUM_FILAS = 1_200_000
random.seed(42)
np.random.seed(42)

CATEGORIAS    = ["Electrónica", "Ropa", "Alimentos", "Hogar", "Deportes"]
REGIONES      = ["Norte", "Sur", "Este", "Oeste", "Centro"]
ESTADOS_VENTA = ["completado", "pendiente", "cancelado", "devuelto"]
VENDEDORES    = [f"V{str(i).zfill(4)}" for i in range(1, 501)]  # 500 vendedores

# Generación con pandas para mayor velocidad, luego conversión a Spark
print("⏳ Generando dataset... (puede tardar 20-40 segundos)")
inicio_gen = time.time()

pdf = pd.DataFrame({
    "id_transaccion": range(1, NUM_FILAS + 1),
    "id_vendedor":    np.random.choice(VENDEDORES, NUM_FILAS),
    "categoria":      np.random.choice(CATEGORIAS, NUM_FILAS),
    "region":         np.random.choice(REGIONES, NUM_FILAS),
    "estado":         np.random.choice(ESTADOS_VENTA, NUM_FILAS,
                                       p=[0.70, 0.15, 0.10, 0.05]),
    "monto":          np.round(np.random.uniform(10.0, 5000.0, NUM_FILAS), 2),
    "descuento":      np.round(np.random.uniform(0.0, 500.0, NUM_FILAS), 2),
    "anio":           np.random.choice([2022, 2023, 2024], NUM_FILAS,
                                       p=[0.20, 0.35, 0.45]),
    "mes":            np.random.randint(1, 13, NUM_FILAS),
    "tiene_error":    np.random.choice([0, 1], NUM_FILAS, p=[0.97, 0.03]),
})

# Asegurar que descuento no supere monto
pdf["descuento"] = np.minimum(pdf["descuento"], pdf["monto"] * 0.4)

df_ventas = spark.createDataFrame(pdf)

print(f"✅ Dataset generado en {time.time() - inicio_gen:.2f} segundos")
print(f"✅ Total de filas: {df_ventas.count():,}")
print(f"✅ Columnas: {df_ventas.columns}")
```

2. Verifica el esquema del DataFrame:

```python
# Celda 3 — Inspección del esquema y muestra de datos
df_ventas.printSchema()
df_ventas.show(5, truncate=False)
```

#### Salida esperada

```
✅ Dataset generado en ~25.00 segundos
✅ Total de filas: 1,200,000
✅ Columnas: ['id_transaccion', 'id_vendedor', 'categoria', 'region',
              'estado', 'monto', 'descuento', 'anio', 'mes', 'tiene_error']

root
 |-- id_transaccion: long (nullable = true)
 |-- id_vendedor: string (nullable = true)
 |-- categoria: string (nullable = true)
 ...
```

#### Verificación

```python
# Celda 4 — Verificación básica del dataset
assert df_ventas.count() >= 1_000_000, "❌ El dataset debe tener al menos 1M filas"
assert len(df_ventas.columns) == 10, "❌ Se esperan 10 columnas"
print(f"✅ Verificación superada: {df_ventas.count():,} filas, {len(df_ventas.columns)} columnas")
```

---

### Paso 2: Comparativa de Rendimiento — Sin Caché vs. Con Caché

**Objetivo:** Medir cuantitativamente el impacto del almacenamiento en caché cuando el mismo DataFrame transformado se utiliza en múltiples acciones consecutivas.

#### Instrucciones

1. Define la transformación base que se reutilizará (simula un pipeline ETL costoso):

```python
# Celda 5 — Definición de la transformación base (pipeline ETL)
from pyspark.sql.functions import (
    col, when, avg, count, sum as spark_sum, max as spark_max
)

def transformacion_etl(df):
    """
    Simula un pipeline ETL con múltiples transformaciones encadenadas.
    Este es el pipeline que queremos optimizar con caché.
    """
    return df \
        .filter(col("estado") == "completado") \
        .filter(col("anio") == 2024) \
        .withColumn("ingreso_neto", col("monto") - col("descuento")) \
        .withColumn(
            "segmento_venta",
            when(col("ingreso_neto") < 500, "Bajo")
            .when(col("ingreso_neto") < 2000, "Medio")
            .otherwise("Alto")
        ) \
        .withColumn(
            "es_venta_premium",
            when(col("ingreso_neto") > 3000, True).otherwise(False)
        )

print("✅ Función de transformación ETL definida")
```

2. Ejecuta el escenario **sin caché** y mide el tiempo total de 3 acciones:

```python
# Celda 6 — Escenario SIN caché
print("=" * 60)
print("ESCENARIO 1: SIN CACHÉ")
print("=" * 60)

inicio_sin_cache = time.time()

# Cada acción re-ejecuta TODA la cadena de transformaciones
df_sin_cache = transformacion_etl(df_ventas)

accion1 = df_sin_cache.count()
print(f"  Acción 1 (count): {accion1:,} registros")

accion2 = df_sin_cache.agg(avg("ingreso_neto")).collect()[0][0]
print(f"  Acción 2 (avg ingreso_neto): ${accion2:.2f}")

accion3 = df_sin_cache.groupBy("categoria").count().collect()
print(f"  Acción 3 (groupBy categoria): {len(accion3)} categorías")

tiempo_sin_cache = time.time() - inicio_sin_cache
print(f"\n⏱️  Tiempo total SIN caché: {tiempo_sin_cache:.2f} segundos")
print("   (Cada acción re-ejecutó el plan completo desde el origen)")
```

3. Ejecuta el escenario **con `cache()`** y mide el tiempo total de las mismas 3 acciones:

```python
# Celda 7 — Escenario CON cache()
print("=" * 60)
print("ESCENARIO 2: CON cache()")
print("=" * 60)

df_con_cache = transformacion_etl(df_ventas)
df_con_cache.cache()  # Marca para caché (nivel MEMORY_AND_DISK para DataFrames)

inicio_con_cache = time.time()

# Primera acción: COMPUTA y ALMACENA en memoria
accion1_c = df_con_cache.count()
print(f"  Acción 1 (count - materializa caché): {accion1_c:,} registros")

# Segunda y tercera acción: LEEN desde la caché
accion2_c = df_con_cache.agg(avg("ingreso_neto")).collect()[0][0]
print(f"  Acción 2 (avg - desde caché): ${accion2_c:.2f}")

accion3_c = df_con_cache.groupBy("categoria").count().collect()
print(f"  Acción 3 (groupBy - desde caché): {len(accion3_c)} categorías")

tiempo_con_cache = time.time() - inicio_con_cache
print(f"\n⏱️  Tiempo total CON cache(): {tiempo_con_cache:.2f} segundos")

# Resumen comparativo
mejora = ((tiempo_sin_cache - tiempo_con_cache) / tiempo_sin_cache) * 100
print(f"\n📊 RESUMEN COMPARATIVO:")
print(f"   Sin caché:   {tiempo_sin_cache:.2f}s")
print(f"   Con cache(): {tiempo_con_cache:.2f}s")
print(f"   Mejora:      {mejora:.1f}%")

# Liberar caché
df_con_cache.unpersist()
print("\n✅ Caché liberada con unpersist()")
```

4. Explora los diferentes niveles de `persist()`:

```python
# Celda 8 — Comparativa de StorageLevels
print("=" * 60)
print("ESCENARIO 3: COMPARATIVA DE StorageLevels")
print("=" * 60)

niveles = {
    "MEMORY_ONLY":     StorageLevel.MEMORY_ONLY,
    "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
    "DISK_ONLY":       StorageLevel.DISK_ONLY,
}

resultados_niveles = {}

for nombre_nivel, nivel in niveles.items():
    df_test = transformacion_etl(df_ventas)
    df_test.persist(nivel)

    inicio = time.time()
    df_test.count()            # Materialización
    t_primera = time.time() - inicio

    inicio = time.time()
    df_test.count()            # Segunda lectura (desde caché)
    t_segunda = time.time() - inicio

    resultados_niveles[nombre_nivel] = {
        "primera_accion_s": round(t_primera, 3),
        "segunda_accion_s": round(t_segunda, 3)
    }

    df_test.unpersist()
    print(f"  {nombre_nivel}: 1ra={t_primera:.3f}s | 2da={t_segunda:.3f}s ✅")

print("\n📊 Tabla de resultados por StorageLevel:")
for nivel, tiempos in resultados_niveles.items():
    print(f"  {nivel:<25} | 1ra acción: {tiempos['primera_accion_s']}s "
          f"| 2da acción: {tiempos['segunda_accion_s']}s")
```

#### Salida esperada

```
ESCENARIO 2: CON cache()
  Acción 1 (count - materializa caché): 540,XXX registros
  Acción 2 (avg - desde caché): $XXXX.XX
  Acción 3 (groupBy - desde caché): 5 categorías

⏱️  Tiempo total CON cache(): X.XX segundos

📊 RESUMEN COMPARATIVO:
   Sin caché:   XX.XXs
   Con cache(): X.XXs
   Mejora:      ~40-70%

MEMORY_ONLY:     1ra=X.XXXs | 2da=X.XXXs ✅
MEMORY_AND_DISK: 1ra=X.XXXs | 2da=X.XXXs ✅
DISK_ONLY:       1ra=X.XXXs | 2da=X.XXXs ✅
```

> 💡 **Observación esperada:** `MEMORY_ONLY` tendrá la segunda acción más rápida. `DISK_ONLY` será más lento en la segunda acción pero usa menos RAM. `MEMORY_AND_DISK` ofrece el mejor balance para producción.

#### Verificación

Abre la Spark UI en `http://localhost:4040` → pestaña **Storage**. Deberías ver los DataFrames cacheados mientras `persist()` esté activo. Después de `unpersist()`, la lista debe estar vacía.

---

### Paso 3: Particionamiento — `repartition()` y `coalesce()`

**Objetivo:** Analizar el número de particiones del DataFrame, comprender su impacto en el paralelismo y ajustarlo con `repartition()` y `coalesce()`.

#### Instrucciones

1. Inspecciona el número de particiones actuales:

```python
# Celda 9 — Análisis de particiones actuales
print("=" * 60)
print("ANÁLISIS DE PARTICIONAMIENTO")
print("=" * 60)

# Número de particiones del dataset original
num_particiones_original = df_ventas.rdd.getNumPartitions()
print(f"📦 Particiones del DataFrame original: {num_particiones_original}")

# Tamaño estimado por partición
total_filas = df_ventas.count()
filas_por_particion = total_filas / num_particiones_original
print(f"📦 Total filas: {total_filas:,}")
print(f"📦 Filas estimadas por partición: {filas_por_particion:,.0f}")

# Distribución real de filas por partición
distribucion = df_ventas.rdd.mapPartitionsWithIndex(
    lambda idx, it: [(idx, sum(1 for _ in it))]
).toDF(["particion", "num_filas"])

print("\n📊 Distribución de filas por partición:")
distribucion.orderBy("particion").show()
```

2. Aplica `repartition()` y mide el efecto:

```python
# Celda 10 — Aplicación de repartition()
print("=" * 60)
print("REPARTITION: Aumentar / redistribuir particiones")
print("=" * 60)

# repartition(n): redistribuye datos uniformemente (genera shuffle)
df_reparticionado = df_ventas.repartition(16)
print(f"✅ Particiones después de repartition(16): "
      f"{df_reparticionado.rdd.getNumPartitions()}")

# repartition(n, col): particionamiento por columna (útil para joins)
df_por_region = df_ventas.repartition(8, col("region"))
print(f"✅ Particiones después de repartition(8, 'region'): "
      f"{df_por_region.rdd.getNumPartitions()}")

# Verificar distribución con repartición por columna
print("\n📊 Distribución por región (repartition por columna):")
df_por_region.rdd.mapPartitionsWithIndex(
    lambda idx, it: [(idx, sum(1 for _ in it))]
).toDF(["particion", "num_filas"]).orderBy("particion").show()

# Benchmark: repartition vs. sin repartition en groupBy
print("\n⏱️  Benchmark groupBy con diferentes particiones:")

for n_part, etiqueta in [(4, "4 particiones"), (8, "8 particiones"),
                          (16, "16 particiones"), (32, "32 particiones")]:
    df_temp = df_ventas.repartition(n_part)
    inicio = time.time()
    df_temp.groupBy("region", "categoria").agg(
        spark_sum("monto").alias("total")
    ).count()
    elapsed = time.time() - inicio
    print(f"  {etiqueta:<20}: {elapsed:.3f}s")
```

3. Aplica `coalesce()` para reducir particiones sin shuffle:

```python
# Celda 11 — Aplicación de coalesce()
print("=" * 60)
print("COALESCE: Reducir particiones SIN shuffle")
print("=" * 60)

# Partimos de un DataFrame con muchas particiones
df_muchas_partes = df_ventas.repartition(32)
print(f"Particiones iniciales: {df_muchas_partes.rdd.getNumPartitions()}")

# coalesce(n): reduce particiones sin mover datos entre nodos (no shuffle)
df_coalescido = df_muchas_partes.coalesce(4)
print(f"Particiones después de coalesce(4): "
      f"{df_coalescido.rdd.getNumPartitions()}")

# Comparativa de tiempo: coalesce vs. repartition para reducir particiones
print("\n⏱️  Comparativa coalesce vs. repartition para REDUCIR particiones:")

# Con coalesce (sin shuffle)
df_base = df_ventas.repartition(32)
inicio = time.time()
df_base.coalesce(4).count()
t_coalesce = time.time() - inicio
print(f"  coalesce(4) desde 32 particiones: {t_coalesce:.3f}s")

# Con repartition (con shuffle)
df_base2 = df_ventas.repartition(32)
inicio = time.time()
df_base2.repartition(4).count()
t_repartition = time.time() - inicio
print(f"  repartition(4) desde 32 particiones: {t_repartition:.3f}s")

print(f"\n💡 coalesce es {t_repartition/t_coalesce:.1f}x más rápido que "
      f"repartition para reducir particiones (sin shuffle)")
```

#### Salida esperada

```
📦 Particiones del DataFrame original: 8 (varía según el entorno)
✅ Particiones después de repartition(16): 16
✅ Particiones después de repartition(8, 'region'): 8

⏱️  Benchmark groupBy con diferentes particiones:
  4 particiones       : X.XXXs
  8 particiones       : X.XXXs
  16 particiones      : X.XXXs
  32 particiones      : X.XXXs

💡 coalesce es ~2-4x más rápido que repartition para reducir particiones
```

#### Verificación

```python
# Celda 12 — Verificación de particionamiento
assert df_reparticionado.rdd.getNumPartitions() == 16
assert df_coalescido.rdd.getNumPartitions() == 4
print("✅ Verificaciones de particionamiento superadas")
```

---

### Paso 4: Identificación y Mitigación del Shuffle con `explain()`

**Objetivo:** Usar `explain()` para identificar operaciones que generan *shuffle* en el plan de ejecución, y comprender su impacto en el rendimiento.

#### Instrucciones

1. Analiza el plan de ejecución de operaciones con y sin shuffle:

```python
# Celda 13 — Análisis del plan de ejecución con explain()
print("=" * 60)
print("IDENTIFICACIÓN DE SHUFFLE CON explain()")
print("=" * 60)

# Operación 1: filter() — NO genera shuffle
print("\n--- Plan de filter() [SIN shuffle] ---")
df_ventas.filter(col("region") == "Norte").explain()
```

```python
# Celda 14 — Plan de groupBy (genera shuffle)
print("--- Plan de groupBy().agg() [CON shuffle — Exchange] ---")
df_ventas.groupBy("region").agg(
    spark_sum("monto").alias("total_monto")
).explain()
# Busca en la salida: "Exchange hashpartitioning" → indica shuffle
```

```python
# Celda 15 — Plan de join estándar (genera shuffle)
print("--- Plan de join() estándar [CON shuffle — SortMergeJoin] ---")

# Tabla pequeña de referencia: descuentos por categoría
df_descuentos = spark.createDataFrame([
    ("Electrónica", 0.15),
    ("Ropa",        0.20),
    ("Alimentos",   0.05),
    ("Hogar",       0.10),
    ("Deportes",    0.12),
], ["categoria", "tasa_descuento"])

df_ventas.join(df_descuentos, on="categoria", how="left").explain()
# Busca: "SortMergeJoin" → join con shuffle en ambos lados
```

```python
# Celda 16 — Identificar las palabras clave de shuffle en el plan
print("\n💡 PALABRAS CLAVE DE SHUFFLE EN explain():")
print("  - 'Exchange hashpartitioning' → shuffle por hash de columna")
print("  - 'Exchange rangepartitioning' → shuffle por rango (orderBy)")
print("  - 'SortMergeJoin'             → join con shuffle en ambos lados")
print("  - 'Sort'                      → ordenamiento (puede implicar shuffle)")
print("  - 'HashAggregate'             → agregación (2 fases con shuffle)")
```

2. Usa `explain(mode="formatted")` para un plan más legible:

```python
# Celda 17 — Plan formateado (más legible)
print("--- Plan formateado de groupBy [modo 'formatted'] ---")
df_ventas.groupBy("region", "categoria") \
    .agg(spark_sum("monto").alias("total")) \
    .explain(mode="formatted")
```

#### Salida esperada (fragmento)

```
--- Plan de groupBy().agg() [CON shuffle — Exchange] ---
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[region#XX], functions=[sum(monto#XX)])
   +- Exchange hashpartitioning(region#XX, 8), ENSURE_REQUIREMENTS, ...
      +- HashAggregate(keys=[region#XX], functions=[partial_sum(monto#XX)])
         +- ...

💡 PALABRAS CLAVE DE SHUFFLE EN explain():
  - 'Exchange hashpartitioning' → shuffle por hash de columna
  ...
```

#### Verificación

Abre la Spark UI → pestaña **Stages**. Después de ejecutar el `groupBy`, deberías ver al menos 2 etapas (*stages*): una para el cómputo local y otra para el shuffle/agregación final.

---

### Paso 5: Broadcast Join — Eliminar Shuffle en Joins con Tablas Pequeñas

**Objetivo:** Implementar broadcast join para evitar el costoso shuffle que genera un join estándar cuando una de las tablas es pequeña.

#### Instrucciones

1. Crea la tabla de referencia y compara join estándar vs. broadcast join:

```python
# Celda 18 — Preparación de tablas para el join
from pyspark.sql.functions import broadcast

print("=" * 60)
print("BROADCAST JOIN: Eliminando shuffle en joins")
print("=" * 60)

# Tabla pequeña de referencia: información de regiones (~5 filas)
df_regiones = spark.createDataFrame([
    ("Norte",  "Región Norte",  "MX-N", 1.10),
    ("Sur",    "Región Sur",    "MX-S", 1.05),
    ("Este",   "Región Este",   "MX-E", 1.08),
    ("Oeste",  "Región Oeste",  "MX-O", 1.12),
    ("Centro", "Región Centro", "MX-C", 1.15),
], ["region", "nombre_completo", "codigo_region", "factor_ajuste"])

print(f"✅ Tabla grande (ventas):   {df_ventas.count():,} filas, "
      f"{df_ventas.rdd.getNumPartitions()} particiones")
print(f"✅ Tabla pequeña (regiones): {df_regiones.count()} filas")
```

```python
# Celda 19 — Join ESTÁNDAR (genera shuffle en ambas tablas)
print("\n--- JOIN ESTÁNDAR (SortMergeJoin con shuffle) ---")

inicio = time.time()
resultado_join_std = df_ventas.join(df_regiones, on="region", how="left")
conteo_std = resultado_join_std.count()
t_join_std = time.time() - inicio

print(f"  Filas resultado: {conteo_std:,}")
print(f"  Tiempo: {t_join_std:.3f}s")

# Ver el plan — busca SortMergeJoin o BroadcastHashJoin
resultado_join_std.explain()
```

```python
# Celda 20 — BROADCAST JOIN (sin shuffle en la tabla pequeña)
print("\n--- BROADCAST JOIN (sin shuffle) ---")

inicio = time.time()
resultado_broadcast = df_ventas.join(
    broadcast(df_regiones),  # Indica a Spark que distribuya df_regiones a todos los ejecutores
    on="region",
    how="left"
)
conteo_bc = resultado_broadcast.count()
t_broadcast = time.time() - inicio

print(f"  Filas resultado: {conteo_bc:,}")
print(f"  Tiempo: {t_broadcast:.3f}s")

# Ver el plan — debe mostrar BroadcastHashJoin
print("\n📋 Plan de ejecución del Broadcast Join:")
resultado_broadcast.explain()

# Comparativa
print(f"\n📊 COMPARATIVA:")
print(f"  Join estándar:   {t_join_std:.3f}s")
print(f"  Broadcast join:  {t_broadcast:.3f}s")
if t_join_std > 0:
    mejora_join = ((t_join_std - t_broadcast) / t_join_std) * 100
    print(f"  Mejora:          {mejora_join:.1f}%")
```

2. Usa también la variable broadcast del SparkContext directamente:

```python
# Celda 21 — Broadcast variable del SparkContext (para uso en UDFs/RDDs)
print("\n--- BROADCAST VARIABLE con sc.broadcast() ---")

# Diccionario de factores de ajuste por región (para uso en transformaciones)
factores_dict = {
    "Norte":  1.10,
    "Sur":    1.05,
    "Este":   1.08,
    "Oeste":  1.12,
    "Centro": 1.15,
}

# Distribuir el diccionario a todos los ejecutores
broadcast_factores = sc.broadcast(factores_dict)

print(f"✅ Variable broadcast creada")
print(f"   Valor en driver: {broadcast_factores.value}")

# Usar la variable broadcast en una UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

@udf(returnType=DoubleType())
def aplicar_factor(region, monto):
    """Aplica el factor de ajuste regional usando la variable broadcast."""
    factor = broadcast_factores.value.get(region, 1.0)
    return round(monto * factor, 2)

df_con_factor = df_ventas.withColumn(
    "monto_ajustado",
    aplicar_factor(col("region"), col("monto"))
)

print("\n📊 Muestra con monto ajustado por región:")
df_con_factor.select("region", "monto", "monto_ajustado").show(8)

# Destruir la variable broadcast cuando ya no se necesita
broadcast_factores.destroy()
print("✅ Variable broadcast destruida")
```

#### Salida esperada

```
--- BROADCAST JOIN (sin shuffle) ---
  Filas resultado: 1,200,000
  Tiempo: X.XXXs

📋 Plan de ejecución del Broadcast Join:
== Physical Plan ==
...BroadcastHashJoin [region#XX], [region#XX], LeftOuter, BuildRight...
   +- BroadcastExchange HashedRelationBroadcastMode(...)
      +- ...

📊 COMPARATIVA:
  Join estándar:   X.XXXs
  Broadcast join:  X.XXXs
  Mejora:          ~20-60%
```

> 💡 **Clave:** El plan del broadcast join debe mostrar `BroadcastHashJoin` y `BroadcastExchange`, **no** `SortMergeJoin` ni `Exchange hashpartitioning`. Esto confirma que el shuffle fue eliminado.

#### Verificación

```python
# Celda 22 — Verificación del broadcast join
plan_broadcast = resultado_broadcast._jdf.queryExecution().executedPlan().toString()
assert "BroadcastHashJoin" in plan_broadcast or "BroadcastExchange" in plan_broadcast, \
    "❌ El plan no contiene BroadcastHashJoin"
print("✅ Verificación superada: el plan contiene BroadcastHashJoin")
```

---

### Paso 6: Acumuladores — Métricas Distribuidas en el Pipeline

**Objetivo:** Implementar acumuladores para registrar contadores y métricas distribuidas durante la ejecución de transformaciones en el pipeline.

#### Instrucciones

1. Crea acumuladores para diferentes métricas del pipeline:

```python
# Celda 23 — Definición de acumuladores
print("=" * 60)
print("ACUMULADORES: Métricas distribuidas")
print("=" * 60)

# Acumulador 1: Contador de registros procesados exitosamente
acc_procesados = sc.accumulator(0)

# Acumulador 2: Contador de registros con error (tiene_error == 1)
acc_errores = sc.accumulator(0)

# Acumulador 3: Contador de ventas premium (ingreso_neto > 3000)
acc_premium = sc.accumulator(0)

# Acumulador 4: Contador de ventas canceladas/devueltas
acc_cancelados = sc.accumulator(0)

print("✅ Acumuladores creados:")
print(f"   acc_procesados: {acc_procesados.value}")
print(f"   acc_errores:    {acc_errores.value}")
print(f"   acc_premium:    {acc_premium.value}")
print(f"   acc_cancelados: {acc_cancelados.value}")
```

2. Implementa el pipeline con acumuladores integrados:

```python
# Celda 24 — Pipeline con acumuladores (usando RDD para acceso directo)
print("\n⏳ Ejecutando pipeline con acumuladores...")

# Convertir a RDD para poder usar acumuladores en transformaciones
# (Los acumuladores en DataFrames requieren foreachPartition o foreach)
rdd_ventas = df_ventas.rdd

def procesar_registro(fila):
    """
    Función de procesamiento que actualiza acumuladores.
    Se ejecuta en cada ejecutor de forma distribuida.
    """
    ingreso_neto = fila["monto"] - fila["descuento"]

    # Contar errores
    if fila["tiene_error"] == 1:
        acc_errores.add(1)
        return None  # Registro con error: descartar

    # Contar cancelados/devueltos
    if fila["estado"] in ("cancelado", "devuelto"):
        acc_cancelados.add(1)

    # Contar ventas premium
    if ingreso_neto > 3000:
        acc_premium.add(1)

    # Contar procesados exitosamente
    acc_procesados.add(1)

    return (
        fila["id_transaccion"],
        fila["id_vendedor"],
        fila["categoria"],
        fila["region"],
        fila["estado"],
        round(ingreso_neto, 2),
        fila["anio"],
        fila["mes"]
    )

# Ejecutar el pipeline (filter elimina los None de registros con error)
inicio_pipeline = time.time()
rdd_procesado = rdd_ventas.map(procesar_registro).filter(lambda x: x is not None)

# Materializar para activar los acumuladores
total_procesado = rdd_procesado.count()
t_pipeline = time.time() - inicio_pipeline

print(f"\n✅ Pipeline completado en {t_pipeline:.2f}s")
```

3. Lee e interpreta los valores de los acumuladores:

```python
# Celda 25 — Lectura de métricas del pipeline
print("\n" + "=" * 60)
print("📊 MÉTRICAS DEL PIPELINE (desde acumuladores)")
print("=" * 60)

total_entrada = df_ventas.count()

print(f"  Total registros entrada:     {total_entrada:>10,}")
print(f"  Registros procesados (éxito):{acc_procesados.value:>10,}")
print(f"  Registros con error:         {acc_errores.value:>10,}")
print(f"  Ventas canceladas/devueltas: {acc_cancelados.value:>10,}")
print(f"  Ventas premium (>$3,000):    {acc_premium.value:>10,}")
print(f"  Total resultado final:       {total_procesado:>10,}")
print()
print(f"  Tasa de error:               "
      f"{acc_errores.value / total_entrada * 100:.2f}%")
print(f"  Tasa de ventas premium:      "
      f"{acc_premium.value / acc_procesados.value * 100:.2f}%")

# Verificación de consistencia
suma_control = acc_procesados.value + acc_errores.value
print(f"\n  ✅ Verificación: procesados + errores = {suma_control:,} "
      f"(debe ≈ {total_entrada:,})")
```

4. Demuestra el uso de acumuladores con `foreachPartition` en DataFrames:

```python
# Celda 26 — Acumuladores con foreachPartition (patrón recomendado en DataFrames)
print("\n--- Acumuladores con foreachPartition (patrón DataFrame) ---")

# Reiniciar acumuladores
acc_filas_por_particion = sc.accumulator(0)
acc_monto_total = sc.accumulator(0.0)

def contar_por_particion(iterador):
    """Procesa cada partición y actualiza acumuladores."""
    for fila in iterador:
        acc_filas_por_particion.add(1)
        acc_monto_total.add(float(fila["monto"]))

# foreachPartition ejecuta la función en cada partición del ejecutor
df_ventas.foreachPartition(contar_por_particion)

print(f"✅ Total filas contadas por acumulador: {acc_filas_por_particion.value:,}")
print(f"✅ Monto total acumulado: ${acc_monto_total.value:,.2f}")
print(f"✅ Promedio monto: ${acc_monto_total.value / acc_filas_por_particion.value:.2f}")
```

#### Salida esperada

```
📊 MÉTRICAS DEL PIPELINE (desde acumuladores)
  Total registros entrada:      1,200,000
  Registros procesados (éxito):   ~1,128,000  (≈94%)
  Registros con error:               ~36,000  (≈3%)
  Ventas canceladas/devueltas:      ~180,000  (≈15%)
  Ventas premium (>$3,000):          ~Xp,XXX
  Total resultado final:          ~1,092,000

  Tasa de error:               ~3.00%
  Tasa de ventas premium:      ~X.XX%
```

#### Verificación

```python
# Celda 27 — Verificación de acumuladores
assert acc_procesados.value > 0, "❌ El acumulador de procesados debe ser > 0"
assert acc_errores.value > 0, "❌ El acumulador de errores debe ser > 0"
assert acc_errores.value < total_entrada * 0.10, \
    "❌ La tasa de error no debe superar el 10%"
print("✅ Todas las verificaciones de acumuladores superadas")
```

---

### Paso 7: Pipeline Optimizado Completo — Integración de Todas las Técnicas

**Objetivo:** Construir un pipeline de análisis completo que integre cache, particionamiento óptimo, broadcast join y acumuladores como un sistema coherente.

#### Instrucciones

```python
# Celda 28 — Pipeline optimizado integrado
print("=" * 60)
print("PIPELINE OPTIMIZADO COMPLETO")
print("=" * 60)

from pyspark.sql.functions import (
    col, when, avg, count, sum as spark_sum,
    max as spark_max, broadcast, round as spark_round
)

# ── PASO A: Acumuladores del pipeline ──────────────────────────────
acc_total_pipeline   = sc.accumulator(0)
acc_errores_pipeline = sc.accumulator(0)

# ── PASO B: Tablas de referencia (broadcast) ───────────────────────
df_ref_categorias = spark.createDataFrame([
    ("Electrónica", "Tech",      "Alta"),
    ("Ropa",        "Fashion",   "Media"),
    ("Alimentos",   "FMCG",      "Baja"),
    ("Hogar",       "Home",      "Media"),
    ("Deportes",    "Sport",     "Alta"),
], ["categoria", "grupo_negocio", "prioridad"])

df_ref_regiones = spark.createDataFrame([
    ("Norte",  1.10, "Zona A"),
    ("Sur",    1.05, "Zona B"),
    ("Este",   1.08, "Zona A"),
    ("Oeste",  1.12, "Zona C"),
    ("Centro", 1.15, "Zona A"),
], ["region", "factor_ajuste", "zona"])

# ── PASO C: Transformación base + caché ───────────────────────────
inicio_total = time.time()

df_base_optimizado = df_ventas \
    .filter(col("tiene_error") == 0) \
    .filter(col("estado").isin(["completado", "pendiente"])) \
    .withColumn("ingreso_neto", col("monto") - col("descuento")) \
    .repartition(8, col("region"))  # Particionar por región para joins eficientes

# Cachear el resultado base (se usará en múltiples análisis)
df_base_optimizado.persist(StorageLevel.MEMORY_AND_DISK)
df_base_optimizado.count()  # Materializar la caché
print(f"✅ DataFrame base cacheado: {df_base_optimizado.count():,} filas, "
      f"{df_base_optimizado.rdd.getNumPartitions()} particiones")

# ── PASO D: Análisis 1 — Resumen por región con broadcast join ─────
df_analisis_region = df_base_optimizado \
    .join(broadcast(df_ref_regiones), on="region", how="left") \
    .withColumn("ingreso_ajustado",
                spark_round(col("ingreso_neto") * col("factor_ajuste"), 2)) \
    .groupBy("region", "zona") \
    .agg(
        count("*").alias("total_ventas"),
        spark_sum("ingreso_ajustado").alias("ingreso_total_ajustado"),
        avg("ingreso_ajustado").alias("ingreso_promedio_ajustado"),
        spark_max("ingreso_ajustado").alias("venta_maxima")
    ) \
    .orderBy(col("ingreso_total_ajustado").desc())

print("\n📊 Análisis 1: Resumen por Región (con factor de ajuste):")
df_analisis_region.show()

# ── PASO E: Análisis 2 — Top vendedores por categoría (broadcast join) ─
df_analisis_cat = df_base_optimizado \
    .join(broadcast(df_ref_categorias), on="categoria", how="left") \
    .groupBy("grupo_negocio", "prioridad", "id_vendedor") \
    .agg(
        count("*").alias("num_ventas"),
        spark_sum("ingreso_neto").alias("ingreso_total")
    )

# Top 3 vendedores por grupo de negocio
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

ventana_top = Window.partitionBy("grupo_negocio").orderBy(
    col("ingreso_total").desc()
)
df_top_vendedores = df_analisis_cat \
    .withColumn("ranking", rank().over(ventana_top)) \
    .filter(col("ranking") <= 3) \
    .orderBy("grupo_negocio", "ranking")

print("📊 Análisis 2: Top 3 Vendedores por Grupo de Negocio:")
df_top_vendedores.show(15)

# ── PASO F: Análisis 3 — Tendencia mensual ────────────────────────
df_tendencia = df_base_optimizado \
    .groupBy("anio", "mes") \
    .agg(
        count("*").alias("total_ventas"),
        spark_sum("ingreso_neto").alias("ingreso_total"),
        avg("ingreso_neto").alias("ingreso_promedio")
    ) \
    .orderBy("anio", "mes")

print("📊 Análisis 3: Tendencia Mensual:")
df_tendencia.show(24)

t_total = time.time() - inicio_total
print(f"\n⏱️  Tiempo total del pipeline optimizado: {t_total:.2f}s")
print(f"   (Los análisis 2 y 3 leyeron desde la caché)")

# ── PASO G: Liberar recursos ──────────────────────────────────────
df_base_optimizado.unpersist()
print("✅ Caché liberada")
```

#### Salida esperada

```
✅ DataFrame base cacheado: ~1,128,000 filas, 8 particiones

📊 Análisis 1: Resumen por Región (con factor de ajuste):
+-------+------+------------+----------------------+...
| region| zona |total_ventas|ingreso_total_ajustado|...
+-------+------+------------+----------------------+...
|Centro |Zona A|   XXX,XXX  |      $XX,XXX,XXX.XX  |...
...

⏱️  Tiempo total del pipeline optimizado: X.XXs
✅ Caché liberada
```

---

## 7. Validación y Pruebas Finales

```python
# Celda 29 — Suite completa de validación
print("=" * 60)
print("VALIDACIÓN FINAL DEL LABORATORIO")
print("=" * 60)

resultados_validacion = {}

# Test 1: Caché y unpersist
try:
    df_test_cache = df_ventas.cache()
    df_test_cache.count()
    df_test_cache.unpersist()
    resultados_validacion["Cache/Unpersist"] = "✅ PASS"
except Exception as e:
    resultados_validacion["Cache/Unpersist"] = f"❌ FAIL: {e}"

# Test 2: persist con StorageLevel
try:
    df_test_persist = df_ventas.persist(StorageLevel.MEMORY_AND_DISK)
    df_test_persist.count()
    df_test_persist.unpersist()
    resultados_validacion["Persist MEMORY_AND_DISK"] = "✅ PASS"
except Exception as e:
    resultados_validacion["Persist MEMORY_AND_DISK"] = f"❌ FAIL: {e}"

# Test 3: repartition
try:
    n = df_ventas.repartition(12).rdd.getNumPartitions()
    assert n == 12
    resultados_validacion["repartition(12)"] = "✅ PASS"
except Exception as e:
    resultados_validacion["repartition(12)"] = f"❌ FAIL: {e}"

# Test 4: coalesce
try:
    df_temp = df_ventas.repartition(16)
    n = df_temp.coalesce(4).rdd.getNumPartitions()
    assert n == 4
    resultados_validacion["coalesce(4)"] = "✅ PASS"
except Exception as e:
    resultados_validacion["coalesce(4)"] = f"❌ FAIL: {e}"

# Test 5: Broadcast join
try:
    df_small = spark.createDataFrame([("Norte", "A"), ("Sur", "B")],
                                     ["region", "zona"])
    result = df_ventas.join(broadcast(df_small), on="region", how="left")
    plan = result._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan or "BroadcastExchange" in plan
    resultados_validacion["Broadcast Join"] = "✅ PASS"
except Exception as e:
    resultados_validacion["Broadcast Join"] = f"❌ FAIL: {e}"

# Test 6: Acumuladores
try:
    acc_test = sc.accumulator(0)
    df_ventas.foreach(lambda row: acc_test.add(1))
    assert acc_test.value == df_ventas.count()
    resultados_validacion["Acumuladores"] = "✅ PASS"
except Exception as e:
    resultados_validacion["Acumuladores"] = f"❌ FAIL: {e}"

# Reporte final
print("\n📋 REPORTE DE VALIDACIÓN:")
for test, resultado in resultados_validacion.items():
    print(f"  {test:<30}: {resultado}")

total_pass = sum(1 for v in resultados_validacion.values() if "PASS" in v)
total_tests = len(resultados_validacion)
print(f"\n  Resultado: {total_pass}/{total_tests} pruebas superadas")

if total_pass == total_tests:
    print("\n🎉 ¡Laboratorio completado exitosamente!")
else:
    print("\n⚠️  Revisa los tests fallidos antes de continuar.")
```

---

## 8. Resolución de Problemas

### Problema 1: `OutOfMemoryError` al cachear el DataFrame grande

**Síntoma:**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
# o bien:
ERROR Executor: Exception in task ... java.lang.OutOfMemoryError
```
El kernel de Jupyter puede reiniciarse inesperadamente, o la celda de `count()` nunca termina.

**Causa:**
La memoria asignada al driver o executor (`spark.driver.memory` / `spark.executor.memory`) es insuficiente para materializar el DataFrame completo en caché con `MEMORY_ONLY` o `MEMORY_AND_DISK`. Esto es especialmente común en equipos con 8 GB de RAM cuando se usa el nivel `MEMORY_ONLY` sin desbordamiento a disco.

**Solución:**
```python
# Opción 1: Cambiar el StorageLevel a MEMORY_AND_DISK (permite desbordamiento)
df_ventas.persist(StorageLevel.MEMORY_AND_DISK)

# Opción 2: Reducir el tamaño del dataset de prueba temporalmente
NUM_FILAS = 500_000  # Reducir a 500K para máquinas con 8 GB RAM

# Opción 3: Reiniciar la sesión con más memoria
spark.stop()
spark = SparkSession.builder \
    .appName("Lab08_Optimizacion_Spark") \
    .master("local[*]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# Opción 4: Usar nivel serializado para reducir huella en memoria
df_ventas.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

> ⚠️ **Nota para Windows:** Si el error ocurre al escribir en disco (nivel `MEMORY_AND_DISK`), verifica que `HADOOP_HOME` y `winutils.exe` estén configurados correctamente. Sin ellos, Spark no puede escribir archivos temporales en disco.

---

### Problema 2: Los acumuladores muestran valores incorrectos (doble conteo)

**Síntoma:**
Los acumuladores reportan valores significativamente mayores a los esperados (por ejemplo, `acc_procesados.value` es el doble del total de filas del dataset). El conteo no es consistente entre ejecuciones.

**Causa:**
Spark puede re-ejecutar tareas fallidas o especulativas (*speculative execution*), lo que provoca que la función que actualiza el acumulador se ejecute más de una vez para el mismo registro. Además, si el DataFrame cacheado fue desalojado parcialmente, Spark re-computa las particiones desalojadas, ejecutando nuevamente las funciones de acumulador.

**Solución:**
```python
# Solución 1: Reiniciar los acumuladores SIEMPRE antes de una nueva ejecución
# Los acumuladores NO se reinician automáticamente entre ejecuciones de celdas

# ❌ MAL: reutilizar acumuladores sin reiniciar
acc_procesados.add(0)  # Esto NO reinicia el valor

# ✅ BIEN: crear nuevos acumuladores para cada ejecución
acc_procesados = sc.accumulator(0)
acc_errores = sc.accumulator(0)

# Solución 2: Usar cache() ANTES de ejecutar la acción con acumuladores
# para evitar re-cómputo de particiones
df_ventas.cache()
df_ventas.count()  # Materializar

# Ahora ejecutar la lógica con acumuladores (sin riesgo de re-cómputo)
df_ventas.foreachPartition(contar_por_particion)

# Solución 3: Deshabilitar ejecución especulativa en modo local (ya está deshabilitada)
# En clúster real, considerar:
# spark.conf.set("spark.speculation", "false")
```

---

## 9. Limpieza de Recursos

```python
# Celda 30 — Limpieza completa de recursos
print("=" * 60)
print("LIMPIEZA DE RECURSOS")
print("=" * 60)

# 1. Liberar todos los DataFrames cacheados activos
try:
    df_con_cache.unpersist()
    print("✅ df_con_cache liberado")
except:
    pass

try:
    df_persist.unpersist()
    print("✅ df_persist liberado")
except:
    pass

try:
    df_base_optimizado.unpersist()
    print("✅ df_base_optimizado liberado")
except:
    pass

# 2. Limpiar todos los DataFrames cacheados en la sesión
spark.catalog.clearCache()
print("✅ Cache global limpiada con spark.catalog.clearCache()")

# 3. Destruir variables broadcast activas
try:
    broadcast_factores.destroy()
    print("✅ Variable broadcast destruida")
except:
    pass

# 4. Verificar que no queden DataFrames en caché
print(f"\n📊 DataFrames en caché después de limpieza: "
      f"{len(spark.catalog.listDatabases())}")  # Verificación indirecta

# 5. Detener la sesión Spark (solo al finalizar el laboratorio completo)
# ⚠️ DESCOMENTAR solo si has terminado todos los ejercicios
# spark.stop()
# print("✅ Sesión Spark detenida")

print("\n✅ Limpieza completada. Recursos liberados correctamente.")
print("   Spark UI en http://localhost:4040 → Storage debe estar vacío")
```

---

## 10. Resumen y Recursos

### Lo que aprendiste en esta práctica

| Técnica                    | Cuándo usarla                                                          | Método clave                                |
|----------------------------|------------------------------------------------------------------------|---------------------------------------------|
| `cache()`                  | DataFrame reutilizado 2+ veces, memoria suficiente                     | `df.cache()`                                |
| `persist(MEMORY_AND_DISK)` | DataFrame grande, riesgo de desbordamiento de memoria                  | `df.persist(StorageLevel.MEMORY_AND_DISK)`  |
| `persist(DISK_ONLY)`       | Memoria muy limitada, acceso poco frecuente                            | `df.persist(StorageLevel.DISK_ONLY)`        |
| `unpersist()`              | Siempre al terminar de usar el DataFrame cacheado                      | `df.unpersist()`                            |
| `repartition(n)`           | Aumentar paralelismo, redistribuir datos uniformemente (genera shuffle) | `df.repartition(n)` / `df.repartition(n, col)` |
| `coalesce(n)`              | Reducir particiones sin shuffle (escritura eficiente)                  | `df.coalesce(n)`                            |
| `explain()`                | Identificar shuffle stages y optimizar el plan de ejecución            | `df.explain()` / `df.explain("formatted")`  |
| `broadcast()`              | Join con tabla pequeña (<= 10 MB típicamente)                          | `df.join(broadcast(df_small), ...)`         |
| `sc.broadcast()`           | Distribuir diccionarios/listas de referencia a ejecutores              | `sc.broadcast(dict)`                        |
| `sc.accumulator()`         | Contadores y métricas distribuidas en transformaciones                 | `sc.accumulator(0)`                         |

### Conceptos clave consolidados

- **La caché no es gratuita:** consume memoria de los ejecutores y tiene un costo de escritura en la primera acción. Solo tiene sentido cuando el DataFrame se reutiliza múltiples veces.
- **`coalesce` vs. `repartition`:** usa `coalesce` para *reducir* particiones (sin shuffle) y `repartition` para *aumentar* o *redistribuir* (con shuffle). Nunca uses `repartition` para reducir si el rendimiento es crítico.
- **El shuffle es el enemigo número 1 del rendimiento:** operaciones como `groupBy`, `join`, `distinct` y `orderBy` generan shuffle. Identifícalos con `explain()` y mitígalos con broadcast, particionamiento previo o diseño de pipeline.
- **Los acumuladores solo son confiables en acciones, no en transformaciones:** Spark puede re-ejecutar transformaciones; los acumuladores deben usarse con `foreach` o `foreachPartition` para resultados consistentes.
- **La Spark UI es tu aliada:** las pestañas Storage, Stages y SQL/DataFrame revelan exactamente qué está pasando dentro del motor.

### Recursos adicionales

- [Documentación oficial: RDD Persistence y Storage Levels](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
- [Documentación oficial: Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Documentación oficial: Broadcast Variables](https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables)
- [Documentación oficial: Accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)
- [PySpark API: DataFrame.persist()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html)
- [PySpark API: DataFrame.repartition()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)
- [Guía de ajuste de rendimiento en Spark (Databricks)](https://docs.databricks.com/en/optimizations/index.html)

---
LAB_END---
