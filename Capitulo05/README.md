# Uso de SQL con DataFrames, Cálculos y Operaciones con Columnas y Transformaciones

## 1. Metadatos

| Campo            | Detalle                                      |
|------------------|----------------------------------------------|
| **Duración**     | 90 minutos                                   |
| **Complejidad**  | Alta                                         |
| **Nivel Bloom**  | Aplicar                                      |
| **Práctica Nº**  | 4 de 9                                       |
| **Módulo**       | 5 — Spark SQL y DataFrame API Avanzada       |

---

## 2. Descripción General

En esta práctica trabajarás con un dataset de e-commerce intencionalmente sucio para aplicar Spark SQL y la DataFrame API de forma integrada. Registrarás DataFrames como vistas temporales y ejecutarás consultas SQL con JOINs, subconsultas y cláusulas GROUP BY/HAVING. Luego aplicarás operaciones avanzadas sobre columnas —expresiones condicionales, casting, limpieza con expresiones regulares— y transformaciones estructurales como `pivot`, `explode` y manejo de nulos. Al finalizar, compararás la expresividad y el rendimiento de RDD, DataFrame y Dataset con ejemplos equivalentes, y analizarás los planes de ejecución generados por el Catalyst Optimizer.

---

## 3. Objetivos de Aprendizaje

Al completar esta práctica serás capaz de:

- [ ] Registrar DataFrames como vistas temporales con `createOrReplaceTempView()` y ejecutar consultas SQL completas (JOINs, subconsultas, GROUP BY, HAVING) usando `spark.sql()`.
- [ ] Aplicar operaciones avanzadas sobre columnas: `when`/`otherwise`, `cast`, `isin`, `regexp_replace` y `withColumnRenamed`.
- [ ] Detectar, tratar y manejar datos faltantes, nulos y duplicados usando `isNull`, `isNotNull`, `fillna`, `dropna` y `replace`.
- [ ] Implementar transformaciones estructurales: `pivot`, `explode`, `posexplode` y manejo de arrays anidados.
- [ ] Contrastar el uso de RDD, DataFrame y Dataset, e interpretar planes de ejecución con `explain()`.

---

## 4. Prerequisitos

### Conocimientos Previos

- Práctica 3 completada: creación y operaciones básicas con DataFrames en PySpark.
- SQL estándar: `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `JOIN`.
- Comprensión de tipos de datos primitivos y conversiones de tipos.
- Familiaridad con datos sucios y conceptos básicos de limpieza de datos.

### Acceso Requerido

- Entorno Jupyter Notebook / JupyterLab funcional con PySpark configurado.
- Apache Spark 3.5.x instalado y variable `SPARK_HOME` configurada.
- Puerto 4040 disponible para acceder a la Spark UI.
- Dataset de práctica descargado (instrucciones en la Sección 5).

---

## 5. Entorno de Laboratorio

### Hardware Mínimo Recomendado

| Componente       | Mínimo              | Recomendado          |
|------------------|---------------------|----------------------|
| RAM              | 8 GB                | 16 GB                |
| CPU              | 4 núcleos, 64 bits  | 6+ núcleos           |
| Almacenamiento   | 5 GB libres         | 10 GB libres         |

### Software Requerido

| Componente          | Versión         |
|---------------------|-----------------|
| Java (JDK)          | 11 o 17 LTS     |
| Apache Spark        | 3.5.x           |
| Python              | 3.10 o 3.11     |
| PySpark             | 3.5.x           |
| JupyterLab          | 7.x             |
| pandas              | 2.0+            |
| findspark           | 2.0.1+          |

### Configuración Inicial del Entorno

Ejecuta los siguientes comandos en una terminal **antes** de abrir Jupyter para preparar el dataset de práctica:

```bash
# 1. Crear el directorio de trabajo para la práctica
mkdir -p ~/spark-labs/lab04/data
cd ~/spark-labs/lab04

# 2. Descargar el script generador de datos (incluido en el repositorio del curso)
# Si tienes acceso al repositorio Git del curso:
# git clone <url-repositorio> && cp <repositorio>/datasets/lab04/* ~/spark-labs/lab04/data/

# 3. Verificar que Java 11 o 17 esté activo
java -version

# 4. Verificar que PySpark esté instalado
python -c "import pyspark; print(pyspark.__version__)"

# 5. Crear el notebook para la práctica
jupyter lab --notebook-dir=~/spark-labs/lab04
```

> **Nota para Windows:** Asegúrate de que `HADOOP_HOME` apunte a la carpeta donde está `winutils.exe` y que `%HADOOP_HOME%\bin` esté en el `PATH`. Sin esto, las operaciones de lectura/escritura de archivos fallarán con errores de permisos.

> **Nota de memoria:** Si tu equipo tiene 8 GB de RAM, configura `spark.driver.memory=2g` y `spark.executor.memory=2g` en la SparkSession (incluido en el Paso 1).

---

## 6. Desarrollo Paso a Paso

---

### Paso 1: Inicialización de la SparkSession y Generación del Dataset

**Objetivo:** Crear una SparkSession correctamente configurada y generar el dataset de e-commerce con datos sucios que se usará durante toda la práctica.

#### Instrucciones

1. Crea un nuevo notebook en JupyterLab llamado `Lab04_SparkSQL_Transformaciones.ipynb`.

2. En la primera celda, inicializa la SparkSession:

```python
# Celda 1: Importaciones y configuración de SparkSession
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType, DateType, TimestampType
)
import pandas as pd
import numpy as np

# Crear SparkSession con configuración adecuada para modo local
spark = SparkSession.builder \
    .appName("Lab04-SparkSQL-Transformaciones") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Configurar nivel de logging para reducir ruido
spark.sparkContext.setLogLevel("WARN")

print(f"✓ SparkSession iniciada correctamente")
print(f"  Versión de Spark : {spark.version}")
print(f"  Master           : {spark.sparkContext.master}")
print(f"  App Name         : {spark.sparkContext.appName}")
print(f"  Spark UI         : http://localhost:4040")
```

3. En la siguiente celda, genera el dataset de e-commerce con datos intencionalmente sucios:

```python
# Celda 2: Generación del dataset de e-commerce (≈ 150,000 filas)
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# --- Parámetros del dataset ---
N_ORDERS = 150_000
N_CUSTOMERS = 5_000
N_PRODUCTS = 200

# --- Datos de referencia ---
categorias = ["Electronics", "Clothing", "Books", "Home & Garden",
              "Sports", "Toys", "Beauty", "Automotive"]
regiones   = ["Norte", "Sur", "Centro", "Este", "Oeste"]
metodos_pago = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash"]
estados_orden = ["completed", "pending", "cancelled", "refunded", "shipped"]

# --- Generación de datos de órdenes ---
def gen_date(start="2022-01-01", end="2024-12-31"):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return (s + timedelta(days=random.randint(0, (e - s).days))).strftime("%Y-%m-%d")

orders_data = []
for i in range(N_ORDERS):
    order_id    = f"ORD-{100000 + i}"
    customer_id = f"CUST-{random.randint(1, N_CUSTOMERS):05d}"
    product_id  = f"PROD-{random.randint(1, N_PRODUCTS):04d}"
    categoria   = random.choice(categorias)
    region      = random.choice(regiones)
    cantidad    = random.randint(1, 10)
    precio_unit = round(random.uniform(5.0, 500.0), 2)
    descuento   = round(random.uniform(0.0, 0.35), 2)
    metodo_pago = random.choice(metodos_pago)
    estado      = random.choice(estados_orden)
    fecha       = gen_date()

    # Introducir datos sucios intencionalmente (~8% de filas)
    if random.random() < 0.04:
        precio_unit = None          # precios nulos
    if random.random() < 0.03:
        cantidad = None             # cantidades nulas
    if random.random() < 0.02:
        region = "  Norte  "        # espacios extra en strings
    if random.random() < 0.015:
        precio_unit = -abs(precio_unit or 10)  # precios negativos (inválidos)
    if random.random() < 0.01:
        estado = "COMPLETED"        # inconsistencia de mayúsculas
    if random.random() < 0.005:
        fecha = "invalid-date"      # fechas inválidas

    # Introducir duplicados (~2%)
    if random.random() < 0.02 and i > 0:
        order_id = f"ORD-{100000 + random.randint(0, i - 1)}"

    orders_data.append((
        order_id, customer_id, product_id, categoria, region,
        cantidad, precio_unit, descuento, metodo_pago, estado, fecha
    ))

# Esquema explícito
schema_orders = StructType([
    StructField("order_id",      StringType(),  True),
    StructField("customer_id",   StringType(),  True),
    StructField("product_id",    StringType(),  True),
    StructField("categoria",     StringType(),  True),
    StructField("region",        StringType(),  True),
    StructField("cantidad",      IntegerType(), True),
    StructField("precio_unit",   DoubleType(),  True),
    StructField("descuento",     DoubleType(),  True),
    StructField("metodo_pago",   StringType(),  True),
    StructField("estado",        StringType(),  True),
    StructField("fecha",         StringType(),  True),   # string para demostrar cast
])

df_orders_raw = spark.createDataFrame(orders_data, schema=schema_orders)

# --- Dataset de clientes ---
customers_data = [
    (f"CUST-{i:05d}",
     f"Cliente_{i}",
     random.choice(["Gold", "Silver", "Bronze", None]),
     random.randint(18, 75),
     random.choice(regiones))
    for i in range(1, N_CUSTOMERS + 1)
]
schema_customers = StructType([
    StructField("customer_id",  StringType(),  True),
    StructField("nombre",       StringType(),  True),
    StructField("segmento",     StringType(),  True),
    StructField("edad",         IntegerType(), True),
    StructField("region",       StringType(),  True),
])
df_customers = spark.createDataFrame(customers_data, schema=schema_customers)

# --- Dataset de productos con arrays de tags ---
products_data = [
    (f"PROD-{i:04d}",
     f"Producto_{i}",
     random.choice(categorias),
     round(random.uniform(5.0, 500.0), 2),
     random.sample(["new", "sale", "trending", "limited", "eco", "premium"], k=random.randint(1, 3)))
    for i in range(1, N_PRODUCTS + 1)
]
schema_products = StructType([
    StructField("product_id",   StringType(),         True),
    StructField("nombre",       StringType(),         True),
    StructField("categoria",    StringType(),         True),
    StructField("precio_base",  DoubleType(),         True),
    StructField("tags",         ArrayType(StringType()), True),
])
df_products = spark.createDataFrame(products_data, schema=schema_products)

print(f"✓ Datasets generados:")
print(f"  df_orders_raw : {df_orders_raw.count():,} filas, {len(df_orders_raw.columns)} columnas")
print(f"  df_customers  : {df_customers.count():,} filas")
print(f"  df_products   : {df_products.count():,} filas")
```

#### Salida Esperada

```
✓ SparkSession iniciada correctamente
  Versión de Spark : 3.5.x
  Master           : local[*]
  App Name         : Lab04-SparkSQL-Transformaciones
  Spark UI         : http://localhost:4040

✓ Datasets generados:
  df_orders_raw : 150,000 filas, 11 columnas
  df_customers  : 5,000 filas
  df_products   : 200 filas
```

#### Verificación

```python
# Verificación del Paso 1
df_orders_raw.printSchema()
df_orders_raw.show(5, truncate=True)
print(f"Particiones de df_orders_raw: {df_orders_raw.rdd.getNumPartitions()}")
```

Debes ver el esquema con 11 columnas y 5 filas de muestra. El número de particiones debe ser mayor que 1 en modo `local[*]`.

---

### Paso 2: Diagnóstico y Limpieza de Datos

**Objetivo:** Detectar y cuantificar problemas de calidad en el dataset, luego aplicar las transformaciones de limpieza necesarias usando las funciones nativas de PySpark.

#### Instrucciones

1. **Diagnóstico de nulos y duplicados:**

```python
# Celda 3: Diagnóstico de calidad de datos

# 2.1 Contar nulos por columna
print("=== NULOS POR COLUMNA ===")
nulos_df = df_orders_raw.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_orders_raw.columns
])
nulos_df.show(truncate=False)

# 2.2 Contar duplicados por order_id
total_filas   = df_orders_raw.count()
ids_distintos = df_orders_raw.select("order_id").distinct().count()
duplicados    = total_filas - ids_distintos

print(f"\n=== RESUMEN DE CALIDAD ===")
print(f"Total filas          : {total_filas:,}")
print(f"IDs únicos           : {ids_distintos:,}")
print(f"Filas duplicadas     : {duplicados:,}")

# 2.3 Detectar precios negativos
precios_negativos = df_orders_raw.filter(F.col("precio_unit") < 0).count()
print(f"Precios negativos    : {precios_negativos:,}")

# 2.4 Detectar inconsistencias en estado
print("\n=== VALORES ÚNICOS EN 'estado' ===")
df_orders_raw.groupBy("estado").count().orderBy("count", ascending=False).show()
```

2. **Aplicar limpieza completa con la DataFrame API:**

```python
# Celda 4: Pipeline de limpieza de datos

df_orders_clean = (
    df_orders_raw
    # --- Paso A: Eliminar duplicados exactos por order_id (conservar primera ocurrencia) ---
    .dropDuplicates(["order_id"])

    # --- Paso B: Limpiar espacios en columnas de texto ---
    .withColumn("region",   F.trim(F.col("region")))
    .withColumn("estado",   F.trim(F.lower(F.col("estado"))))
    .withColumn("categoria", F.trim(F.col("categoria")))

    # --- Paso C: Normalizar estado con when/otherwise ---
    .withColumn("estado",
        F.when(F.col("estado").isin("completed", "complete"), "completed")
         .when(F.col("estado") == "pending",    "pending")
         .when(F.col("estado") == "cancelled",  "cancelled")
         .when(F.col("estado") == "refunded",   "refunded")
         .when(F.col("estado") == "shipped",    "shipped")
         .otherwise("unknown")
    )

    # --- Paso D: Reemplazar precios negativos o nulos con la mediana (usamos fillna con 0 como proxy) ---
    .withColumn("precio_unit",
        F.when(
            F.col("precio_unit").isNull() | (F.col("precio_unit") < 0),
            None   # marcar como nulo para tratamiento posterior
        ).otherwise(F.col("precio_unit"))
    )

    # --- Paso E: Rellenar nulos numéricos con valores por defecto ---
    .fillna({"precio_unit": 50.0, "cantidad": 1, "descuento": 0.0})

    # --- Paso F: Filtrar filas con fecha inválida (no sigue patrón YYYY-MM-DD) ---
    .withColumn("fecha_valida",
        F.col("fecha").rlike(r"^\d{4}-\d{2}-\d{2}$")
    )
    .filter(F.col("fecha_valida") == True)
    .drop("fecha_valida")

    # --- Paso G: Convertir fecha string a DateType ---
    .withColumn("fecha", F.to_date(F.col("fecha"), "yyyy-MM-dd"))

    # --- Paso H: Calcular monto total de la orden ---
    .withColumn("monto_total",
        F.round(
            F.col("cantidad") * F.col("precio_unit") * (1 - F.col("descuento")),
            2
        )
    )

    # --- Paso I: Agregar columna de año y mes para particionamiento lógico ---
    .withColumn("anio",  F.year(F.col("fecha")))
    .withColumn("mes",   F.month(F.col("fecha")))
)

# Persistir el DataFrame limpio en memoria para reutilización
df_orders_clean.cache()
df_orders_clean.count()  # materializar el caché

print("=== DATASET LIMPIO ===")
print(f"Filas después de limpieza: {df_orders_clean.count():,}")
df_orders_clean.printSchema()
df_orders_clean.show(5, truncate=False)
```

3. **Verificar la efectividad de la limpieza:**

```python
# Celda 5: Verificación post-limpieza

# Confirmar que no quedan nulos en columnas críticas
nulos_post = df_orders_clean.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ["order_id", "precio_unit", "cantidad", "monto_total", "fecha"]
])
print("Nulos en columnas críticas (deben ser 0):")
nulos_post.show()

# Confirmar estados normalizados
print("Estados únicos post-limpieza:")
df_orders_clean.groupBy("estado").count().orderBy("count", ascending=False).show()
```

#### Salida Esperada

```
=== DATASET LIMPIO ===
Filas después de limpieza: ~146,500  (varía levemente por aleatoriedad)

Nulos en columnas críticas (deben ser 0):
+--------+-----------+--------+------------+----+
|order_id|precio_unit|cantidad|monto_total |fecha|
+--------+-----------+--------+------------+----+
|0       |0          |0       |0           |0   |
+--------+-----------+--------+------------+----+

Estados únicos post-limpieza:
+---------+------+
|estado   |count |
+---------+------+
|completed|~44000|
|pending  |~29000|
|shipped  |~29000|
|cancelled|~29000|
|refunded |~14000|
|unknown  |~1000 |
+---------+------+
```

#### Verificación

```python
assert df_orders_clean.filter(F.col("precio_unit") < 0).count() == 0, "ERROR: Quedan precios negativos"
assert df_orders_clean.filter(F.col("monto_total").isNull()).count() == 0, "ERROR: Hay montos nulos"
print("✓ Todas las verificaciones de limpieza pasaron correctamente")
```

---

### Paso 3: Registro de Vistas Temporales y Consultas SQL Básicas

**Objetivo:** Registrar los DataFrames como vistas temporales y ejecutar consultas SQL con `spark.sql()`, demostrando la integración entre SQL y la DataFrame API.

#### Instrucciones

1. **Registrar vistas temporales:**

```python
# Celda 6: Registro de vistas temporales

# Registrar los tres DataFrames como vistas
df_orders_clean.createOrReplaceTempView("ordenes")
df_customers.createOrReplaceTempView("clientes")
df_products.createOrReplaceTempView("productos")

# Verificar las vistas disponibles en el catálogo
print("=== VISTAS REGISTRADAS EN EL CATÁLOGO ===")
spark.catalog.listTables()
for tabla in spark.catalog.listTables():
    print(f"  [{tabla.tableType}] {tabla.name}")
```

2. **Consultas SQL de análisis básico:**

```python
# Celda 7: Análisis de ventas por región y año

resultado_region = spark.sql("""
    SELECT
        region,
        anio,
        COUNT(*)                        AS num_ordenes,
        ROUND(SUM(monto_total), 2)      AS ventas_totales,
        ROUND(AVG(monto_total), 2)      AS ticket_promedio,
        ROUND(MAX(monto_total), 2)      AS orden_maxima,
        COUNT(DISTINCT customer_id)     AS clientes_unicos
    FROM ordenes
    WHERE estado = 'completed'
    GROUP BY region, anio
    HAVING SUM(monto_total) > 100000
    ORDER BY anio, ventas_totales DESC
""")

print("=== VENTAS COMPLETADAS POR REGIÓN Y AÑO ===")
resultado_region.show(20, truncate=False)
```

3. **Consulta con JOIN entre vistas:**

```python
# Celda 8: JOIN entre órdenes y clientes

resultado_join = spark.sql("""
    SELECT
        c.segmento,
        o.categoria,
        COUNT(o.order_id)               AS total_ordenes,
        ROUND(SUM(o.monto_total), 2)    AS ingresos,
        ROUND(AVG(o.descuento) * 100, 1) AS descuento_prom_pct
    FROM ordenes o
    INNER JOIN clientes c ON o.customer_id = c.customer_id
    WHERE o.estado IN ('completed', 'shipped')
      AND c.segmento IS NOT NULL
    GROUP BY c.segmento, o.categoria
    ORDER BY c.segmento, ingresos DESC
""")

print("=== INGRESOS POR SEGMENTO DE CLIENTE Y CATEGORÍA ===")
resultado_join.show(30, truncate=False)
```

4. **Subconsulta para identificar clientes top:**

```python
# Celda 9: Subconsulta — top 10 clientes por valor total

top_clientes = spark.sql("""
    SELECT
        c.customer_id,
        c.nombre,
        c.segmento,
        c.region,
        stats.total_gastado,
        stats.num_ordenes,
        stats.categoria_favorita
    FROM clientes c
    INNER JOIN (
        SELECT
            customer_id,
            ROUND(SUM(monto_total), 2)  AS total_gastado,
            COUNT(*)                    AS num_ordenes,
            FIRST(categoria)            AS categoria_favorita
        FROM ordenes
        WHERE estado = 'completed'
        GROUP BY customer_id
        HAVING COUNT(*) >= 5
    ) stats ON c.customer_id = stats.customer_id
    ORDER BY stats.total_gastado DESC
    LIMIT 10
""")

print("=== TOP 10 CLIENTES POR VALOR TOTAL (COMPLETADAS, ≥5 ÓRDENES) ===")
top_clientes.show(truncate=False)
```

#### Salida Esperada

```
=== VISTAS REGISTRADAS EN EL CATÁLOGO ===
  [TEMPORARY] ordenes
  [TEMPORARY] clientes
  [TEMPORARY] productos

=== VENTAS COMPLETADAS POR REGIÓN Y AÑO ===
+------+----+-----------+--------------+---------------+-------------+----------------+
|region|anio|num_ordenes|ventas_totales|ticket_promedio|orden_maxima |clientes_unicos |
+------+----+-----------+--------------+---------------+-------------+----------------+
|Norte |2022|...        |...           |...            |...          |...             |
...
```

#### Verificación

```python
# El resultado del JOIN debe tener exactamente 3 segmentos × N categorías
segmentos_unicos = resultado_join.select("segmento").distinct().count()
print(f"Segmentos únicos en resultado: {segmentos_unicos} (esperado: 3)")
assert segmentos_unicos == 3, "ERROR: Número incorrecto de segmentos"
print("✓ Consultas SQL ejecutadas correctamente")
```

---

### Paso 4: Operaciones Avanzadas sobre Columnas

**Objetivo:** Aplicar la Column API avanzada de PySpark: expresiones condicionales, casting, transformaciones matemáticas y limpieza con expresiones regulares.

#### Instrucciones

1. **Expresiones condicionales con `when`/`otherwise` y `isin`:**

```python
# Celda 10: Clasificación de órdenes con lógica condicional

df_orders_enriched = df_orders_clean \
    .withColumn("rango_monto",
        F.when(F.col("monto_total") < 50,   "Micro")
         .when(F.col("monto_total") < 200,  "Pequeña")
         .when(F.col("monto_total") < 500,  "Mediana")
         .when(F.col("monto_total") < 1000, "Grande")
         .otherwise("Premium")
    ) \
    .withColumn("es_orden_digital",
        F.col("metodo_pago").isin("paypal", "credit_card", "debit_card").cast("integer")
    ) \
    .withColumn("descuento_pct",
        F.round(F.col("descuento") * 100, 1)
    ) \
    .withColumn("precio_con_descuento",
        F.round(F.col("precio_unit") * (1 - F.col("descuento")), 2)
    ) \
    .withColumn("categoria_normalizada",
        F.regexp_replace(
            F.lower(F.trim(F.col("categoria"))),
            r"[^a-z0-9]", "_"
        )
    ) \
    .withColumn("order_id_numerico",
        F.regexp_replace(F.col("order_id"), r"ORD-", "").cast(IntegerType())
    )

print("=== DATAFRAME ENRIQUECIDO ===")
df_orders_enriched.select(
    "order_id", "monto_total", "rango_monto",
    "es_orden_digital", "descuento_pct",
    "precio_con_descuento", "categoria_normalizada"
).show(10, truncate=False)
```

2. **Transformaciones matemáticas y estadísticas:**

```python
# Celda 11: Métricas derivadas y normalización

# Calcular estadísticas de referencia para normalización
stats = df_orders_clean.agg(
    F.mean("monto_total").alias("media"),
    F.stddev("monto_total").alias("desv_std"),
    F.percentile_approx("monto_total", 0.5).alias("mediana")
).collect()[0]

media, desv_std, mediana = stats["media"], stats["desv_std"], stats["mediana"]
print(f"Media: {media:.2f} | Desv. Std: {desv_std:.2f} | Mediana: {mediana:.2f}")

# Agregar score Z y flag de outlier
df_orders_enriched = df_orders_enriched \
    .withColumn("z_score",
        F.round((F.col("monto_total") - F.lit(media)) / F.lit(desv_std), 3)
    ) \
    .withColumn("es_outlier",
        F.abs(F.col("z_score")) > 3.0
    )

# Mostrar distribución de rangos
print("\n=== DISTRIBUCIÓN POR RANGO DE MONTO ===")
df_orders_enriched.groupBy("rango_monto") \
    .agg(
        F.count("*").alias("cantidad"),
        F.round(F.sum("monto_total"), 2).alias("total")
    ) \
    .orderBy("total", ascending=False) \
    .show()

# Contar outliers
outliers = df_orders_enriched.filter(F.col("es_outlier") == True).count()
print(f"Órdenes outlier (|z| > 3): {outliers:,}")
```

3. **Renombrado y selección de columnas:**

```python
# Celda 12: Renombrado y proyección final

df_orders_final = df_orders_enriched \
    .withColumnRenamed("customer_id",  "id_cliente") \
    .withColumnRenamed("product_id",   "id_producto") \
    .withColumnRenamed("monto_total",  "valor_orden") \
    .drop("z_score", "es_outlier", "order_id_numerico")

# Reordenar columnas para legibilidad
columnas_ordenadas = [
    "order_id", "id_cliente", "id_producto",
    "categoria_normalizada", "region", "estado",
    "cantidad", "precio_unit", "descuento_pct",
    "precio_con_descuento", "valor_orden",
    "rango_monto", "es_orden_digital",
    "metodo_pago", "fecha", "anio", "mes"
]
df_orders_final = df_orders_final.select(columnas_ordenadas)

print(f"Columnas finales: {df_orders_final.columns}")
df_orders_final.show(5, truncate=False)
```

#### Salida Esperada

```
=== DISTRIBUCIÓN POR RANGO DE MONTO ===
+---------+--------+-------------+
|rango_monto|cantidad|total       |
+---------+--------+-------------+
|Mediana  |~45000  |~13,500,000  |
|Grande   |~35000  |~24,500,000  |
|Pequeña  |~30000  |~3,600,000   |
|Premium  |~25000  |~35,000,000  |
|Micro    |~11000  |~330,000     |
+---------+--------+-------------+

Órdenes outlier (|z| > 3): ~150
```

#### Verificación

```python
# Verificar que no hay nulos en las nuevas columnas calculadas
assert df_orders_final.filter(F.col("valor_orden").isNull()).count() == 0
assert df_orders_final.filter(F.col("rango_monto").isNull()).count() == 0
assert "id_cliente" in df_orders_final.columns
assert "customer_id" not in df_orders_final.columns
print("✓ Operaciones avanzadas de columnas verificadas correctamente")
```

---

### Paso 5: Transformaciones Estructurales — Pivot, Explode y Arrays

**Objetivo:** Aplicar transformaciones que cambian la forma del DataFrame: `pivot` para reshaping, `explode`/`posexplode` para desanidar arrays, y operaciones sobre estructuras complejas.

#### Instrucciones

1. **Transformación Pivot — Ventas por categoría y mes:**

```python
# Celda 13: Pivot — tabla de ventas por categoría y estado

pivot_categoria_estado = df_orders_final \
    .filter(F.col("anio") == 2023) \
    .groupBy("categoria_normalizada") \
    .pivot("estado", ["completed", "pending", "cancelled", "shipped", "refunded"]) \
    .agg(F.round(F.sum("valor_orden"), 2)) \
    .fillna(0.0)

print("=== PIVOT: VENTAS 2023 POR CATEGORÍA Y ESTADO ===")
pivot_categoria_estado.show(truncate=False)

# Pivot por mes (para análisis de tendencia mensual)
pivot_mensual = df_orders_final \
    .filter(
        (F.col("anio") == 2023) &
        (F.col("estado") == "completed")
    ) \
    .groupBy("categoria_normalizada") \
    .pivot("mes", list(range(1, 13))) \
    .agg(F.round(F.sum("valor_orden"), 0)) \
    .fillna(0.0)

print("\n=== PIVOT: VENTAS COMPLETADAS 2023 POR CATEGORÍA Y MES ===")
pivot_mensual.show(truncate=False)
```

2. **Explode de arrays — Tags de productos:**

```python
# Celda 14: Explode — desanidar el array de tags de productos

# Mostrar estructura original
print("=== ESTRUCTURA ORIGINAL DE PRODUCTOS (con array de tags) ===")
df_products.show(5, truncate=False)

# Explode: crea una fila por cada tag
df_products_exploded = df_products \
    .withColumn("tag", F.explode(F.col("tags"))) \
    .drop("tags")

print("\n=== DESPUÉS DE EXPLODE (una fila por tag) ===")
df_products_exploded.show(10, truncate=False)
print(f"Filas originales: {df_products.count()} | Filas después de explode: {df_products_exploded.count()}")

# Posexplode: incluye la posición del elemento en el array
df_products_posexploded = df_products \
    .select(
        "product_id", "nombre", "categoria",
        F.posexplode(F.col("tags")).alias("posicion", "tag")
    )

print("\n=== POSEXPLODE (con posición del tag) ===")
df_products_posexploded.show(10, truncate=False)
```

3. **Análisis de tags más frecuentes con JOIN post-explode:**

```python
# Celda 15: Análisis de tags con JOIN a órdenes

# Registrar la vista de productos explodidos
df_products_exploded.createOrReplaceTempView("productos_tags")

# Análisis: ingresos por tag de producto
ingresos_por_tag = spark.sql("""
    SELECT
        pt.tag,
        COUNT(DISTINCT o.order_id)      AS num_ordenes,
        COUNT(DISTINCT o.id_cliente)    AS clientes_unicos,
        ROUND(SUM(o.valor_orden), 2)    AS ingresos_totales,
        ROUND(AVG(o.valor_orden), 2)    AS ticket_promedio
    FROM ordenes o
    INNER JOIN productos_tags pt ON o.id_producto = pt.product_id
    WHERE o.estado = 'completed'
    GROUP BY pt.tag
    ORDER BY ingresos_totales DESC
""")

print("=== INGRESOS POR TAG DE PRODUCTO ===")
ingresos_por_tag.show(truncate=False)

# Verificar si una orden tiene productos con tag 'sale' usando array_contains
df_orders_with_tags = df_orders_final \
    .join(df_products.select("product_id", "tags"), 
          df_orders_final["id_producto"] == df_products["product_id"], 
          "left") \
    .withColumn("tiene_tag_sale",
        F.array_contains(F.col("tags"), "sale")
    ) \
    .drop("product_id", "tags")

sale_orders = df_orders_with_tags.filter(F.col("tiene_tag_sale") == True).count()
print(f"\nÓrdenes con productos etiquetados como 'sale': {sale_orders:,}")
```

#### Salida Esperada

```
=== DESPUÉS DE EXPLODE (una fila por tag) ===
+----------+------------+-------------+-----------+------+
|product_id|nombre      |categoria    |precio_base|tag   |
+----------+------------+-------------+-----------+------+
|PROD-0001 |Producto_1  |Electronics  |245.50     |new   |
|PROD-0001 |Producto_1  |Electronics  |245.50     |sale  |
...

Filas originales: 200 | Filas después de explode: ~440

=== INGRESOS POR TAG DE PRODUCTO ===
+---------+-----------+---------------+----------------+---------------+
|tag      |num_ordenes|clientes_unicos|ingresos_totales|ticket_promedio|
+---------+-----------+---------------+----------------+---------------+
|sale     |...        |...            |...             |...            |
|new      |...        |...            |...             |...            |
...
```

#### Verificación

```python
# El pivot debe tener exactamente 5 columnas de estado + 1 de categoría
assert len(pivot_categoria_estado.columns) == 6, "ERROR: Columnas incorrectas en pivot"
# El explode debe tener más filas que el original
assert df_products_exploded.count() > df_products.count()
print("✓ Transformaciones estructurales verificadas correctamente")
```

---

### Paso 6: Consultas SQL Avanzadas con Window Functions y Subconsultas

**Objetivo:** Ejecutar consultas SQL complejas que incluyen funciones de ventana, CTEs y subconsultas correlacionadas para análisis analítico avanzado.

#### Instrucciones

1. **Funciones de ventana en SQL:**

```python
# Celda 16: Window Functions en Spark SQL

ranking_clientes = spark.sql("""
    WITH ventas_cliente AS (
        SELECT
            customer_id     AS id_cliente,
            region,
            anio,
            ROUND(SUM(valor_orden), 2) AS total_gastado,
            COUNT(*)                   AS num_ordenes
        FROM ordenes
        WHERE estado = 'completed'
        GROUP BY customer_id, region, anio
    )
    SELECT
        id_cliente,
        region,
        anio,
        total_gastado,
        num_ordenes,
        RANK() OVER (
            PARTITION BY region, anio
            ORDER BY total_gastado DESC
        ) AS rank_en_region,
        ROUND(
            total_gastado / SUM(total_gastado) OVER (PARTITION BY region, anio) * 100,
            2
        ) AS pct_del_total_regional,
        ROUND(AVG(total_gastado) OVER (PARTITION BY region, anio), 2) AS promedio_regional
    FROM ventas_cliente
    QUALIFY RANK() OVER (PARTITION BY region, anio ORDER BY total_gastado DESC) <= 5
    ORDER BY anio, region, rank_en_region
""")

print("=== TOP 5 CLIENTES POR REGIÓN Y AÑO (con ranking y % del total) ===")
ranking_clientes.show(30, truncate=False)
```

> **Nota:** `QUALIFY` es una extensión de Spark SQL 3.x que permite filtrar sobre funciones de ventana directamente. Si tu versión no la soporta, usa una subconsulta envolvente.

2. **Análisis de tendencia mensual con LAG:**

```python
# Celda 17: Análisis de tendencia con LAG

tendencia_mensual = spark.sql("""
    WITH ventas_mes AS (
        SELECT
            anio,
            mes,
            ROUND(SUM(valor_orden), 2) AS ventas_mes
        FROM ordenes
        WHERE estado = 'completed'
        GROUP BY anio, mes
    )
    SELECT
        anio,
        mes,
        ventas_mes,
        LAG(ventas_mes, 1) OVER (ORDER BY anio, mes) AS ventas_mes_anterior,
        ROUND(
            (ventas_mes - LAG(ventas_mes, 1) OVER (ORDER BY anio, mes))
            / LAG(ventas_mes, 1) OVER (ORDER BY anio, mes) * 100,
            2
        ) AS variacion_pct
    FROM ventas_mes
    ORDER BY anio, mes
""")

print("=== TENDENCIA MENSUAL DE VENTAS CON VARIACIÓN % ===")
tendencia_mensual.show(36, truncate=False)
```

#### Salida Esperada

```
=== TOP 5 CLIENTES POR REGIÓN Y AÑO (con ranking y % del total) ===
+----------+------+----+-------------+-----------+--------------+----------------------+------------------+
|id_cliente|region|anio|total_gastado|num_ordenes|rank_en_region|pct_del_total_regional|promedio_regional |
+----------+------+----+-------------+-----------+--------------+----------------------+------------------+
|CUST-...  |Centro|2022|...          |...        |1             |...                   |...               |
...
```

#### Verificación

```python
# El ranking no debe exceder 5 por combinación región+año
max_rank = ranking_clientes.agg(F.max("rank_en_region")).collect()[0][0]
assert max_rank <= 5, f"ERROR: rank máximo es {max_rank}, debe ser ≤ 5"
print("✓ Window Functions SQL verificadas correctamente")
```

---

### Paso 7: Comparación RDD vs DataFrame vs Dataset

**Objetivo:** Implementar la misma operación analítica usando RDD y DataFrame para contrastar la expresividad, rendimiento y facilidad de uso de cada abstracción.

#### Instrucciones

1. **Operación equivalente con RDD:**

```python
# Celda 18: Implementación con RDD

import time

# Convertir a RDD para demostración
rdd_orders = df_orders_clean.rdd

# Calcular ventas totales por región usando RDD
start_rdd = time.time()

ventas_por_region_rdd = (
    rdd_orders
    .filter(lambda row: row["estado"] == "completed")
    .map(lambda row: (row["region"], row["monto_total"]))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
    .collect()
)

tiempo_rdd = time.time() - start_rdd

print("=== RESULTADO CON RDD ===")
for region, total in ventas_por_region_rdd:
    print(f"  {region:<10} → ${total:>15,.2f}")
print(f"\nTiempo de ejecución RDD: {tiempo_rdd:.3f} segundos")
```

2. **Misma operación con DataFrame API:**

```python
# Celda 19: Implementación equivalente con DataFrame

start_df = time.time()

ventas_por_region_df = (
    df_orders_clean
    .filter(F.col("estado") == "completed")
    .groupBy("region")
    .agg(F.round(F.sum("monto_total"), 2).alias("total_ventas"))
    .orderBy("total_ventas", ascending=False)
)
ventas_por_region_df.collect()  # acción para medir tiempo real

tiempo_df = time.time() - start_df

print("=== RESULTADO CON DATAFRAME ===")
ventas_por_region_df.show(truncate=False)
print(f"Tiempo de ejecución DataFrame: {tiempo_df:.3f} segundos")
```

3. **Misma operación con Spark SQL:**

```python
# Celda 20: Implementación con Spark SQL

start_sql = time.time()

ventas_por_region_sql = spark.sql("""
    SELECT
        region,
        ROUND(SUM(monto_total), 2) AS total_ventas
    FROM ordenes
    WHERE estado = 'completed'
    GROUP BY region
    ORDER BY total_ventas DESC
""")
ventas_por_region_sql.collect()

tiempo_sql = time.time() - start_sql

print("=== RESULTADO CON SPARK SQL ===")
ventas_por_region_sql.show(truncate=False)
print(f"Tiempo de ejecución SQL: {tiempo_sql:.3f} segundos")
```

4. **Tabla comparativa y análisis de planes:**

```python
# Celda 21: Comparativa y análisis de planes de ejecución

print("=" * 60)
print("COMPARATIVA DE RENDIMIENTO Y EXPRESIVIDAD")
print("=" * 60)
print(f"{'Abstracción':<15} {'Tiempo (s)':<12} {'Líneas código':<15} {'Optimizado'}")
print("-" * 60)
print(f"{'RDD':<15} {tiempo_rdd:<12.3f} {'~6':<15} {'No (manual)'}")
print(f"{'DataFrame':<15} {tiempo_df:<12.3f} {'~5':<15} {'Sí (Catalyst)'}")
print(f"{'Spark SQL':<15} {tiempo_sql:<12.3f} {'~7 (SQL)':<15} {'Sí (Catalyst)'}")
print("=" * 60)

# Analizar el plan de ejecución del DataFrame
print("\n=== PLAN DE EJECUCIÓN — DataFrame (explain simple) ===")
ventas_por_region_df.explain()

print("\n=== PLAN DE EJECUCIÓN — DataFrame (explain extendido) ===")
ventas_por_region_df.explain(extended=True)

# Nota sobre Dataset en Python
print("""
=== NOTA SOBRE DATASET API ===
La Dataset API tipada (Dataset[T]) es exclusiva de Scala y Java.
En Python, todos los DataFrames son equivalentes a Dataset[Row].
La diferencia clave es:
  - Scala/Java: Dataset[CaseClass] → seguridad de tipos en compilación
  - Python:     DataFrame (= Dataset[Row]) → verificación en runtime
  - RDD:        sin optimización de Catalyst, control total pero verboso
""")
```

#### Salida Esperada

```
============================================================
COMPARATIVA DE RENDIMIENTO Y EXPRESIVIDAD
============================================================
Abstracción     Tiempo (s)   Líneas código   Optimizado
------------------------------------------------------------
RDD             X.XXX        ~6              No (manual)
DataFrame       X.XXX        ~5              Sí (Catalyst)
Spark SQL       X.XXX        ~7 (SQL)        Sí (Catalyst)
============================================================

=== PLAN DE EJECUCIÓN — DataFrame (explain simple) ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [total_ventas#XX DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(total_ventas#XX DESC NULLS LAST, 8), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[region#XX], functions=[sum(monto_total#XX)])
         +- Exchange hashpartitioning(region#XX, 8), ENSURE_REQUIREMENTS
            +- HashAggregate(keys=[region#XX], functions=[partial_sum(monto_total#XX)])
               +- Filter (estado#XX = completed)
                  +- InMemoryTableScan ...
```

> **Observación clave:** DataFrame y SQL producen planes físicos idénticos. El RDD requiere más código y no se beneficia del Catalyst Optimizer.

#### Verificación

```python
# Los tres métodos deben producir los mismos resultados
rdd_dict = {r: t for r, t in ventas_por_region_rdd}
df_rows  = {r["region"]: r["total_ventas"] for r in ventas_por_region_df.collect()}

for region in rdd_dict:
    diff = abs(rdd_dict[region] - df_rows.get(region, 0))
    assert diff < 1.0, f"Discrepancia en región {region}: {diff}"

print("✓ Los tres métodos producen resultados equivalentes")
```

---

### Paso 8: Manejo de Errores en Pipelines de Datos

**Objetivo:** Implementar un pipeline robusto con manejo de errores, validación de datos y registro de filas problemáticas para simular un entorno de producción.

#### Instrucciones

1. **Pipeline con validación y separación de filas válidas/inválidas:**

```python
# Celda 22: Pipeline de datos robusto con validación

# Definir reglas de validación como columnas booleanas
df_validado = df_orders_raw \
    .withColumn("error_precio_nulo",
        F.col("precio_unit").isNull()
    ) \
    .withColumn("error_precio_negativo",
        F.col("precio_unit").isNotNull() & (F.col("precio_unit") < 0)
    ) \
    .withColumn("error_cantidad_nula",
        F.col("cantidad").isNull()
    ) \
    .withColumn("error_fecha_invalida",
        ~F.col("fecha").rlike(r"^\d{4}-\d{2}-\d{2}$")
    ) \
    .withColumn("tiene_errores",
        F.col("error_precio_nulo") |
        F.col("error_precio_negativo") |
        F.col("error_cantidad_nula") |
        F.col("error_fecha_invalida")
    )

# Separar filas válidas e inválidas
df_validas   = df_validado.filter(F.col("tiene_errores") == False)
df_invalidas = df_validado.filter(F.col("tiene_errores") == True)

print("=== REPORTE DE VALIDACIÓN ===")
print(f"Filas válidas   : {df_validas.count():,}")
print(f"Filas inválidas : {df_invalidas.count():,}")

# Resumen de tipos de error
print("\n=== DESGLOSE DE ERRORES ===")
df_invalidas.select(
    F.sum(F.col("error_precio_nulo").cast("int")).alias("precio_nulo"),
    F.sum(F.col("error_precio_negativo").cast("int")).alias("precio_negativo"),
    F.sum(F.col("error_cantidad_nula").cast("int")).alias("cantidad_nula"),
    F.sum(F.col("error_fecha_invalida").cast("int")).alias("fecha_invalida"),
).show()

# Guardar filas inválidas para auditoría (en modo local, en memoria)
df_invalidas \
    .select("order_id", "customer_id", "fecha", "precio_unit", "cantidad",
            "error_precio_nulo", "error_precio_negativo",
            "error_cantidad_nula", "error_fecha_invalida") \
    .show(10, truncate=False)
```

2. **Manejo de UDF con try/except para transformaciones inseguras:**

```python
# Celda 23: UDF con manejo de errores

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# UDF que puede fallar con entradas inesperadas
def calcular_margen_seguro(precio, descuento, costo_estimado_pct=0.6):
    """Calcula el margen de ganancia estimado con manejo de errores."""
    try:
        if precio is None or precio <= 0:
            return None
        if descuento is None:
            descuento = 0.0
        precio_neto = precio * (1 - descuento)
        costo = precio * costo_estimado_pct
        margen = (precio_neto - costo) / precio_neto * 100
        return round(float(margen), 2)
    except Exception:
        return None  # Retornar None en lugar de propagar el error

calcular_margen_udf = udf(calcular_margen_seguro, DoubleType())

df_con_margen = df_orders_clean \
    .withColumn("margen_pct",
        calcular_margen_udf(F.col("precio_unit"), F.col("descuento"))
    )

print("=== MÁRGENES CALCULADOS ===")
df_con_margen \
    .groupBy("categoria") \
    .agg(
        F.round(F.avg("margen_pct"), 2).alias("margen_promedio_pct"),
        F.count(F.when(F.col("margen_pct").isNull(), 1)).alias("errores_calculo")
    ) \
    .orderBy("margen_promedio_pct", ascending=False) \
    .show(truncate=False)
```

#### Salida Esperada

```
=== REPORTE DE VALIDACIÓN ===
Filas válidas   : ~143,000
Filas inválidas : ~7,000

=== DESGLOSE DE ERRORES ===
+------------+---------------+--------------+--------------+
|precio_nulo |precio_negativo|cantidad_nula |fecha_invalida|
+------------+---------------+--------------+--------------+
|~6,000      |~2,250         |~4,500        |~750          |
+------------+---------------+--------------+--------------+
```

#### Verificación

```python
# La suma de válidas + inválidas debe ser igual al total original
total_original = df_orders_raw.count()
total_split = df_validas.count() + df_invalidas.count()
assert total_original == total_split, \
    f"ERROR: {total_original} ≠ {total_split} (se perdieron filas)"
print(f"✓ Pipeline de validación correcto: {total_original:,} filas conservadas")
```

---

## 7. Validación y Pruebas Finales

Ejecuta las siguientes celdas de validación para confirmar que toda la práctica se completó correctamente:

```python
# Celda de Validación Final

print("=" * 65)
print("VALIDACIÓN FINAL — LAB 04")
print("=" * 65)

resultados = {}

# 1. Dataset limpio generado
try:
    assert df_orders_clean.count() > 100_000
    assert "monto_total" in df_orders_clean.columns
    assert "fecha" in df_orders_clean.columns
    resultados["1. Dataset limpio"] = "✓ PASS"
except AssertionError as e:
    resultados["1. Dataset limpio"] = f"✗ FAIL: {e}"

# 2. Vistas temporales registradas
try:
    vistas = [t.name for t in spark.catalog.listTables()]
    assert "ordenes" in vistas
    assert "clientes" in vistas
    assert "productos" in vistas
    resultados["2. Vistas temporales"] = "✓ PASS"
except AssertionError as e:
    resultados["2. Vistas temporales"] = f"✗ FAIL: {e}"

# 3. Consulta SQL con JOIN
try:
    test_sql = spark.sql("""
        SELECT COUNT(*) AS total
        FROM ordenes o
        INNER JOIN clientes c ON o.customer_id = c.customer_id
        WHERE o.estado = 'completed'
    """).collect()[0]["total"]
    assert test_sql > 0
    resultados["3. SQL con JOIN"] = f"✓ PASS ({test_sql:,} filas)"
except Exception as e:
    resultados["3. SQL con JOIN"] = f"✗ FAIL: {e}"

# 4. Columnas derivadas presentes
try:
    assert "rango_monto" in df_orders_enriched.columns
    assert "es_orden_digital" in df_orders_enriched.columns
    assert "z_score" in df_orders_enriched.columns
    resultados["4. Columnas derivadas"] = "✓ PASS"
except AssertionError as e:
    resultados["4. Columnas derivadas"] = f"✗ FAIL: {e}"

# 5. Pivot generado
try:
    assert "completed" in pivot_categoria_estado.columns
    assert "cancelled" in pivot_categoria_estado.columns
    resultados["5. Transformación Pivot"] = "✓ PASS"
except AssertionError as e:
    resultados["5. Transformación Pivot"] = f"✗ FAIL: {e}"

# 6. Explode de arrays
try:
    assert df_products_exploded.count() > df_products.count()
    assert "tag" in df_products_exploded.columns
    resultados["6. Explode de arrays"] = "✓ PASS"
except AssertionError as e:
    resultados["6. Explode de arrays"] = f"✗ FAIL: {e}"

# 7. Pipeline de validación
try:
    total = df_orders_raw.count()
    split = df_validas.count() + df_invalidas.count()
    assert total == split
    resultados["7. Pipeline validación"] = "✓ PASS"
except AssertionError as e:
    resultados["7. Pipeline validación"] = f"✗ FAIL: {e}"

# Imprimir resultados
for check, resultado in resultados.items():
    print(f"  {check:<28} {resultado}")

passed = sum(1 for v in resultados.values() if "PASS" in v)
total_checks = len(resultados)
print("-" * 65)
print(f"  Resultado: {passed}/{total_checks} verificaciones pasadas")
print("=" * 65)

if passed == total_checks:
    print("\n  🎉 ¡Práctica 4 completada exitosamente!")
else:
    print(f"\n  ⚠️  Revisar las verificaciones fallidas antes de continuar.")
```

---

## 8. Resolución de Problemas

### Problema 1: `AnalysisException: Table or view not found`

**Síntomas:**
Al ejecutar `spark.sql("SELECT * FROM ordenes ...")`, Spark lanza:
```
AnalysisException: Table or view not found: ordenes; ...
```

**Causa:**
Las vistas temporales están vinculadas a la `SparkSession` activa. Si el kernel de Jupyter fue reiniciado, o si la SparkSession fue cerrada y recreada, todas las vistas registradas con `createOrReplaceTempView()` se pierden. También ocurre si se ejecuta la celda de consulta SQL antes de la celda que registra la vista.

**Solución:**

```python
# Paso 1: Verificar si la vista existe antes de consultarla
def vista_existe(nombre: str) -> bool:
    return spark.catalog.tableExists(nombre)

for vista in ["ordenes", "clientes", "productos"]:
    if not vista_existe(vista):
        print(f"⚠️  Vista '{vista}' no encontrada. Re-registrando...")

# Paso 2: Re-registrar las vistas (re-ejecutar las celdas correspondientes)
# Si la SparkSession fue recreada, también debes re-ejecutar las celdas
# de generación de datos (Paso 1) y limpieza (Paso 2) antes de registrar.

# Verificar el estado de la sesión
print(f"SparkSession activa: {spark.sparkContext._jvm is not None}")
print(f"Vistas disponibles: {[t.name for t in spark.catalog.listTables()]}")
```

---

### Problema 2: `OutOfMemoryError` o Kernel muerto en Jupyter durante operaciones de Pivot

**Síntomas:**
El kernel de Jupyter muere silenciosamente o Spark lanza `java.lang.OutOfMemoryError: Java heap space` durante la operación de `pivot` o al ejecutar joins sobre el dataset completo.

**Causa:**
La operación `pivot` genera una columna por cada valor único del campo pivotado. Si no se especifica la lista de valores explícitamente, Spark primero realiza un `collect()` interno para descubrir todos los valores únicos, lo que puede saturar la memoria del driver. Adicionalmente, en máquinas con 8 GB de RAM, el caché de múltiples DataFrames puede agotar la memoria JVM.

**Solución:**

```python
# Paso 1: SIEMPRE especificar los valores del pivot explícitamente
# MAL: puede agotar memoria al descubrir valores
# df.groupBy("cat").pivot("estado").agg(F.sum("valor"))

# BIEN: lista explícita de valores
estados_conocidos = ["completed", "pending", "cancelled", "shipped", "refunded"]
df.groupBy("categoria_normalizada") \
  .pivot("estado", estados_conocidos) \   # ← lista explícita
  .agg(F.round(F.sum("valor_orden"), 2))

# Paso 2: Liberar caché de DataFrames que ya no se necesitan
df_orders_raw.unpersist()   # liberar el raw si ya tienes el clean cacheado

# Paso 3: Reducir el dataset para pruebas si el hardware es limitado
df_sample = df_orders_clean.sample(fraction=0.3, seed=42)
df_sample.createOrReplaceTempView("ordenes")  # usar muestra para pruebas

# Paso 4: Verificar uso de memoria
print(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().totalMemory() // (1024**2), "MB asignados a JVM")

# Paso 5: Si el problema persiste, reiniciar con más memoria
# Detener la sesión actual y recrear con más memoria:
# spark.stop()
# spark = SparkSession.builder \
#     .config("spark.driver.memory", "4g") \  # aumentar si hay RAM disponible
#     .getOrCreate()
```

---

## 9. Limpieza del Entorno

Ejecuta las siguientes celdas al finalizar la práctica para liberar recursos:

```python
# Celda de Limpieza

print("Iniciando limpieza del entorno...")

# 1. Eliminar vistas temporales del catálogo
vistas_a_eliminar = ["ordenes", "clientes", "productos", "productos_tags"]
for vista in vistas_a_eliminar:
    if spark.catalog.tableExists(vista):
        spark.catalog.dropTempView(vista)
        print(f"  ✓ Vista '{vista}' eliminada")

# 2. Liberar DataFrames cacheados
for df_name, df_obj in [("df_orders_clean", df_orders_clean),
                         ("df_orders_enriched", df_orders_enriched)]:
    try:
        df_obj.unpersist()
        print(f"  ✓ Caché de {df_name} liberado")
    except Exception:
        pass

# 3. Limpiar caché general de Spark
spark.catalog.clearCache()
print("  ✓ Caché general de Spark limpiado")

# 4. Verificar que no quedan vistas
vistas_restantes = spark.catalog.listTables()
print(f"\n  Vistas restantes en catálogo: {len(vistas_restantes)}")

# 5. Detener la SparkSession (SOLO al finalizar completamente)
# Descomenta la siguiente línea SOLO si no continuarás con otra práctica:
# spark.stop()
# print("  ✓ SparkSession detenida")

print("\n✓ Limpieza completada. SparkSession disponible para la siguiente práctica.")
```

---

## 10. Resumen

### Conceptos Aplicados en Esta Práctica

| Concepto                          | Método / API Utilizado                                      |
|-----------------------------------|-------------------------------------------------------------|
| Registro de vistas temporales     | `createOrReplaceTempView()`, `spark.catalog`               |
| Consultas SQL completas           | `spark.sql()` con JOIN, GROUP BY, HAVING, subconsultas     |
| Window Functions en SQL           | `RANK()`, `LAG()`, `PARTITION BY`, `QUALIFY`               |
| Expresiones condicionales         | `when()`, `otherwise()`, `isin()`                          |
| Conversión de tipos               | `.cast()`, `to_date()`, `regexp_replace()`                  |
| Manejo de nulos                   | `isNull()`, `isNotNull()`, `fillna()`, `dropna()`          |
| Transformaciones estructurales    | `pivot()`, `explode()`, `posexplode()`, `array_contains()`  |
| Análisis de planes de ejecución   | `explain()`, `explain(extended=True)`                      |
| Comparativa RDD vs DataFrame      | Implementación equivalente, medición de tiempo             |
| Pipeline robusto con validación   | Columnas de error, separación válidas/inválidas, UDFs      |

### Conclusiones Clave

1. **SQL y DataFrame API son intercambiables:** Ambos enfoques pasan por el Catalyst Optimizer y producen planes de ejecución idénticos. La elección es de preferencia y contexto, no de rendimiento.

2. **El RDD es más verboso y menos optimizado:** Para operaciones analíticas sobre datos estructurados, la DataFrame API y Spark SQL son siempre preferibles. El RDD tiene su lugar en transformaciones no estructuradas o cuando se necesita control total del procesamiento.

3. **La limpieza de datos es iterativa:** El diagnóstico con `isNull()`, `rlike()` y conteos agrupados debe preceder siempre a cualquier transformación analítica.

4. **`explain()` es tu aliado en optimización:** Revisar el plan físico permite identificar operaciones costosas (shuffles, broadcasts) y oportunidades de mejora antes de escalar a producción.

5. **El Catalyst Optimizer aplica `predicate pushdown` automáticamente:** Los filtros `WHERE` son empujados hacia la fuente de datos, reduciendo el volumen de datos procesados en etapas posteriores.

### Recursos Adicionales

- [Spark SQL Functions Reference — PySpark Docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Spark SQL Guide — Apache Spark 3.5](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Catalyst Optimizer Deep Dive — Databricks Blog](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)
- [PySpark Window Functions Guide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [Learning Spark, 2nd Edition — O'Reilly (Capítulos 5 y 6)](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)

---
