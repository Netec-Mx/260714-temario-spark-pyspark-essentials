---LAB_START---
LAB_ID: 09-00-01
---MARKDOWN---
# Práctica 8 — Uso de Agregaciones, Agrupaciones y Relaciones

## Metadatos

| Campo            | Detalle                                      |
|------------------|----------------------------------------------|
| **Duración**     | 60 minutos                                   |
| **Complejidad**  | Media                                        |
| **Nivel Bloom**  | Aplicar (Apply)                              |
| **Práctica N°**  | 8 de 9                                       |
| **Módulo**       | Spark SQL — Agrupaciones, Joins y Métricas   |

---

## Descripción General

En esta práctica aplicarás las operaciones de agrupación y combinación de tablas más utilizadas en pipelines de analítica empresarial. Trabajarás con un modelo de datos relacional de ventas compuesto por cuatro tablas —**clientes**, **productos**, **órdenes** y **detalle de órdenes**— para calcular métricas de negocio reales usando `groupBy()`, `agg()`, `rollup()`, `cube()`, `pivot()` y todos los tipos de `join` disponibles en PySpark. Al finalizar, exportarás los resultados a pandas y generarás gráficos básicos con matplotlib para construir un mini-dashboard analítico.

---

## Objetivos de Aprendizaje

- [ ] Aplicar `groupBy()` con `agg()` para calcular métricas de negocio: ventas totales por región, ticket promedio por cliente y productos más vendidos por categoría.
- [ ] Implementar los tipos de join de PySpark (inner, left, right, full outer, left_semi, left_anti, cross) con casos de uso justificados sobre datasets relacionados.
- [ ] Utilizar funciones de agregación avanzadas: `collect_list`, `collect_set`, `countDistinct`, `percentile_approx` y `approx_count_distinct`.
- [ ] Construir subtotales jerárquicos con `rollup()` y `cube()`, e interpretar correctamente las filas con valor `null`.
- [ ] Exportar resultados a pandas y generar visualizaciones con matplotlib como parte de un pipeline analítico completo.

---

## Prerrequisitos

### Conocimiento Previo

| Área                          | Nivel Requerido                                                                  |
|-------------------------------|----------------------------------------------------------------------------------|
| Práctica 7 completada         | Conocimiento de acciones, escritura de datos y optimización básica               |
| SQL con GROUP BY / HAVING     | Sólido — debe poder escribir consultas con subgrupos y filtros de agregación     |
| Modelos relacionales          | Comprensión de claves primarias, foráneas y cardinalidad de relaciones           |
| Estadística básica            | Media, desviación estándar, percentiles                                          |
| PySpark DataFrames API        | Creación de DataFrames, selección de columnas, filtros con `filter()`/`where()`  |

### Acceso y Software

| Componente              | Versión Requerida        |
|-------------------------|--------------------------|
| Apache Spark            | 3.5.x                    |
| PySpark                 | 3.5.x                    |
| Java (JDK)              | 11 o 17 LTS              |
| Python                  | 3.10 o 3.11              |
| JupyterLab              | 7.x                      |
| pandas                  | 2.0+                     |
| matplotlib              | 3.7+                     |
| findspark               | 2.0.1+                   |

---

## Entorno del Laboratorio

### Configuración de Memoria Recomendada

> ⚠️ **Nota para equipos con 8 GB RAM:** Configura `spark.driver.memory=2g` y `spark.executor.memory=2g` en la `SparkConf` para evitar errores `OutOfMemoryError`.

### Estructura de Archivos del Laboratorio

Crea la siguiente estructura de directorios antes de comenzar:

```bash
mkdir -p ~/spark-labs/lab09
cd ~/spark-labs/lab09
mkdir -p data output notebooks
```

### Verificación del Entorno

Ejecuta el siguiente comando en una terminal para confirmar que Spark está disponible:

```bash
python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
```

Salida esperada:
```
PySpark version: 3.5.x
```

---

## Pasos del Laboratorio

---

### Paso 1 — Inicialización de la SparkSession y Creación del Modelo de Datos

**Objetivo:** Inicializar Spark con configuración adecuada y construir el modelo de datos relacional de ventas que se utilizará durante toda la práctica.

#### Instrucciones

1. Abre JupyterLab y crea un nuevo notebook llamado `lab09_agregaciones_joins.ipynb` dentro de `~/spark-labs/lab09/notebooks/`.

2. En la primera celda, importa las dependencias necesarias e inicializa la `SparkSession`:

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# Inicializar SparkSession con configuración de memoria
spark = (
    SparkSession.builder
    .appName("Lab09-Agregaciones-Joins")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")   # Reducido para modo local
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
print(f"Spark UI disponible en: http://localhost:4040")
```

3. En la siguiente celda, crea los cuatro DataFrames que representan el modelo de ventas. Para simular volumen suficiente, generaremos datos programáticamente:

```python
from datetime import date, timedelta
import random

random.seed(42)

# ─────────────────────────────────────────────
# TABLA: clientes  (200 registros)
# ─────────────────────────────────────────────
regiones   = ["Norte", "Sur", "Este", "Oeste", "Centro"]
segmentos  = ["Premium", "Estándar", "Básico"]

clientes_data = [
    (
        i,
        f"Cliente_{i:04d}",
        f"cliente{i}@ejemplo.com",
        random.choice(regiones),
        random.choice(segmentos),
        random.choice(["Activo", "Inactivo"])
    )
    for i in range(1, 201)
]

schema_clientes = StructType([
    StructField("cliente_id",  IntegerType(), False),
    StructField("nombre",      StringType(),  True),
    StructField("email",       StringType(),  True),
    StructField("region",      StringType(),  True),
    StructField("segmento",    StringType(),  True),
    StructField("estado",      StringType(),  True),
])

df_clientes = spark.createDataFrame(clientes_data, schema_clientes)
df_clientes.cache()

# ─────────────────────────────────────────────
# TABLA: productos  (50 registros)
# ─────────────────────────────────────────────
categorias = ["Electrónica", "Ropa", "Hogar", "Deportes", "Alimentación"]
marcas     = ["MarcaA", "MarcaB", "MarcaC", "MarcaD"]

productos_data = [
    (
        i,
        f"Producto_{i:03d}",
        random.choice(categorias),
        random.choice(marcas),
        round(random.uniform(10.0, 1500.0), 2),
        random.choice(["Disponible", "Agotado"])
    )
    for i in range(1, 51)
]

schema_productos = StructType([
    StructField("producto_id",  IntegerType(), False),
    StructField("nombre",       StringType(),  True),
    StructField("categoria",    StringType(),  True),
    StructField("marca",        StringType(),  True),
    StructField("precio_base",  DoubleType(),  True),
    StructField("disponibilidad", StringType(), True),
])

df_productos = spark.createDataFrame(productos_data, schema_productos)
df_productos.cache()

# ─────────────────────────────────────────────
# TABLA: ordenes  (2 000 registros)
# ─────────────────────────────────────────────
estados_orden  = ["Completada", "Pendiente", "Cancelada", "Devuelta"]
base_date      = date(2023, 1, 1)

ordenes_data = [
    (
        i,
        random.randint(1, 200),                                     # cliente_id
        base_date + timedelta(days=random.randint(0, 364)),          # fecha
        random.choice(estados_orden),
        random.choice(["Tarjeta", "Transferencia", "Efectivo"]),
        round(random.uniform(0.0, 0.20), 2)                          # descuento
    )
    for i in range(1, 2001)
]

schema_ordenes = StructType([
    StructField("orden_id",    IntegerType(), False),
    StructField("cliente_id",  IntegerType(), True),
    StructField("fecha",       DateType(),    True),
    StructField("estado",      StringType(),  True),
    StructField("metodo_pago", StringType(),  True),
    StructField("descuento",   DoubleType(),  True),
])

df_ordenes = spark.createDataFrame(ordenes_data, schema_ordenes)
df_ordenes.cache()

# ─────────────────────────────────────────────
# TABLA: detalle_ordenes  (6 000 registros)
# ─────────────────────────────────────────────
detalle_data = [
    (
        i,
        random.randint(1, 2000),    # orden_id
        random.randint(1, 50),      # producto_id
        random.randint(1, 10),      # cantidad
        round(random.uniform(10.0, 1500.0), 2)  # precio_unitario
    )
    for i in range(1, 6001)
]

schema_detalle = StructType([
    StructField("detalle_id",      IntegerType(), False),
    StructField("orden_id",        IntegerType(), True),
    StructField("producto_id",     IntegerType(), True),
    StructField("cantidad",        IntegerType(), True),
    StructField("precio_unitario", DoubleType(),  True),
])

df_detalle = spark.createDataFrame(detalle_data, schema_detalle)
df_detalle.cache()

print("Modelo de datos creado:")
print(f"  clientes:        {df_clientes.count():>6,} filas")
print(f"  productos:       {df_productos.count():>6,} filas")
print(f"  ordenes:         {df_ordenes.count():>6,} filas")
print(f"  detalle_ordenes: {df_detalle.count():>6,} filas")
```

4. Registra las vistas temporales para poder usar Spark SQL en pasos posteriores:

```python
df_clientes.createOrReplaceTempView("clientes")
df_productos.createOrReplaceTempView("productos")
df_ordenes.createOrReplaceTempView("ordenes")
df_detalle.createOrReplaceTempView("detalle_ordenes")

print("Vistas temporales registradas correctamente.")
```

#### Salida Esperada

```
Spark version: 3.5.x
Spark UI disponible en: http://localhost:4040

Modelo de datos creado:
  clientes:           200 filas
  productos:           50 filas
  ordenes:          2,000 filas
  detalle_ordenes:  6,000 filas

Vistas temporales registradas correctamente.
```

#### Verificación

```python
# Verificar esquemas y primeras filas
for nombre, df in [("clientes", df_clientes), ("productos", df_productos),
                   ("ordenes", df_ordenes), ("detalle_ordenes", df_detalle)]:
    print(f"\n── {nombre} ──")
    df.printSchema()
    df.show(3, truncate=False)
```

---

### Paso 2 — Agrupaciones Básicas con `groupBy()` y `agg()`

**Objetivo:** Calcular métricas de negocio fundamentales usando `groupBy()` combinado con múltiples funciones de agregación en una sola llamada a `agg()`.

#### Instrucciones

1. **Ventas totales y métricas por región.** Calcula el importe total, el número de órdenes y el descuento promedio agrupado por región del cliente:

```python
# ── 2.1 Métricas de órdenes por región ──────────────────────────────────────
ventas_region = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .join(
        df_detalle
        .withColumn("importe_linea",
                    F.col("cantidad") * F.col("precio_unitario")),
        on="orden_id",
        how="inner"
    )
    .groupBy("region")
    .agg(
        F.sum("importe_linea").alias("ventas_totales"),
        F.count(F.expr("DISTINCT orden_id")).alias("num_ordenes"),
        F.avg("descuento").alias("descuento_promedio"),
        F.min("importe_linea").alias("linea_minima"),
        F.max("importe_linea").alias("linea_maxima"),
        F.stddev("importe_linea").alias("desviacion_std")
    )
    .withColumn("ventas_totales",   F.round("ventas_totales", 2))
    .withColumn("descuento_promedio", F.round("descuento_promedio", 4))
    .withColumn("desviacion_std",   F.round("desviacion_std", 2))
    .orderBy(F.desc("ventas_totales"))
)

print("=== Métricas de ventas por región ===")
ventas_region.show(truncate=False)
```

2. **Ticket promedio por segmento de cliente.** Calcula el importe promedio por orden agrupado por segmento:

```python
# ── 2.2 Ticket promedio por segmento ────────────────────────────────────────
ticket_segmento = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .join(
        df_detalle.groupBy("orden_id")
                  .agg(F.sum(F.col("cantidad") * F.col("precio_unitario"))
                         .alias("total_orden")),
        on="orden_id",
        how="inner"
    )
    .groupBy("segmento")
    .agg(
        F.avg("total_orden").alias("ticket_promedio"),
        F.count("orden_id").alias("total_ordenes"),
        F.sum("total_orden").alias("revenue_total")
    )
    .withColumn("ticket_promedio", F.round("ticket_promedio", 2))
    .withColumn("revenue_total",   F.round("revenue_total", 2))
    .orderBy(F.desc("ticket_promedio"))
)

print("=== Ticket promedio por segmento de cliente ===")
ticket_segmento.show(truncate=False)
```

3. **Productos más vendidos por categoría.** Identifica el producto con mayor cantidad vendida dentro de cada categoría usando `groupBy` con dos columnas:

```python
# ── 2.3 Cantidad vendida por categoría y producto ───────────────────────────
ventas_categoria_producto = (
    df_detalle
    .join(df_productos, on="producto_id", how="inner")
    .groupBy("categoria", "producto_id", df_productos["nombre"].alias("nombre_producto"))
    .agg(
        F.sum("cantidad").alias("unidades_vendidas"),
        F.sum(F.col("cantidad") * F.col("precio_unitario")).alias("revenue")
    )
    .withColumn("revenue", F.round("revenue", 2))
    .orderBy("categoria", F.desc("unidades_vendidas"))
)

print("=== Top productos por categoría (primeras 15 filas) ===")
ventas_categoria_producto.show(15, truncate=False)
```

#### Salida Esperada

```
=== Métricas de ventas por región ===
+-------+--------------+----------+------------------+------------+------------+--------------+
|region |ventas_totales|num_ordenes|descuento_promedio|linea_minima|linea_maxima|desviacion_std|
+-------+--------------+----------+------------------+------------+------------+--------------+
|Norte  |  xxxxxxxx.xx |      xxx |            x.xxx |       xx.xx|    xxxx.xx |       xxx.xx |
...
```

> Los valores exactos variarán según la semilla aleatoria; lo importante es que aparezcan las 5 regiones y todas las métricas calculadas.

#### Verificación

```python
# Verificar que aparecen exactamente 5 regiones
assert ventas_region.count() == 5, "Deben existir exactamente 5 regiones"

# Verificar que las ventas totales son positivas
assert ventas_region.filter(F.col("ventas_totales") <= 0).count() == 0

# Verificar que los 3 segmentos están presentes
assert ticket_segmento.count() == 3, "Deben existir 3 segmentos"

print("✓ Verificaciones de agrupación básica superadas")
```

---

### Paso 3 — Subtotales Jerárquicos con `rollup()` y `cube()`

**Objetivo:** Generar reportes con subtotales automáticos usando `rollup()` para jerarquías unidireccionales y `cube()` para análisis multidimensional, interpretando correctamente las filas con `null`.

#### Instrucciones

1. **Reporte de ventas con ROLLUP por región y segmento:**

```python
# ── 3.1 ROLLUP: región → segmento → gran total ──────────────────────────────
df_para_rollup = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .join(
        df_detalle
        .withColumn("importe", F.col("cantidad") * F.col("precio_unitario")),
        on="orden_id",
        how="inner"
    )
    .select("region", "segmento", "importe")
)

reporte_rollup = (
    df_para_rollup
    .rollup("region", "segmento")
    .agg(
        F.sum("importe").alias("ventas_totales"),
        F.count("*").alias("num_lineas")
    )
    .withColumn("ventas_totales", F.round("ventas_totales", 2))
    # Reemplazar null con etiquetas legibles
    .select(
        F.coalesce(F.col("region"),   F.lit("── TOTAL GENERAL ──")).alias("region"),
        F.coalesce(F.col("segmento"), F.lit("  ↳ SUBTOTAL")).alias("segmento"),
        "ventas_totales",
        "num_lineas"
    )
    .orderBy("region", "segmento")
)

print("=== Reporte ROLLUP: Ventas por Región y Segmento ===")
reporte_rollup.show(30, truncate=False)
```

2. **Análisis multidimensional con CUBE por región y categoría:**

```python
# ── 3.2 CUBE: región × categoría (todas las combinaciones) ──────────────────
df_para_cube = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .join(df_detalle, on="orden_id", how="inner")
    .join(df_productos, on="producto_id", how="inner")
    .withColumn("importe", F.col("cantidad") * F.col("precio_unitario"))
    .select("region", df_productos["categoria"], "importe")
)

reporte_cube = (
    df_para_cube
    .cube("region", "categoria")
    .agg(F.round(F.sum("importe"), 2).alias("ventas_totales"))
    .orderBy("region", "categoria")
)

print("=== Reporte CUBE: todas las combinaciones región × categoría ===")
reporte_cube.show(40, truncate=False)

# Extraer solo los subtotales por categoría (región es null)
print("\n=== Subtotales por Categoría (independiente de región) ===")
reporte_cube.filter(
    F.col("region").isNull() & F.col("categoria").isNotNull()
).orderBy(F.desc("ventas_totales")).show(truncate=False)

# Extraer solo el gran total
print("\n=== Gran Total ===")
reporte_cube.filter(
    F.col("region").isNull() & F.col("categoria").isNull()
).show(truncate=False)
```

3. **Comparación de filas generadas por cada método:**

```python
# ── 3.3 Comparación cuantitativa ────────────────────────────────────────────
n_groupby = df_para_cube.groupBy("region", "categoria").count().count()
n_rollup  = df_para_cube.rollup("region", "categoria").count().count()
n_cube    = df_para_cube.cube("region", "categoria").count().count()

print(f"Filas generadas por groupBy : {n_groupby}")
print(f"Filas generadas por rollup  : {n_rollup}")
print(f"Filas generadas por cube    : {n_cube}")
print(f"\nFilas extra de rollup vs groupBy : {n_rollup - n_groupby}")
print(f"Filas extra de cube   vs groupBy : {n_cube  - n_groupby}")
```

#### Salida Esperada

```
=== Reporte ROLLUP: Ventas por Región y Segmento ===
+--------------------+-------------+--------------+----------+
|region              |segmento     |ventas_totales|num_lineas|
+--------------------+-------------+--------------+----------+
|── TOTAL GENERAL ──|  ↳ SUBTOTAL |   xxxxxxxxx  |   xxxx   |
|Centro              |  ↳ SUBTOTAL |   xxxxxxxx   |   xxxx   |
|Centro              |Básico       |   xxxxxxx    |   xxx    |
...

Filas generadas por groupBy : 25
Filas generadas por rollup  : 31
Filas generadas por cube    : 37
```

#### Verificación

```python
# rollup debe generar exactamente n_regiones + 1 filas extra vs groupBy
n_regiones = df_clientes.select("region").distinct().count()
n_categorias = df_productos.select("categoria").distinct().count()

assert n_rollup == n_groupby + n_regiones + 1, \
    f"rollup debe tener {n_groupby + n_regiones + 1} filas"
assert n_cube == n_groupby + n_regiones + n_categorias + 1, \
    f"cube debe tener {n_groupby + n_regiones + n_categorias + 1} filas"

print("✓ Verificaciones de rollup y cube superadas")
```

---

### Paso 4 — Tipos de Join en PySpark

**Objetivo:** Implementar y comparar los distintos tipos de join disponibles en PySpark, comprendiendo cuándo usar cada uno según el caso de uso analítico.

#### Instrucciones

1. **Inner Join — Órdenes con datos completos de cliente y producto:**

```python
# ── 4.1 INNER JOIN: solo registros con coincidencia en ambas tablas ──────────
df_ordenes_completo = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .join(
        df_detalle.join(df_productos, on="producto_id", how="inner"),
        on="orden_id",
        how="inner"
    )
    .select(
        "orden_id",
        df_clientes["nombre"].alias("cliente"),
        "region",
        "segmento",
        df_productos["nombre"].alias("producto"),
        "categoria",
        "cantidad",
        "precio_unitario",
        (F.col("cantidad") * F.col("precio_unitario")).alias("importe"),
        "fecha",
        df_ordenes["estado"].alias("estado_orden")
    )
)

print(f"Registros en join completo (inner): {df_ordenes_completo.count():,}")
df_ordenes_completo.show(5, truncate=False)
```

2. **Left Join — Clientes con o sin órdenes:**

```python
# ── 4.2 LEFT JOIN: todos los clientes, tengan o no órdenes ──────────────────
clientes_con_ordenes = (
    df_clientes
    .join(df_ordenes, on="cliente_id", how="left")
    .groupBy("cliente_id", df_clientes["nombre"], "region", "segmento")
    .agg(
        F.count("orden_id").alias("num_ordenes"),
        F.sum(F.when(F.col("estado") == "Completada", 1).otherwise(0))
         .alias("ordenes_completadas")
    )
    .orderBy(F.asc("num_ordenes"))
)

print("=== Clientes con y sin órdenes (LEFT JOIN) ===")
clientes_con_ordenes.show(10, truncate=False)

# Identificar clientes sin ninguna orden
clientes_sin_ordenes = clientes_con_ordenes.filter(F.col("num_ordenes") == 0)
print(f"\nClientes sin ninguna orden: {clientes_sin_ordenes.count()}")
```

3. **Left Anti Join — Clientes que NUNCA han ordenado:**

```python
# ── 4.3 LEFT ANTI JOIN: clientes sin ningún registro en órdenes ─────────────
clientes_huerfanos = (
    df_clientes
    .join(df_ordenes, on="cliente_id", how="left_anti")
    .select("cliente_id", "nombre", "region", "segmento", "estado")
)

print(f"=== Clientes sin órdenes (LEFT ANTI JOIN): {clientes_huerfanos.count()} ===")
clientes_huerfanos.show(10, truncate=False)
```

4. **Left Semi Join — Clientes que SÍ tienen al menos una orden:**

```python
# ── 4.4 LEFT SEMI JOIN: clientes que tienen al menos una orden ───────────────
clientes_activos = (
    df_clientes
    .join(df_ordenes, on="cliente_id", how="left_semi")
    .select("cliente_id", "nombre", "region", "segmento")
)

print(f"=== Clientes con órdenes (LEFT SEMI JOIN): {clientes_activos.count()} ===")
clientes_activos.show(5, truncate=False)

# Verificación: anti + semi = total clientes
total = clientes_huerfanos.count() + clientes_activos.count()
print(f"\nVerificación: {clientes_huerfanos.count()} + {clientes_activos.count()} = {total} "
      f"(total clientes: {df_clientes.count()})")
```

5. **Full Outer Join — Detectar inconsistencias en el modelo de datos:**

```python
# ── 4.5 FULL OUTER JOIN: detectar registros huérfanos en ambas tablas ───────
full_join_detalle_productos = (
    df_detalle
    .join(df_productos, on="producto_id", how="full")
    .select(
        F.col("producto_id"),
        F.col("detalle_id"),
        df_productos["nombre"].alias("nombre_producto"),
        F.col("cantidad"),
        F.col("precio_unitario")
    )
)

# Registros en detalle sin producto correspondiente
detalle_sin_producto = full_join_detalle_productos.filter(
    F.col("nombre_producto").isNull()
)
# Productos sin ninguna línea de detalle
productos_sin_detalle = full_join_detalle_productos.filter(
    F.col("detalle_id").isNull()
)

print(f"Líneas de detalle sin producto: {detalle_sin_producto.count()}")
print(f"Productos sin líneas de detalle: {productos_sin_detalle.count()}")
```

6. **Join con condición múltiple — Precio con tolerancia:**

```python
# ── 4.6 JOIN con condición compuesta ────────────────────────────────────────
# Detectar líneas de detalle donde el precio cobrado difiere más de un 20%
# respecto al precio base del producto

condicion_precio = (
    (df_detalle["producto_id"] == df_productos["producto_id"]) &
    (
        F.abs(df_detalle["precio_unitario"] - df_productos["precio_base"]) /
        df_productos["precio_base"] > 0.20
    )
)

precios_anomalos = (
    df_detalle
    .join(df_productos, condicion_precio, how="inner")
    .select(
        df_detalle["detalle_id"],
        df_productos["nombre"].alias("producto"),
        df_productos["precio_base"],
        df_detalle["precio_unitario"],
        F.round(
            (F.abs(df_detalle["precio_unitario"] - df_productos["precio_base"]) /
             df_productos["precio_base"]) * 100, 2
        ).alias("desviacion_pct")
    )
    .orderBy(F.desc("desviacion_pct"))
)

print(f"=== Precios con desviación > 20% respecto al precio base ===")
print(f"Registros encontrados: {precios_anomalos.count():,}")
precios_anomalos.show(10, truncate=False)
```

#### Salida Esperada

```
Registros en join completo (inner): x,xxx

=== Clientes con y sin órdenes (LEFT JOIN) ===
...
Clientes sin ninguna orden: x  (puede ser 0 dado que cliente_id va de 1 a 200)

=== Clientes sin órdenes (LEFT ANTI JOIN): x ===
=== Clientes con órdenes (LEFT SEMI JOIN): xxx ===
Verificación: x + xxx = 200 (total clientes: 200)
```

#### Verificación

```python
# La suma de anti + semi debe ser igual al total de clientes
assert clientes_huerfanos.count() + clientes_activos.count() == df_clientes.count(), \
    "Anti join + semi join debe sumar el total de clientes"

# El inner join no puede tener más filas que el detalle de órdenes
assert df_ordenes_completo.count() <= df_detalle.count() * 2

print("✓ Verificaciones de joins superadas")
```

---

### Paso 5 — Funciones de Agregación Avanzadas

**Objetivo:** Aplicar funciones de agregación avanzadas para obtener insights que no son posibles con las funciones básicas: listas de valores por grupo, estadísticas robustas y cardinalidad aproximada.

#### Instrucciones

1. **`collect_list` y `collect_set` — Agrupar valores en arrays:**

```python
# ── 5.1 collect_list y collect_set por cliente ──────────────────────────────
historial_cliente = (
    df_ordenes
    .join(df_detalle, on="orden_id", how="inner")
    .join(df_productos, on="producto_id", how="inner")
    .groupBy("cliente_id")
    .agg(
        F.collect_list("orden_id").alias("lista_ordenes"),           # con duplicados
        F.collect_set(df_productos["categoria"]).alias("categorias_compradas"),  # sin duplicados
        F.size(F.collect_set(df_productos["categoria"])).alias("num_categorias_distintas"),
        F.count("detalle_id").alias("total_lineas")
    )
    .orderBy(F.desc("num_categorias_distintas"))
)

print("=== Historial de compras por cliente (collect_list / collect_set) ===")
historial_cliente.show(10, truncate=False)
```

2. **`percentile_approx` y `approx_count_distinct` — Estadísticas robustas:**

```python
# ── 5.2 Percentiles y cardinalidad aproximada ───────────────────────────────
estadisticas_categoria = (
    df_detalle
    .join(df_productos, on="producto_id", how="inner")
    .withColumn("importe", F.col("cantidad") * F.col("precio_unitario"))
    .groupBy("categoria")
    .agg(
        F.count("*").alias("num_lineas"),
        F.round(F.avg("importe"), 2).alias("importe_promedio"),
        F.round(F.percentile_approx("importe", 0.50), 2).alias("mediana"),
        F.round(F.percentile_approx("importe", 0.25), 2).alias("p25"),
        F.round(F.percentile_approx("importe", 0.75), 2).alias("p75"),
        F.round(F.percentile_approx("importe", 0.95), 2).alias("p95"),
        F.approx_count_distinct("orden_id").alias("ordenes_distintas_aprox"),
        F.countDistinct("producto_id").alias("productos_distintos_exacto")
    )
    .orderBy("categoria")
)

print("=== Estadísticas robustas por categoría ===")
estadisticas_categoria.show(truncate=False)
```

3. **`first`, `last` y `countDistinct` — Análisis temporal:**

```python
# ── 5.3 Primera y última orden por cliente ──────────────────────────────────
actividad_cliente = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .groupBy("cliente_id", df_clientes["nombre"])
    .agg(
        F.min("fecha").alias("primera_orden"),
        F.max("fecha").alias("ultima_orden"),
        F.countDistinct("orden_id").alias("ordenes_unicas"),
        F.first("metodo_pago").alias("primer_metodo_pago"),
        F.last("metodo_pago").alias("ultimo_metodo_pago"),
        F.count(F.when(F.col("estado") == "Cancelada", 1)).alias("ordenes_canceladas")
    )
    .withColumn(
        "dias_activo",
        F.datediff(F.col("ultima_orden"), F.col("primera_orden"))
    )
    .orderBy(F.desc("ordenes_unicas"))
)

print("=== Actividad por cliente (first / last / countDistinct) ===")
actividad_cliente.show(10, truncate=False)
```

4. **Agregación condicional con `when` — Tasa de conversión:**

```python
# ── 5.4 Métricas condicionales por región ───────────────────────────────────
tasa_conversion = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .groupBy("region")
    .agg(
        F.count("orden_id").alias("total_ordenes"),
        F.sum(F.when(F.col("estado") == "Completada", 1).otherwise(0))
         .alias("completadas"),
        F.sum(F.when(F.col("estado") == "Cancelada", 1).otherwise(0))
         .alias("canceladas"),
        F.sum(F.when(F.col("estado") == "Devuelta", 1).otherwise(0))
         .alias("devueltas")
    )
    .withColumn("tasa_exito_pct",
        F.round(F.col("completadas") / F.col("total_ordenes") * 100, 2))
    .withColumn("tasa_cancelacion_pct",
        F.round(F.col("canceladas") / F.col("total_ordenes") * 100, 2))
    .orderBy(F.desc("tasa_exito_pct"))
)

print("=== Tasas de conversión por región ===")
tasa_conversion.show(truncate=False)
```

#### Verificación

```python
# Verificar que collect_set no tiene duplicados dentro de cada array
max_categorias = historial_cliente.select(
    F.max("num_categorias_distintas")
).collect()[0][0]
assert max_categorias <= 5, "No puede haber más de 5 categorías distintas"

# Verificar que los percentiles están ordenados correctamente
p_check = estadisticas_categoria.select("categoria", "p25", "mediana", "p75").collect()
for row in p_check:
    assert row["p25"] <= row["mediana"] <= row["p75"], \
        f"Percentiles fuera de orden en categoría: {row['categoria']}"

print("✓ Verificaciones de agregaciones avanzadas superadas")
```

---

### Paso 6 — Tabla Pivot y Análisis Cruzado

**Objetivo:** Construir tablas de contingencia con `pivot()` para analizar métricas de dos dimensiones simultáneamente en formato de matriz.

#### Instrucciones

1. **Pivot: ventas por región y categoría:**

```python
# ── 6.1 Tabla pivot: ventas por región (filas) × categoría (columnas) ───────
lista_categorias = [row["categoria"] for row in
                    df_productos.select("categoria").distinct().collect()]
lista_categorias.sort()

pivot_ventas = (
    df_ordenes
    .join(df_clientes, on="cliente_id", how="inner")
    .join(df_detalle, on="orden_id", how="inner")
    .join(df_productos, on="producto_id", how="inner")
    .withColumn("importe", F.col("cantidad") * F.col("precio_unitario"))
    .groupBy("region")
    .pivot("categoria", lista_categorias)
    .agg(F.round(F.sum("importe"), 2))
    .orderBy("region")
)

print("=== Tabla Pivot: Ventas por Región × Categoría ===")
pivot_ventas.show(truncate=False)
```

2. **Pivot: número de órdenes por método de pago y estado:**

```python
# ── 6.2 Pivot: conteo de órdenes por método de pago × estado ────────────────
pivot_metodo_estado = (
    df_ordenes
    .groupBy("metodo_pago")
    .pivot("estado")
    .agg(F.count("orden_id"))
    .fillna(0)
    .orderBy("metodo_pago")
)

print("=== Tabla Pivot: Órdenes por Método de Pago × Estado ===")
pivot_metodo_estado.show(truncate=False)
```

#### Verificación

```python
# El número de columnas del pivot debe ser: 1 (región) + n_categorías
assert len(pivot_ventas.columns) == 1 + len(lista_categorias), \
    "La tabla pivot debe tener una columna por categoría"

print("✓ Verificaciones de pivot superadas")
```

---

### Paso 7 — Dashboard Analítico con pandas y matplotlib

**Objetivo:** Exportar los resultados clave a pandas y generar un dashboard de 4 gráficos que visualice las métricas calculadas durante la práctica.

#### Instrucciones

1. **Preparar los DataFrames para exportación:**

```python
# ── 7.1 Exportar resultados a pandas ────────────────────────────────────────
pd_ventas_region    = ventas_region.toPandas()
pd_ticket_segmento  = ticket_segmento.toPandas()
pd_tasa_conversion  = tasa_conversion.toPandas()
pd_estadisticas_cat = estadisticas_categoria.toPandas()

print("DataFrames exportados a pandas:")
for nombre, df_pd in [
    ("ventas_region", pd_ventas_region),
    ("ticket_segmento", pd_ticket_segmento),
    ("tasa_conversion", pd_tasa_conversion),
    ("estadisticas_cat", pd_estadisticas_cat)
]:
    print(f"  {nombre}: {df_pd.shape}")
```

2. **Generar el dashboard de 4 gráficos:**

```python
# ── 7.2 Dashboard analítico ──────────────────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle("Dashboard Analítico — Modelo de Ventas\nPráctica 8: Agregaciones y Joins",
             fontsize=15, fontweight="bold", y=1.01)

# ── Gráfico 1: Ventas totales por región (barras horizontales) ───────────────
ax1 = axes[0, 0]
pd_ventas_region_sorted = pd_ventas_region.sort_values("ventas_totales")
bars = ax1.barh(
    pd_ventas_region_sorted["region"],
    pd_ventas_region_sorted["ventas_totales"],
    color=plt.cm.Blues(
        [0.4 + 0.6 * i / len(pd_ventas_region_sorted)
         for i in range(len(pd_ventas_region_sorted))]
    )
)
ax1.set_title("Ventas Totales por Región", fontweight="bold")
ax1.set_xlabel("Ventas ($)")
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"${x:,.0f}"
))
for bar in bars:
    width = bar.get_width()
    ax1.text(width * 1.01, bar.get_y() + bar.get_height() / 2,
             f"${width:,.0f}", va="center", fontsize=8)

# ── Gráfico 2: Ticket promedio por segmento (barras verticales) ──────────────
ax2 = axes[0, 1]
colores_seg = {"Premium": "#2196F3", "Estándar": "#4CAF50", "Básico": "#FF9800"}
ax2.bar(
    pd_ticket_segmento["segmento"],
    pd_ticket_segmento["ticket_promedio"],
    color=[colores_seg.get(s, "#9E9E9E") for s in pd_ticket_segmento["segmento"]],
    edgecolor="white", linewidth=1.5
)
ax2.set_title("Ticket Promedio por Segmento de Cliente", fontweight="bold")
ax2.set_ylabel("Ticket Promedio ($)")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
for i, (_, row) in enumerate(pd_ticket_segmento.iterrows()):
    ax2.text(i, row["ticket_promedio"] * 1.01,
             f"${row['ticket_promedio']:,.0f}", ha="center", fontsize=9)

# ── Gráfico 3: Tasa de éxito por región (barras apiladas) ────────────────────
ax3 = axes[1, 0]
x = range(len(pd_tasa_conversion))
ax3.bar(x, pd_tasa_conversion["tasa_exito_pct"],
        label="Completadas %", color="#4CAF50", alpha=0.85)
ax3.bar(x, pd_tasa_conversion["tasa_cancelacion_pct"],
        bottom=pd_tasa_conversion["tasa_exito_pct"],
        label="Canceladas %", color="#F44336", alpha=0.85)
ax3.set_xticks(list(x))
ax3.set_xticklabels(pd_tasa_conversion["region"], rotation=15)
ax3.set_title("Tasa de Éxito y Cancelación por Región", fontweight="bold")
ax3.set_ylabel("Porcentaje (%)")
ax3.legend(loc="upper right", fontsize=8)
ax3.set_ylim(0, 115)

# ── Gráfico 4: Boxplot de percentiles por categoría ──────────────────────────
ax4 = axes[1, 1]
categorias_ord = pd_estadisticas_cat.sort_values("mediana")
positions = range(len(categorias_ord))
for i, (_, row) in enumerate(categorias_ord.iterrows()):
    ax4.plot([i, i], [row["p25"], row["p75"]],
             color="#1565C0", linewidth=8, alpha=0.4, solid_capstyle="round")
    ax4.plot(i, row["mediana"], "o", color="#1565C0", markersize=10, zorder=5)
    ax4.plot(i, row["importe_promedio"], "D", color="#FF6F00",
             markersize=7, zorder=5, label="Media" if i == 0 else "")
    ax4.plot([i, i], [row["p25"], row["p95"]],
             color="#90CAF9", linewidth=2, linestyle="--")

ax4.set_xticks(list(positions))
ax4.set_xticklabels(categorias_ord["categoria"], rotation=20, ha="right", fontsize=8)
ax4.set_title("Distribución de Importes por Categoría\n(P25–P75 barra, ● mediana, ◆ media)",
              fontweight="bold")
ax4.set_ylabel("Importe por Línea ($)")
ax4.legend(fontsize=8)

plt.tight_layout()

# Guardar
output_path = "../output/dashboard_lab09.png"
plt.savefig(output_path, dpi=150, bbox_inches="tight")
plt.show()
print(f"\n✓ Dashboard guardado en: {output_path}")
```

#### Salida Esperada

Se generará un archivo `dashboard_lab09.png` con 4 gráficos:
- **Superior izquierdo:** Barras horizontales con ventas totales por región.
- **Superior derecho:** Barras verticales con ticket promedio por segmento (Premium > Estándar > Básico).
- **Inferior izquierdo:** Barras apiladas con tasas de éxito/cancelación por región.
- **Inferior derecho:** Visualización de percentiles por categoría.

#### Verificación

```python
import os
assert os.path.exists(output_path), "El archivo del dashboard no fue creado"
assert os.path.getsize(output_path) > 50_000, "El archivo del dashboard parece estar vacío"
print(f"✓ Dashboard generado correctamente ({os.path.getsize(output_path):,} bytes)")
```

---

## Validación y Pruebas Finales

Ejecuta el siguiente bloque de validación integral al finalizar todos los pasos:

```python
# ════════════════════════════════════════════════════════════════════════════
# VALIDACIÓN INTEGRAL — Lab 09-00-01
# ════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("VALIDACIÓN INTEGRAL — Lab 09-00-01")
print("=" * 60)

resultados = {}

# ── Validación 1: Modelo de datos ────────────────────────────────────────────
resultados["modelo_datos"] = (
    df_clientes.count() == 200 and
    df_productos.count() == 50 and
    df_ordenes.count() == 2000 and
    df_detalle.count() == 6000
)

# ── Validación 2: groupBy y agg ──────────────────────────────────────────────
resultados["groupby_agg"] = (
    ventas_region.count() == 5 and
    ticket_segmento.count() == 3 and
    "ventas_totales" in ventas_region.columns and
    "ticket_promedio" in ticket_segmento.columns
)

# ── Validación 3: rollup y cube ──────────────────────────────────────────────
resultados["rollup_cube"] = (
    n_rollup > n_groupby and
    n_cube > n_rollup
)

# ── Validación 4: joins ──────────────────────────────────────────────────────
resultados["joins"] = (
    clientes_huerfanos.count() + clientes_activos.count() == 200 and
    df_ordenes_completo.count() > 0
)

# ── Validación 5: agregaciones avanzadas ────────────────────────────────────
resultados["agg_avanzadas"] = (
    "categorias_compradas" in historial_cliente.columns and
    "mediana" in estadisticas_categoria.columns and
    "p95" in estadisticas_categoria.columns
)

# ── Validación 6: pivot ──────────────────────────────────────────────────────
resultados["pivot"] = (
    len(pivot_ventas.columns) == 1 + len(lista_categorias)
)

# ── Validación 7: dashboard ──────────────────────────────────────────────────
import os
resultados["dashboard"] = os.path.exists(output_path)

# ── Reporte final ─────────────────────────────────────────────────────────────
todos_ok = True
for nombre, ok in resultados.items():
    estado = "✓ PASS" if ok else "✗ FAIL"
    print(f"  {estado}  {nombre}")
    if not ok:
        todos_ok = False

print("=" * 60)
if todos_ok:
    print("✓ TODAS LAS VALIDACIONES SUPERADAS — Práctica 8 completada")
else:
    print("✗ Algunas validaciones fallaron. Revisa los pasos indicados.")
print("=" * 60)
```

---

## Resolución de Problemas

### Problema 1 — `AnalysisException: Resolved attribute(s) missing from child`

**Síntoma:** Al encadenar múltiples joins, Spark lanza un error de tipo `AnalysisException` indicando que una columna no puede resolverse en el plan de ejecución. El error suele aparecer cuando se referencia `df["columna"]` de un DataFrame que ya fue transformado en pasos anteriores.

**Causa:** Cuando se realizan joins sucesivos sobre DataFrames que comparten el mismo nombre de columna (por ejemplo, `nombre` aparece tanto en `df_clientes` como en `df_productos`), Spark no puede determinar de forma unívoca a cuál se refiere la expresión. El problema se agrava si se usa la misma variable de DataFrame en múltiples puntos del pipeline.

**Solución:**
1. Usa `alias()` sobre el DataFrame antes del join para distinguir las columnas ambiguas:
   ```python
   df_clientes_alias = df_clientes.alias("cli")
   df_productos_alias = df_productos.alias("prod")
   resultado = df_detalle \
       .join(df_clientes_alias, on="cliente_id") \
       .join(df_productos_alias, on="producto_id") \
       .select(F.col("cli.nombre").alias("cliente"),
               F.col("prod.nombre").alias("producto"))
   ```
2. Alternativamente, renombra las columnas conflictivas antes del join con `.withColumnRenamed("nombre", "nombre_cliente")`.
3. Cuando uses `on="columna"` (string), Spark elimina automáticamente la columna duplicada; cuando uses una condición booleana, ambas columnas permanecen y deben seleccionarse explícitamente.

---

### Problema 2 — `collect_list` devuelve listas de tamaño inesperadamente grande / `OutOfMemoryError` al usar `collect_set`

**Síntoma:** Al ejecutar `collect_list` sobre un grupo con muchos elementos, la celda tarda demasiado o el driver lanza `java.lang.OutOfMemoryError`. En algunos casos, `show()` muestra arrays con miles de elementos por fila.

**Causa:** `collect_list` y `collect_set` acumulan **todos** los valores del grupo en la memoria del driver. Si un grupo tiene decenas de miles de filas (por ejemplo, un cliente con miles de órdenes), el array resultante puede saturar la memoria disponible. Este problema es silencioso en modo local porque el driver y los ejecutores comparten la misma JVM.

**Solución:**
1. Limita el tamaño de las listas usando `slice()` o una ventana previa:
   ```python
   from pyspark.sql.functions import slice

   historial_limitado = (
       df_ordenes
       .groupBy("cliente_id")
       .agg(F.collect_list("orden_id").alias("lista_ordenes"))
       .withColumn("ultimas_10_ordenes", slice("lista_ordenes", 1, 10))
   )
   ```
2. Si solo necesitas el conteo, usa `count()` o `countDistinct()` en lugar de `collect_set` seguido de `size()`.
3. Aumenta la memoria del driver temporalmente si el análisis requiere listas completas:
   ```python
   spark.stop()
   spark = (SparkSession.builder
            .config("spark.driver.memory", "4g")
            .getOrCreate())
   ```
4. En producción, considera usar `approx_count_distinct` en lugar de `collect_set` cuando solo necesitas la cardinalidad.

---

## Limpieza del Entorno

Ejecuta el siguiente bloque al finalizar la práctica para liberar recursos:

```python
# ── Limpieza de caché ────────────────────────────────────────────────────────
df_clientes.unpersist()
df_productos.unpersist()
df_ordenes.unpersist()
df_detalle.unpersist()

# ── Eliminar vistas temporales ───────────────────────────────────────────────
for vista in ["clientes", "productos", "ordenes", "detalle_ordenes"]:
    spark.catalog.dropTempView(vista)
    print(f"Vista '{vista}' eliminada")

# ── Cerrar SparkSession ──────────────────────────────────────────────────────
spark.stop()
print("\n✓ SparkSession cerrada. Recursos liberados.")
```

Para limpiar los archivos de salida generados (opcional):

```bash
# Ejecutar desde terminal si se desea eliminar los archivos de salida
rm -rf ~/spark-labs/lab09/output/dashboard_lab09.png
```

---

## Resumen

En esta práctica aplicaste las operaciones de agrupación y combinación de tablas más importantes del ecosistema Spark SQL sobre un modelo de datos relacional de ventas de cuatro tablas. Los conceptos clave trabajados fueron:

| Concepto                        | Método / Función                                         | Caso de Uso Demostrado                                   |
|---------------------------------|----------------------------------------------------------|----------------------------------------------------------|
| Agrupación básica               | `groupBy().agg()`                                        | Ventas totales por región, ticket promedio por segmento  |
| Subtotales jerárquicos          | `rollup()`                                               | Reporte región → segmento → gran total                   |
| Análisis multidimensional       | `cube()`                                                 | Todas las combinaciones región × categoría               |
| Joins de combinación            | `inner`, `left`, `right`, `full`                         | Órdenes completas, clientes sin órdenes, inconsistencias |
| Joins de filtrado               | `left_semi`, `left_anti`                                 | Clientes activos vs. clientes huérfanos                  |
| Join con condición compuesta    | `join(condicion_booleana)`                               | Detección de precios anómalos                            |
| Funciones avanzadas             | `collect_list`, `collect_set`, `percentile_approx`       | Historial de categorías, distribución de importes        |
| Tabla de contingencia           | `pivot()`                                                | Ventas por región × categoría en formato matricial       |
| Visualización                   | `toPandas()` + `matplotlib`                              | Dashboard analítico de 4 gráficos                        |

### Puntos Clave para Recordar

- `rollup` genera subtotales en **una dirección jerárquica**; `cube` genera subtotales para **todas las combinaciones posibles**. Las filas con `null` en las columnas de agrupación representan niveles de subtotal, no datos faltantes.
- `left_anti` y `left_semi` son herramientas poderosas para **auditoría de calidad de datos**: detectar registros huérfanos y confirmar la existencia de relaciones, respectivamente.
- `collect_list` y `collect_set` son útiles para consolidar valores, pero deben usarse con precaución en grupos de gran cardinalidad para evitar problemas de memoria.
- Especificar la lista de valores del `pivot()` explícitamente (en lugar de dejar que Spark la infiera) mejora el rendimiento al evitar un paso adicional de análisis del plan.

### Recursos Adicionales

- [Documentación oficial PySpark — GroupedData](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html)
- [Documentación oficial PySpark — DataFrame.join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
- [Spark SQL Functions Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Databricks Blog — CUBE y ROLLUP en Spark SQL](https://www.databricks.com/blog/2016/02/09/reshaping-data-with-pivot-in-apache-spark.html)

---
LAB_END---
