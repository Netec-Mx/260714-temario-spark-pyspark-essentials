# Uso de Funciones en Spark SQL

## Metadatos

| Campo            | Detalle                                      |
|------------------|----------------------------------------------|
| **Duración**     | 60 minutos                                   |
| **Complejidad**  | Alta                                         |
| **Nivel Bloom**  | Aplicar                                      |
| **Práctica**     | 9 de 9 — Práctica integradora final del curso |

---

## Descripción General

Esta práctica integradora cierra el curso aplicando el espectro completo de funciones avanzadas de Spark SQL sobre un dataset de e-commerce. Trabajarás con funciones de fecha y hora, funciones de texto, manipulación de colecciones (arrays y maps), funciones de ventana (Window Functions), User Defined Functions (UDFs) escalares y vectorizadas, y finalizarás analizando los planes de ejecución generados por el Catalyst Optimizer. Al completar esta práctica habrás consolidado todos los conceptos del curso en un pipeline analítico cohesivo y de calidad profesional.

---

## Objetivos de Aprendizaje

- [ ] Aplicar funciones de fecha y hora de Spark SQL (`to_timestamp`, `date_trunc`, `date_add`, `last_day`, `unix_timestamp`, `year`, `month`, `dayofweek`) para análisis de estacionalidad y series temporales.
- [ ] Utilizar funciones de texto avanzadas (`soundex`, `levenshtein`, `translate`, `format_string`, `sentences`, `regexp_extract_all`) para limpieza y enriquecimiento de datos.
- [ ] Manipular tipos de colección con funciones nativas (`transform`, `filter`, `zip_with`, `map_keys`, `map_values`, `flatten`) sobre campos de arrays y maps.
- [ ] Implementar Window Functions (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `sum` y `avg` con `rowsBetween`) para cálculos analíticos avanzados.
- [ ] Crear UDFs con `@udf` y `@pandas_udf`, registrarlas en el catálogo SQL, y analizar planes de ejecución del Catalyst Optimizer con `explain()`.

---

## Prerrequisitos

### Conocimiento previo
- Prácticas 1–8 completadas satisfactoriamente.
- Dominio de la DataFrame API y Spark SQL (SELECT, GROUP BY, JOIN).
- Comprensión de tipos complejos: `ArrayType`, `MapType`, `StructType`.
- Familiaridad con funciones analíticas SQL (`OVER`, `PARTITION BY`, `ORDER BY`).
- Experiencia con funciones lambda en Python (`lambda x: ...`).

### Acceso requerido
- Entorno local con Apache Spark 3.5.x y PySpark 3.5.x instalados y funcionando.
- Puerto 4040 disponible para la Spark UI (verificar que no esté bloqueado).
- Acceso al repositorio de datasets del curso o capacidad de generarlos (el notebook incluye generación sintética).
- JupyterLab 7.x o Jupyter Notebook 7.x ejecutándose correctamente.

---

## Entorno de Laboratorio

### Hardware recomendado

| Recurso       | Mínimo        | Recomendado                     |
|---------------|---------------|---------------------------------|
| RAM           | 8 GB          | 16 GB                           |
| CPU           | 4 núcleos     | 6–8 núcleos (64 bits, VT-x/AMD-V)|
| Almacenamiento| 20 GB libres  | 30 GB libres                    |

### Software requerido

| Componente         | Versión       |
|--------------------|---------------|
| Java (JDK)         | 11 o 17 LTS   |
| Apache Spark       | 3.5.x         |
| Python             | 3.10 o 3.11   |
| PySpark            | 3.5.x         |
| JupyterLab         | 7.x           |
| pandas             | 2.0+          |
| NumPy              | 1.24+         |
| findspark          | 2.0.1+        |

### Configuración inicial del entorno

Ejecuta los siguientes comandos en una celda de verificación **antes** de comenzar la práctica:

```python
# Celda 0 — Verificación del entorno
import sys
import subprocess

print(f"Python: {sys.version}")

# Verificar PySpark
try:
    import pyspark
    print(f"PySpark: {pyspark.__version__}")
except ImportError:
    print("ERROR: PySpark no está instalado. Ejecuta: pip install pyspark==3.5.1")

# Verificar pandas y numpy
import pandas as pd
import numpy as np
print(f"pandas: {pd.__version__}")
print(f"NumPy:  {np.__version__}")
```

```python
# Celda 1 — Inicialización de SparkSession con configuración para esta práctica
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType, MapType, TimestampType, LongType
)

spark = SparkSession.builder \
    .appName("Practica9_FuncionesSpark") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
print(f"Spark UI disponible en: http://localhost:4040")
```

> **Nota para Windows:** Si encuentras errores de permisos al escribir archivos, asegúrate de que `HADOOP_HOME` apunte a la carpeta donde está `winutils.exe` y que `%HADOOP_HOME%\bin` esté en el `PATH`.

---

## Instrucciones Paso a Paso

---

### Paso 1: Generación del Dataset de E-Commerce

**Objetivo:** Crear el dataset sintético de e-commerce que se usará durante toda la práctica. El dataset incluye campos de texto libre, fechas en múltiples formatos, arrays de etiquetas y maps de atributos.

#### Instrucciones

1. Crea una nueva celda en tu notebook y nómbrala con el comentario `# Paso 1 — Generación del dataset`.

2. Ejecuta el siguiente código para generar el dataset sintético:

```python
# Paso 1 — Generación del dataset de e-commerce
import random
from datetime import datetime, timedelta

random.seed(42)

# Categorías y productos
categorias = ["Electrónica", "Ropa", "Hogar", "Deportes", "Libros", "Juguetes"]
productos_por_categoria = {
    "Electrónica": ["Laptop Pro 15", "Tablet X10", "Auriculares BT", "Smartwatch Z", "Cámara 4K"],
    "Ropa":        ["Camiseta Básica", "Jeans Slim", "Vestido Floral", "Chaqueta Polar", "Zapatillas Run"],
    "Hogar":       ["Silla Ergonómica", "Lámpara LED", "Cafetera Espresso", "Aspiradora Robot", "Set Ollas"],
    "Deportes":    ["Bicicleta MTB", "Pesas 10kg", "Colchoneta Yoga", "Raqueta Tenis", "Cuerda Saltar"],
    "Libros":      ["Python Avanzado", "Data Science 101", "El Arte de la IA", "SQL Experto", "Spark en Acción"],
    "Juguetes":    ["LEGO Ciudad", "Muñeca Articulada", "Drone Mini", "Puzzle 1000pcs", "Auto RC"]
}
etiquetas_pool = ["oferta", "nuevo", "popular", "premium", "eco", "limitado", "recomendado", "outlet", "exclusivo"]
paises = ["México", "Colombia", "Argentina", "Chile", "Perú", "España", "Uruguay"]
formatos_fecha = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%dT%H:%M:%SZ"]

def gen_fecha(base_days_back=365):
    base = datetime(2023, 1, 1)
    delta = timedelta(days=random.randint(0, base_days_back),
                      hours=random.randint(0, 23),
                      minutes=random.randint(0, 59))
    dt = base + delta
    fmt = random.choice(formatos_fecha)
    return dt.strftime(fmt)

def gen_atributos(categoria):
    attrs = {"color": random.choice(["rojo", "azul", "negro", "blanco", "verde"]),
             "garantia_meses": str(random.choice([6, 12, 24, 36]))}
    if categoria == "Electrónica":
        attrs["voltaje"] = random.choice(["110V", "220V", "dual"])
    return attrs

rows = []
for i in range(1, 120_001):
    cat = random.choice(categorias)
    prod_list = productos_por_categoria[cat]
    producto = random.choice(prod_list)
    precio = round(random.uniform(9.99, 2999.99), 2)
    cantidad = random.randint(1, 10)
    rating = round(random.uniform(1.0, 5.0), 1)
    n_etiquetas = random.randint(1, 4)
    etiquetas = random.sample(etiquetas_pool, n_etiquetas)
    atributos = gen_atributos(cat)
    fecha_str = gen_fecha()
    pais = random.choice(paises)
    cliente_id = f"CLI-{random.randint(1000, 9999)}"
    descripcion = f"Producto {producto} de categoría {cat}. Disponible en {pais}. Rating: {rating}."
    rows.append((i, cliente_id, producto, cat, precio, cantidad,
                 rating, etiquetas, atributos, fecha_str, pais, descripcion))

schema = StructType([
    StructField("order_id",     IntegerType(), False),
    StructField("cliente_id",   StringType(),  False),
    StructField("producto",     StringType(),  False),
    StructField("categoria",    StringType(),  False),
    StructField("precio",       DoubleType(),  False),
    StructField("cantidad",     IntegerType(), False),
    StructField("rating",       DoubleType(),  False),
    StructField("etiquetas",    ArrayType(StringType()), True),
    StructField("atributos",    MapType(StringType(), StringType()), True),
    StructField("fecha_str",    StringType(),  False),
    StructField("pais",         StringType(),  False),
    StructField("descripcion",  StringType(),  False),
])

df_raw = spark.createDataFrame(rows, schema=schema)
df_raw.cache()
print(f"Dataset generado: {df_raw.count():,} filas, {len(df_raw.columns)} columnas")
df_raw.printSchema()
df_raw.show(3, truncate=60)
```

#### Salida esperada

```
Dataset generado: 120,000 filas, 12 columnas

root
 |-- order_id: integer (nullable = false)
 |-- cliente_id: string (nullable = false)
 |-- producto: string (nullable = false)
 |-- categoria: string (nullable = false)
 |-- precio: double (nullable = false)
 |-- cantidad: integer (nullable = false)
 |-- rating: double (nullable = false)
 |-- etiquetas: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- atributos: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- fecha_str: string (nullable = false)
 |-- pais: string (nullable = false)
 |-- descripcion: string (nullable = false)
```

#### Verificación

```python
# Verificar distribución de categorías
df_raw.groupBy("categoria").count().orderBy(F.desc("count")).show()
# Verificar que hay fechas en múltiples formatos
df_raw.select("fecha_str").sample(0.001).show(10, truncate=False)
```

---

### Paso 2: Funciones de Fecha y Hora — Análisis de Estacionalidad

**Objetivo:** Parsear fechas en múltiples formatos, estandarizarlas a `TimestampType` y extraer componentes temporales para analizar patrones de compra por mes, día de la semana y hora del día.

#### Instrucciones

1. Crea la celda `# Paso 2 — Funciones de fecha y hora`.

2. Implementa el parseo multi-formato y la extracción de componentes:

```python
# Paso 2 — Funciones de fecha y hora

# 2a. Parseo de fechas en múltiples formatos
# Spark intentará cada formato en orden; el primero que haga match gana
df_fechas = df_raw.withColumn(
    "timestamp_orden",
    F.coalesce(
        F.to_timestamp(F.col("fecha_str"), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col("fecha_str"), "dd/MM/yyyy HH:mm"),
        F.to_timestamp(F.col("fecha_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
)

# 2b. Extraer componentes temporales
df_fechas = df_fechas.withColumn("anio",         F.year("timestamp_orden")) \
    .withColumn("mes",          F.month("timestamp_orden")) \
    .withColumn("dia_semana",   F.dayofweek("timestamp_orden")) \
    .withColumn("hora",         F.hour("timestamp_orden")) \
    .withColumn("nombre_mes",   F.date_format("timestamp_orden", "MMMM")) \
    .withColumn("semana_anio",  F.weekofyear("timestamp_orden")) \
    .withColumn("trimestre",    F.quarter("timestamp_orden")) \
    .withColumn("inicio_mes",   F.trunc("timestamp_orden", "MM")) \
    .withColumn("fin_mes",      F.last_day(F.to_date("timestamp_orden"))) \
    .withColumn("prox_lunes",   F.next_day(F.to_date("timestamp_orden"), "Monday")) \
    .withColumn("unix_ts",      F.unix_timestamp("timestamp_orden")) \
    .withColumn("fecha_trunc_hora", F.date_trunc("hour", "timestamp_orden"))

# 2c. Aritmética de fechas
df_fechas = df_fechas \
    .withColumn("fecha_entrega_est",
                F.date_add(F.to_date("timestamp_orden"), 5)) \
    .withColumn("dias_desde_orden",
                F.datediff(F.current_date(), F.to_date("timestamp_orden")))

# 2d. Verificar nulos en el parseo (indicaría un formato no contemplado)
nulos_fecha = df_fechas.filter(F.col("timestamp_orden").isNull()).count()
print(f"Registros con fecha no parseada: {nulos_fecha}")

# 2e. Análisis de estacionalidad — ventas por mes y día de semana
print("\n=== Ventas por Trimestre ===")
df_fechas.groupBy("trimestre") \
    .agg(
        F.count("order_id").alias("num_ordenes"),
        F.round(F.sum(F.col("precio") * F.col("cantidad")), 2).alias("ingresos_total")
    ) \
    .orderBy("trimestre") \
    .show()

print("\n=== Horas pico de compra ===")
df_fechas.groupBy("hora") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(5) \
    .show()

# 2f. Reconstrucción desde Unix timestamp
print("\n=== Verificación Unix timestamp → fecha ===")
df_fechas.select(
    "order_id",
    "unix_ts",
    F.from_unixtime(F.col("unix_ts"), "yyyy-MM-dd HH:mm:ss").alias("reconstruida")
).show(3)
```

#### Salida esperada

```
Registros con fecha no parseada: 0

=== Ventas por Trimestre ===
+----------+-----------+--------------+
|trimestre |num_ordenes|ingresos_total|
+----------+-----------+--------------+
|1         |~30000     |~45,000,000   |
|2         |~30000     |~45,000,000   |
|3         |~30000     |~45,000,000   |
|4         |~30000     |~45,000,000   |
+----------+-----------+--------------+
```

#### Verificación

```python
# Confirmar que timestamp_orden es de tipo Timestamp
assert dict(df_fechas.dtypes)["timestamp_orden"] == "timestamp", \
    "ERROR: La columna timestamp_orden debe ser de tipo timestamp"
print("✓ Columna timestamp_orden correctamente tipada")

# Confirmar que no hay nulos
assert df_fechas.filter(F.col("timestamp_orden").isNull()).count() == 0, \
    "ERROR: Existen fechas sin parsear"
print("✓ Todas las fechas parseadas correctamente")
```

---

### Paso 3: Funciones de Texto Avanzadas

**Objetivo:** Aplicar funciones de texto para limpieza, similitud fonética, distancia de edición y extracción de patrones en la columna de descripción.

#### Instrucciones

1. Crea la celda `# Paso 3 — Funciones de texto avanzadas`.

2. Implementa las transformaciones de texto:

```python
# Paso 3 — Funciones de texto avanzadas

# 3a. Limpieza y normalización básica
df_texto = df_fechas.withColumn(
    "producto_limpio",
    F.initcap(F.trim(F.lower(F.col("producto"))))
).withColumn(
    "categoria_upper",
    F.upper(F.col("categoria"))
)

# 3b. format_string — crear etiqueta de producto formateada
df_texto = df_texto.withColumn(
    "etiqueta_producto",
    F.format_string("[%s] %s — $%.2f", F.col("categoria"), F.col("producto"), F.col("precio"))
)

# 3c. lpad/rpad — formatear order_id con ceros a la izquierda
df_texto = df_texto.withColumn(
    "order_id_fmt",
    F.lpad(F.col("order_id").cast(StringType()), 8, "0")
)

# 3d. translate — reemplazar caracteres especiales (útil para normalizar acentos básicos)
df_texto = df_texto.withColumn(
    "producto_ascii",
    F.translate(F.col("producto"), "áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN")
)

# 3e. soundex — codificación fonética para búsqueda aproximada
df_texto = df_texto.withColumn(
    "soundex_producto",
    F.soundex(F.col("producto"))
)

# 3f. levenshtein — distancia de edición entre producto y una referencia
df_texto = df_texto.withColumn(
    "distancia_laptop",
    F.levenshtein(F.lower(F.col("producto")), F.lit("laptop pro 15"))
)

# 3g. sentences — tokenizar descripción en oraciones y palabras
df_texto = df_texto.withColumn(
    "oraciones",
    F.sentences(F.col("descripcion"))
)

# 3h. regexp_extract_all — extraer todos los números del texto de descripción
df_texto = df_texto.withColumn(
    "numeros_descripcion",
    F.regexp_extract_all(F.col("descripcion"), F.lit(r"\d+\.?\d*"), 0)
)

# Mostrar resultados seleccionados
print("=== Funciones de texto aplicadas ===")
df_texto.select(
    "order_id_fmt",
    "etiqueta_producto",
    "producto_ascii",
    "soundex_producto",
    "distancia_laptop"
).show(5, truncate=60)

print("\n=== Tokenización con sentences() ===")
df_texto.select("descripcion", "oraciones") \
    .limit(2).show(truncate=80)

print("\n=== Productos más similares a 'laptop pro 15' (Levenshtein) ===")
df_texto.select("producto", "distancia_laptop") \
    .dropDuplicates(["producto"]) \
    .orderBy("distancia_laptop") \
    .limit(8) \
    .show()
```

#### Salida esperada

```
=== Productos más similares a 'laptop pro 15' (Levenshtein) ===
+------------------+----------------+
|producto          |distancia_laptop|
+------------------+----------------+
|Laptop Pro 15     |0               |
|Tablet X10        |9               |
|Python Avanzado   |11              |
...
```

#### Verificación

```python
# Verificar que soundex devuelve código de 4 caracteres
sample_soundex = df_texto.select("soundex_producto").first()[0]
assert len(sample_soundex) == 4, "ERROR: Soundex debe retornar 4 caracteres"
print(f"✓ Soundex correcto: ejemplo = {sample_soundex}")

# Verificar que levenshtein = 0 para producto exacto
dist_exacta = df_texto.filter(F.lower(F.col("producto")) == "laptop pro 15") \
    .select("distancia_laptop").first()[0]
assert dist_exacta == 0, "ERROR: Distancia para coincidencia exacta debe ser 0"
print("✓ Levenshtein correcto: distancia 0 para coincidencia exacta")
```

---

### Paso 4: Manipulación de Colecciones (Arrays y Maps)

**Objetivo:** Aplicar funciones nativas de PySpark para transformar, filtrar y combinar arrays y maps presentes en el dataset.

#### Instrucciones

1. Crea la celda `# Paso 4 — Funciones de colección`.

2. Implementa las operaciones sobre arrays y maps:

```python
# Paso 4 — Funciones de colección: arrays y maps

# 4a. Inspección básica de colecciones
print("=== Muestra de etiquetas (array) y atributos (map) ===")
df_texto.select("order_id", "etiquetas", "atributos").show(3, truncate=False)

# 4b. map_keys y map_values — extraer claves y valores del map de atributos
df_col = df_texto.withColumn("claves_atributos",  F.map_keys("atributos")) \
                 .withColumn("valores_atributos", F.map_values("atributos"))

# 4c. Acceso a valores específicos del map
df_col = df_col.withColumn("color_producto",     F.col("atributos")["color"]) \
               .withColumn("garantia_meses",     F.col("atributos")["garantia_meses"].cast(IntegerType()))

# 4d. transform — aplicar función a cada elemento del array (mayúsculas)
df_col = df_col.withColumn(
    "etiquetas_upper",
    F.transform("etiquetas", lambda x: F.upper(x))
)

# 4e. filter sobre array — conservar solo etiquetas con más de 5 caracteres
df_col = df_col.withColumn(
    "etiquetas_largas",
    F.filter("etiquetas", lambda x: F.length(x) > 5)
)

# 4f. aggregate — concatenar todas las etiquetas en un string
df_col = df_col.withColumn(
    "etiquetas_concat",
    F.aggregate(
        "etiquetas",
        F.lit(""),
        lambda acc, x: F.when(acc == "", x).otherwise(F.concat(acc, F.lit("|"), x))
    )
)

# 4g. array_contains — verificar si el array contiene "oferta"
df_col = df_col.withColumn(
    "tiene_oferta",
    F.array_contains("etiquetas", "oferta")
)

# 4h. size del array
df_col = df_col.withColumn("num_etiquetas", F.size("etiquetas"))

# 4i. zip_with — combinar dos arrays elemento a elemento (demo con array sintético)
df_col = df_col.withColumn(
    "etiquetas_numeradas",
    F.zip_with(
        "etiquetas",
        F.sequence(F.lit(1), F.size("etiquetas")),
        lambda e, n: F.concat(n.cast(StringType()), F.lit(". "), e)
    )
)

# 4j. explode + análisis de frecuencia de etiquetas
print("\n=== Top 10 etiquetas más frecuentes ===")
df_texto.select(F.explode("etiquetas").alias("etiqueta")) \
    .groupBy("etiqueta") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(10)

# 4k. Análisis de colores más populares por categoría
print("\n=== Color más común por categoría ===")
df_col.groupBy("categoria", "color_producto") \
    .count() \
    .withColumn("rk", F.row_number().over(
        Window.partitionBy("categoria").orderBy(F.desc("count"))
    )) \
    .filter(F.col("rk") == 1) \
    .select("categoria", "color_producto", "count") \
    .orderBy("categoria") \
    .show()

# Mostrar resultado final del paso
df_col.select("order_id", "etiquetas_upper", "etiquetas_largas",
              "etiquetas_concat", "tiene_oferta", "num_etiquetas").show(4, truncate=60)
```

#### Salida esperada

```
=== Top 10 etiquetas más frecuentes ===
+------------+------+
|etiqueta    |count |
+------------+------+
|oferta      |~26000|
|popular     |~26000|
|nuevo       |~26000|
...
```

#### Verificación

```python
# Verificar que transform produjo etiquetas en mayúsculas
sample = df_col.select("etiquetas", "etiquetas_upper").first()
original = sample["etiquetas"][0]
transformada = sample["etiquetas_upper"][0]
assert transformada == original.upper(), \
    f"ERROR: transform no aplicó upper. Original: {original}, Transformada: {transformada}"
print(f"✓ transform correcto: '{original}' → '{transformada}'")

# Verificar num_etiquetas entre 1 y 4 (según generación)
stats = df_col.agg(F.min("num_etiquetas"), F.max("num_etiquetas")).first()
assert 1 <= stats[0] and stats[1] <= 4, "ERROR: num_etiquetas fuera del rango esperado [1, 4]"
print(f"✓ num_etiquetas en rango correcto: min={stats[0]}, max={stats[1]}")
```

---

### Paso 5: Window Functions — Análisis Analítico Avanzado

**Objetivo:** Implementar funciones de ventana para calcular rankings, comparaciones temporales (lag/lead) y agregaciones móviles sobre particiones de datos.

#### Instrucciones

1. Crea la celda `# Paso 5 — Window Functions`.

2. Implementa las Window Functions:

```python
# Paso 5 — Window Functions

# Preparar DataFrame base para análisis de ventana
df_ventas = df_col.select(
    "order_id", "cliente_id", "categoria", "producto",
    "precio", "cantidad", "rating",
    F.to_date("timestamp_orden").alias("fecha"),
    "pais"
).withColumn("total_venta", F.round(F.col("precio") * F.col("cantidad"), 2))

# ── 5a. Ranking de productos por categoría ──────────────────────────────────
w_cat_precio = Window.partitionBy("categoria").orderBy(F.desc("total_venta"))

df_ranking = df_ventas.withColumn("row_number_cat",  F.row_number().over(w_cat_precio)) \
                      .withColumn("rank_cat",         F.rank().over(w_cat_precio)) \
                      .withColumn("dense_rank_cat",   F.dense_rank().over(w_cat_precio)) \
                      .withColumn("ntile_4",          F.ntile(4).over(w_cat_precio)) \
                      .withColumn("cume_dist_cat",    F.round(F.cume_dist().over(w_cat_precio), 4))

print("=== Top 3 ventas por categoría (row_number, rank, dense_rank) ===")
df_ranking.filter(F.col("row_number_cat") <= 3) \
    .select("categoria", "producto", "total_venta",
            "row_number_cat", "rank_cat", "dense_rank_cat") \
    .orderBy("categoria", "row_number_cat") \
    .show(20, truncate=40)

# ── 5b. Lag y Lead — comparación con venta anterior y siguiente por cliente ──
w_cliente_fecha = Window.partitionBy("cliente_id").orderBy("fecha", "order_id")

df_lag_lead = df_ventas.withColumn(
    "venta_anterior",
    F.lag("total_venta", 1).over(w_cliente_fecha)
).withColumn(
    "venta_siguiente",
    F.lead("total_venta", 1).over(w_cliente_fecha)
).withColumn(
    "diff_anterior",
    F.round(F.col("total_venta") - F.col("venta_anterior"), 2)
).withColumn(
    "tendencia",
    F.when(F.col("diff_anterior") > 0, "↑ sube")
     .when(F.col("diff_anterior") < 0, "↓ baja")
     .when(F.col("diff_anterior") == 0, "= igual")
     .otherwise("primera compra")
)

print("\n=== Lag/Lead: tendencia de compra por cliente ===")
df_lag_lead.select(
    "cliente_id", "fecha", "total_venta",
    "venta_anterior", "diff_anterior", "tendencia"
).filter(F.col("venta_anterior").isNotNull()) \
 .orderBy("cliente_id", "fecha") \
 .show(10, truncate=30)

# ── 5c. Agregaciones móviles — suma y promedio deslizante ───────────────────
# Agregación por fecha para tener una serie temporal diaria
df_diario = df_ventas.groupBy("fecha") \
    .agg(F.sum("total_venta").alias("ingresos_dia")) \
    .orderBy("fecha")

w_rolling_7 = Window.orderBy("fecha").rowsBetween(-6, 0)   # últimas 7 filas
w_rolling_30 = Window.orderBy("fecha").rowsBetween(-29, 0) # últimas 30 filas

df_rolling = df_diario \
    .withColumn("media_movil_7d",  F.round(F.avg("ingresos_dia").over(w_rolling_7), 2)) \
    .withColumn("media_movil_30d", F.round(F.avg("ingresos_dia").over(w_rolling_30), 2)) \
    .withColumn("suma_acum_7d",    F.round(F.sum("ingresos_dia").over(w_rolling_7), 2))

print("\n=== Media móvil 7 y 30 días (primeras 10 fechas) ===")
df_rolling.show(10)

# ── 5d. rangeBetween — ventana basada en valor (no en filas) ────────────────
w_range = Window.partitionBy("categoria") \
    .orderBy("total_venta") \
    .rangeBetween(-500, 500)  # ventas dentro de ±$500

df_range = df_ventas.withColumn(
    "ventas_similares_count",
    F.count("order_id").over(w_range)
)

print("\n=== Órdenes con ventas similares (±$500) por categoría — muestra ===")
df_range.select("categoria", "producto", "total_venta", "ventas_similares_count") \
    .orderBy("categoria", "total_venta") \
    .show(8, truncate=40)
```

#### Salida esperada

```
=== Top 3 ventas por categoría (row_number, rank, dense_rank) ===
+------------+------------------+-----------+--------------+--------+---------------+
|categoria   |producto          |total_venta|row_number_cat|rank_cat|dense_rank_cat |
+------------+------------------+-----------+--------------+--------+---------------+
|Deportes    |Bicicleta MTB     |29990.01   |1             |1       |1              |
...
```

#### Verificación

```python
# Verificar que row_number no tiene empates (siempre único por partición-posición)
dup_check = df_ranking.groupBy("categoria", "row_number_cat").count() \
    .filter(F.col("count") > 1).count()
assert dup_check == 0, "ERROR: row_number tiene duplicados — no debería ocurrir"
print("✓ row_number sin duplicados por categoría")

# Verificar que lag produce nulos en la primera fila de cada cliente
primer_compra_nulos = df_lag_lead \
    .withColumn("rn", F.row_number().over(w_cliente_fecha)) \
    .filter((F.col("rn") == 1) & F.col("venta_anterior").isNotNull()) \
    .count()
assert primer_compra_nulos == 0, "ERROR: La primera compra de cada cliente debe tener venta_anterior = null"
print("✓ lag correcto: primera compra de cada cliente tiene venta_anterior = null")
```

---

### Paso 6: User Defined Functions (UDFs) — Escalares y Vectorizadas

**Objetivo:** Crear UDFs en Python con el decorador `@udf`, registrarlas en el catálogo SQL y compararlas con `@pandas_udf` (Vectorized UDFs) en términos de rendimiento.

#### Instrucciones

1. Crea la celda `# Paso 6 — UDFs escalares y vectorizadas`.

2. Implementa y compara los dos tipos de UDFs:

```python
# Paso 6 — UDFs escalares y pandas_udf (vectorizadas)
import time
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

# ── 6a. UDF escalar con @udf decorator ──────────────────────────────────────
@udf(returnType=StringType())
def clasificar_precio(precio: float) -> str:
    """Clasifica el precio en segmentos de negocio."""
    if precio is None:
        return "desconocido"
    if precio < 50:
        return "económico"
    elif precio < 300:
        return "accesible"
    elif precio < 1000:
        return "premium"
    else:
        return "lujo"

@udf(returnType=DoubleType())
def calcular_descuento(precio: float, rating: float) -> float:
    """Calcula descuento sugerido basado en el rating del producto."""
    if precio is None or rating is None:
        return 0.0
    if rating >= 4.5:
        return round(precio * 0.05, 2)   # 5% descuento productos excelentes
    elif rating >= 3.5:
        return round(precio * 0.10, 2)   # 10% descuento productos buenos
    else:
        return round(precio * 0.20, 2)   # 20% descuento productos con bajo rating

# Aplicar UDF escalar
t0 = time.time()
df_udf = df_ventas.withColumn("segmento_precio", clasificar_precio("precio")) \
                  .withColumn("descuento_sugerido", calcular_descuento("precio", "rating"))
df_udf.count()  # forzar ejecución
t_udf = time.time() - t0
print(f"UDF escalar ejecutada en: {t_udf:.2f}s")

df_udf.select("producto", "precio", "rating", "segmento_precio", "descuento_sugerido") \
    .show(5, truncate=40)

# ── 6b. Registrar UDF en el catálogo SQL ────────────────────────────────────
spark.udf.register("clasificar_precio_sql", clasificar_precio)
spark.udf.register("calcular_descuento_sql", calcular_descuento)

df_ventas.createOrReplaceTempView("ventas_view")

print("\n=== Uso de UDF desde Spark SQL ===")
spark.sql("""
    SELECT
        producto,
        precio,
        rating,
        clasificar_precio_sql(precio)         AS segmento,
        calcular_descuento_sql(precio, rating) AS descuento
    FROM ventas_view
    LIMIT 5
""").show(truncate=40)

# ── 6c. pandas_udf (Vectorized UDF) — misma lógica, mayor rendimiento ───────
@pandas_udf(StringType())
def clasificar_precio_pandas(precio_series: pd.Series) -> pd.Series:
    """Versión vectorizada de clasificar_precio usando pandas."""
    def _clasif(p):
        if pd.isna(p):    return "desconocido"
        if p < 50:        return "económico"
        elif p < 300:     return "accesible"
        elif p < 1000:    return "premium"
        else:             return "lujo"
    return precio_series.apply(_clasif)

@pandas_udf(DoubleType())
def calcular_descuento_pandas(precio_series: pd.Series, rating_series: pd.Series) -> pd.Series:
    """Versión vectorizada de calcular_descuento."""
    def _desc(row):
        p, r = row["precio"], row["rating"]
        if pd.isna(p) or pd.isna(r): return 0.0
        if r >= 4.5:   return round(p * 0.05, 2)
        elif r >= 3.5: return round(p * 0.10, 2)
        else:          return round(p * 0.20, 2)
    return pd.DataFrame({"precio": precio_series, "rating": rating_series}).apply(_desc, axis=1)

# Aplicar pandas_udf
t0 = time.time()
df_pudf = df_ventas.withColumn("segmento_precio_v", clasificar_precio_pandas("precio")) \
                   .withColumn("descuento_v", calcular_descuento_pandas("precio", "rating"))
df_pudf.count()
t_pudf = time.time() - t0
print(f"\npandas_udf ejecutada en: {t_pudf:.2f}s")

# ── 6d. Comparación de rendimiento ──────────────────────────────────────────
print(f"\n{'='*50}")
print(f"  Comparación de rendimiento (120,000 filas)")
print(f"{'='*50}")
print(f"  UDF escalar (@udf):      {t_udf:.2f}s")
print(f"  pandas_udf vectorizada:  {t_pudf:.2f}s")
mejora = ((t_udf - t_pudf) / t_udf * 100) if t_udf > t_pudf else 0
print(f"  Mejora pandas_udf:       {mejora:.1f}%")
print(f"{'='*50}")
print("""
Conclusión:
- @udf: serializa fila a fila entre JVM y Python (lento para grandes volúmenes).
- @pandas_udf: transfiere batches de datos usando Apache Arrow (mucho más eficiente).
- Siempre preferir pandas_udf cuando la lógica de negocio requiere UDF.
- Mejor aún: usar funciones nativas de pyspark.sql.functions cuando sea posible.
""")

# ── 6e. Verificar consistencia entre UDF escalar y pandas_udf ───────────────
diff_count = df_ventas \
    .withColumn("seg_escalar", clasificar_precio("precio")) \
    .withColumn("seg_pandas",  clasificar_precio_pandas("precio")) \
    .filter(F.col("seg_escalar") != F.col("seg_pandas")) \
    .count()
print(f"Diferencias entre UDF escalar y pandas_udf: {diff_count} (esperado: 0)")
```

#### Salida esperada

```
UDF escalar ejecutada en: ~8-15s
pandas_udf ejecutada en:  ~3-7s

==================================================
  Comparación de rendimiento (120,000 filas)
==================================================
  UDF escalar (@udf):      ~10.5s
  pandas_udf vectorizada:  ~4.2s
  Mejora pandas_udf:       ~60%
==================================================
```

> **Nota:** Los tiempos variarán según el hardware. En máquinas con 8 GB RAM, la diferencia puede ser menos pronunciada. Lo importante es observar la tendencia.

#### Verificación

```python
assert diff_count == 0, "ERROR: Las dos implementaciones de UDF producen resultados diferentes"
print("✓ UDF escalar y pandas_udf producen resultados idénticos")

# Verificar que la UDF está registrada en el catálogo SQL
udfs_registradas = [f.name for f in spark.catalog.listFunctions()
                    if "clasificar_precio" in f.name.lower()]
assert len(udfs_registradas) > 0, "ERROR: UDF no encontrada en el catálogo SQL"
print(f"✓ UDF registrada en catálogo SQL: {udfs_registradas}")
```

---

### Paso 7: Catalyst Optimizer — Análisis de Planes de Ejecución

**Objetivo:** Usar `explain()` con diferentes modos para interpretar los planes de ejecución del Catalyst Optimizer y aplicar técnicas que generen planes más eficientes (predicate pushdown, column pruning).

#### Instrucciones

1. Crea la celda `# Paso 7 — Catalyst Optimizer`.

2. Analiza e interpreta los planes de ejecución:

```python
# Paso 7 — Catalyst Optimizer: análisis de planes de ejecución

# ── 7a. Construir una consulta analítica compleja ────────────────────────────
df_analisis = df_col.select(
    "order_id", "cliente_id", "categoria", "producto",
    "precio", "cantidad", "rating", "etiquetas",
    F.to_date("timestamp_orden").alias("fecha"),
    "pais", "color_producto"
).withColumn("total_venta", F.round(F.col("precio") * F.col("cantidad"), 2))

# Consulta con filtro, proyección y agregación
df_query = df_analisis \
    .filter(F.col("categoria").isin(["Electrónica", "Deportes"])) \
    .filter(F.col("total_venta") > 100) \
    .groupBy("categoria", "pais") \
    .agg(
        F.count("order_id").alias("ordenes"),
        F.round(F.avg("total_venta"), 2).alias("ticket_promedio"),
        F.round(F.sum("total_venta"), 2).alias("ingresos")
    ) \
    .orderBy(F.desc("ingresos"))

# ── 7b. explain() en modo simple (por defecto) ──────────────────────────────
print("=" * 70)
print("PLAN FÍSICO SIMPLE (explain())")
print("=" * 70)
df_query.explain()

# ── 7c. explain() en modo extended — muestra los 4 planes ───────────────────
print("\n" + "=" * 70)
print("PLAN EXTENDIDO (explain('extended')) — Parsed + Analyzed + Optimized + Physical")
print("=" * 70)
df_query.explain("extended")

# ── 7d. explain() en modo formatted — más legible ───────────────────────────
print("\n" + "=" * 70)
print("PLAN FORMATEADO (explain('formatted'))")
print("=" * 70)
df_query.explain("formatted")

# ── 7e. Demostración de Predicate Pushdown ───────────────────────────────────
print("\n" + "=" * 70)
print("DEMOSTRACIÓN: Predicate Pushdown")
print("=" * 70)

# Sin pushdown explícito — filtro después de join
df_sin_pushdown = df_analisis.join(
    df_analisis.select("order_id", "total_venta").alias("b"),
    "order_id"
).filter(F.col("categoria") == "Electrónica")

# Con pushdown explícito — filtrar ANTES del join
df_con_pushdown = df_analisis \
    .filter(F.col("categoria") == "Electrónica") \
    .join(
        df_analisis.filter(F.col("categoria") == "Electrónica")
                   .select("order_id", "total_venta").alias("b"),
        "order_id"
    )

print("\n--- Plan SIN pushdown manual ---")
df_sin_pushdown.explain("formatted")

print("\n--- Plan CON pushdown manual ---")
df_con_pushdown.explain("formatted")

# ── 7f. Demostración de Column Pruning ──────────────────────────────────────
print("\n" + "=" * 70)
print("DEMOSTRACIÓN: Column Pruning")
print("=" * 70)

# Seleccionar solo las columnas necesarias — Catalyst elimina las demás
df_pruned = df_analisis \
    .select("categoria", "total_venta", "pais") \
    .filter(F.col("categoria") == "Libros") \
    .groupBy("pais") \
    .agg(F.sum("total_venta").alias("ingresos"))

print("Plan con Column Pruning (solo 3 columnas seleccionadas):")
df_pruned.explain("formatted")

# ── 7g. Interpretación guiada de los planes ─────────────────────────────────
print("""
╔══════════════════════════════════════════════════════════════════════╗
║           GUÍA DE INTERPRETACIÓN DEL CATALYST OPTIMIZER             ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Etapa 1 — PARSED LOGICAL PLAN                                       ║
║    • Árbol sintáctico generado desde el código Python/SQL.           ║
║    • Aún no resuelve nombres de columnas ni tipos.                   ║
║                                                                      ║
║  Etapa 2 — ANALYZED LOGICAL PLAN                                     ║
║    • Resuelve referencias a columnas usando el catálogo de metadatos.║
║    • Infiere tipos de datos y valida la consulta.                    ║
║                                                                      ║
║  Etapa 3 — OPTIMIZED LOGICAL PLAN                                    ║
║    • Aplica reglas de optimización: predicate pushdown,              ║
║      column pruning, constant folding, etc.                          ║
║    • Este es el plan "lógico" final antes de la ejecución física.    ║
║                                                                      ║
║  Etapa 4 — PHYSICAL PLAN                                             ║
║    • Traduce el plan lógico a operaciones físicas concretas:         ║
║      BroadcastHashJoin, SortMergeJoin, HashAggregate, etc.           ║
║    • Aquí se decide qué algoritmo se usa para cada operación.        ║
║                                                                      ║
║  Indicadores clave a buscar en el Physical Plan:                     ║
║    • "Filter" cerca del origen de datos → Predicate Pushdown ✓      ║
║    • Pocas columnas en "Project" → Column Pruning ✓                 ║
║    • "BroadcastHashJoin" en lugar de "SortMergeJoin" → más rápido   ║
║    • "Exchange" → shuffle de datos entre particiones (costoso)       ║
╚══════════════════════════════════════════════════════════════════════╝
""")
```

3. Ejecuta la consulta y observa los planes en la salida:

```python
# 7h. Ejecutar la consulta y mostrar resultados finales
print("=== Resultados de la consulta analítica ===")
df_query.show(20)

# 7i. Verificar optimizaciones activas en la sesión
print("\n=== Configuraciones de optimización activas ===")
configs_opt = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.shuffle.partitions"
]
for cfg in configs_opt:
    val = spark.conf.get(cfg, "no configurado")
    print(f"  {cfg}: {val}")
```

#### Salida esperada

En el plan físico deberías identificar:
- `Filter` aplicado temprano (predicate pushdown automático).
- `Project` con solo las columnas necesarias (column pruning).
- `HashAggregate` para la agregación eficiente.
- Posiblemente `Exchange hashpartitioning` para el `groupBy`.

#### Verificación

```python
# Verificar que la consulta produce resultados para las categorías filtradas
result = df_query.collect()
categorias_result = set(row["categoria"] for row in result)
assert categorias_result == {"Electrónica", "Deportes"}, \
    f"ERROR: Categorías inesperadas en resultado: {categorias_result}"
print(f"✓ Predicate pushdown correcto: solo categorías filtradas en resultado: {categorias_result}")

# Verificar que todos los totales son > 0
assert all(row["ingresos"] > 0 for row in result), \
    "ERROR: Existen ingresos <= 0 en el resultado"
print("✓ Todos los ingresos son positivos")
```

---

## Validación y Pruebas Finales

Ejecuta la siguiente celda de validación integral al finalizar todos los pasos:

```python
# ══════════════════════════════════════════════════════════════════════
# VALIDACIÓN INTEGRAL — Práctica 9
# ══════════════════════════════════════════════════════════════════════
print("╔══════════════════════════════════════════════════════╗")
print("║        VALIDACIÓN INTEGRAL — PRÁCTICA 9             ║")
print("╚══════════════════════════════════════════════════════╝\n")

resultados = []

# ── Paso 1: Dataset ──────────────────────────────────────────────────
try:
    assert df_raw.count() == 120_000
    assert len(df_raw.columns) == 12
    resultados.append(("Paso 1 — Dataset generado",           "✓ PASS"))
except AssertionError as e:
    resultados.append(("Paso 1 — Dataset generado",           f"✗ FAIL: {e}"))

# ── Paso 2: Fechas ───────────────────────────────────────────────────
try:
    assert dict(df_fechas.dtypes)["timestamp_orden"] == "timestamp"
    assert df_fechas.filter(F.col("timestamp_orden").isNull()).count() == 0
    assert "trimestre" in df_fechas.columns
    assert "dias_desde_orden" in df_fechas.columns
    resultados.append(("Paso 2 — Funciones de fecha",          "✓ PASS"))
except AssertionError as e:
    resultados.append(("Paso 2 — Funciones de fecha",          f"✗ FAIL: {e}"))

# ── Paso 3: Texto ────────────────────────────────────────────────────
try:
    assert "soundex_producto" in df_texto.columns
    assert "distancia_laptop" in df_texto.columns
    assert "etiqueta_producto" in df_texto.columns
    s = df_texto.select("soundex_producto").first()[0]
    assert len(s) == 4
    resultados.append(("Paso 3 — Funciones de texto",          "✓ PASS"))
except AssertionError as e:
    resultados.append(("Paso 3 — Funciones de texto",          f"✗ FAIL: {e}"))

# ── Paso 4: Colecciones ──────────────────────────────────────────────
try:
    assert "etiquetas_upper" in df_col.columns
    assert "etiquetas_largas" in df_col.columns
    assert "color_producto" in df_col.columns
    assert "tiene_oferta" in df_col.columns
    sample = df_col.select("etiquetas", "etiquetas_upper").first()
    assert sample["etiquetas_upper"][0] == sample["etiquetas"][0].upper()
    resultados.append(("Paso 4 — Funciones de colección",      "✓ PASS"))
except AssertionError as e:
    resultados.append(("Paso 4 — Funciones de colección",      f"✗ FAIL: {e}"))

# ── Paso 5: Window Functions ─────────────────────────────────────────
try:
    dup = df_ranking.groupBy("categoria", "row_number_cat").count() \
        .filter(F.col("count") > 1).count()
    assert dup == 0
    assert "media_movil_7d" in df_rolling.columns
    assert "media_movil_30d" in df_rolling.columns
    resultados.append(("Paso 5 — Window Functions",            "✓ PASS"))
except AssertionError as e:
    resultados.append(("Paso 5 — Window Functions",            f"✗ FAIL: {e}"))

# ── Paso 6: UDFs ─────────────────────────────────────────────────────
try:
    diff = df_ventas \
        .withColumn("s1", clasificar_precio("precio")) \
        .withColumn("s2", clasificar_precio_pandas("precio")) \
        .filter(F.col("s1") != F.col("s2")).count()
    assert diff == 0
    resultados.append(("Paso 6 — UDFs escalar y pandas_udf",  "✓ PASS"))
except AssertionError as e:
    resultados.append(("Paso 6 — UDFs escalar y pandas_udf",  f"✗ FAIL: {e}"))

# ── Paso 7: Catalyst Optimizer ───────────────────────────────────────
try:
    r = df_query.collect()
    cats = set(row["categoria"] for row in r)
    assert cats == {"Electrónica", "Deportes"}
    assert all(row["ingresos"] > 0 for row in r)
    resultados.append(("Paso 7 — Catalyst Optimizer",          "✓ PASS"))
except AssertionError as e:
    resultados.append(("Paso 7 — Catalyst Optimizer",          f"✗ FAIL: {e}"))

# ── Reporte final ────────────────────────────────────────────────────
print(f"\n{'─'*60}")
print(f"{'COMPONENTE':<40} {'ESTADO':<20}")
print(f"{'─'*60}")
for nombre, estado in resultados:
    print(f"{nombre:<40} {estado:<20}")
print(f"{'─'*60}")

total = len(resultados)
aprobados = sum(1 for _, e in resultados if "PASS" in e)
print(f"\nResultado: {aprobados}/{total} componentes validados correctamente")
if aprobados == total:
    print("\n🎉 ¡Práctica 9 completada exitosamente! Todos los pasos validados.")
else:
    print(f"\n⚠️  Revisar los pasos con estado FAIL antes de finalizar.")
```

---

## Solución de Problemas

### Problema 1: `OutOfMemoryError` o Spark se congela al ejecutar UDFs

**Síntomas:**
- El kernel de Jupyter se reinicia inesperadamente durante el Paso 6.
- Aparece el error `java.lang.OutOfMemoryError: GC overhead limit exceeded` en los logs de Spark.
- La Spark UI muestra tareas en estado "FAILED" con mensaje de memoria insuficiente.

**Causa:**
Las UDFs escalares (`@udf`) serializan datos fila a fila entre la JVM y el proceso Python, consumiendo memoria adicional. Con 120,000 filas y 8 GB de RAM, la configuración por defecto puede ser insuficiente.

**Solución:**
```python
# Opción 1: Reiniciar la SparkSession con más memoria
spark.stop()

spark = SparkSession.builder \
    .appName("Practica9_FuncionesSpark") \
    .master("local[2]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.python.worker.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Opción 2: Reducir el dataset para el Paso 6
df_ventas_sample = df_ventas.sample(fraction=0.5, seed=42)
# Usar df_ventas_sample en lugar de df_ventas para el benchmark de UDFs

# Opción 3: Preferir pandas_udf en lugar de @udf siempre que sea posible
# pandas_udf usa Apache Arrow para transferencia en batch (mucho más eficiente)
```

---

### Problema 2: `AnalysisException` al usar funciones de colección (`transform`, `filter`, `zip_with`)

**Síntomas:**
- Error: `AnalysisException: Undefined function: 'transform'` o similar.
- Error: `TypeError: filter() takes 1 positional argument but 2 were given` al usar `F.filter()`.
- Las funciones de array no reconocen las lambdas de Python.

**Causa:**
Hay dos causas comunes: (1) la versión de Spark instalada es anterior a 3.1 (donde se introdujeron funciones de orden superior con lambdas en Python), o (2) hay un conflicto de nombres entre `pyspark.sql.functions.filter` y la función nativa `filter` de Python cuando no se usa el alias `F`.

**Solución:**
```python
# Verificar versión de Spark (debe ser 3.1+)
print(f"Spark version: {spark.version}")
# Si es < 3.1, actualizar: pip install pyspark==3.5.1

# Solución al conflicto de nombres — SIEMPRE usar el alias F
# ❌ Incorrecto (usa filter nativo de Python):
# from pyspark.sql.functions import filter
# df.withColumn("x", filter("etiquetas", lambda x: length(x) > 5))

# ✓ Correcto (usa F para el módulo de Spark):
from pyspark.sql import functions as F

df.withColumn("etiquetas_largas",
    F.filter("etiquetas", lambda x: F.length(x) > 5))

# Si el problema persiste con funciones de orden superior, usar SQL alternativo:
df.createOrReplaceTempView("tmp")
spark.sql("""
    SELECT order_id,
           FILTER(etiquetas, x -> LENGTH(x) > 5) AS etiquetas_largas
    FROM tmp
""")
```

---

## Limpieza del Entorno

Ejecuta la siguiente celda al finalizar la práctica para liberar recursos:

```python
# ══════════════════════════════════════════════════════════════════════
# LIMPIEZA DEL ENTORNO — Práctica 9
# ══════════════════════════════════════════════════════════════════════

# 1. Eliminar vistas temporales creadas durante la práctica
vistas = ["ventas_view", "tmp"]
for vista in vistas:
    spark.catalog.dropTempView(vista)
    print(f"Vista temporal eliminada: {vista}")

# 2. Liberar caché de DataFrames
df_raw.unpersist()
print("Caché de df_raw liberada")

# 3. Limpiar todos los DataFrames en caché
spark.catalog.clearCache()
print("Caché de Spark limpiada completamente")

# 4. Detener la SparkSession
spark.stop()
print("SparkSession detenida correctamente")
print("\n✓ Entorno limpiado. Spark UI en http://localhost:4040 ya no estará disponible.")
```

---

## Resumen

### Conceptos Consolidados

En esta práctica integradora aplicaste el espectro completo de funciones avanzadas de Spark SQL sobre un dataset real de 120,000 registros de e-commerce:

| Área | Funciones clave aplicadas | Caso de uso |
|------|--------------------------|-------------|
| **Fecha y Hora** | `to_timestamp`, `coalesce`, `date_trunc`, `last_day`, `unix_timestamp`, `year/month/dayofweek` | Parseo multi-formato, análisis de estacionalidad, aritmética temporal |
| **Texto** | `soundex`, `levenshtein`, `translate`, `format_string`, `sentences`, `regexp_extract_all` | Búsqueda fonética, distancia de edición, tokenización, formateo |
| **Colecciones** | `transform`, `filter`, `aggregate`, `zip_with`, `map_keys`, `map_values`, `array_contains` | ETL de datos semi-estructurados con arrays y maps |
| **Window Functions** | `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `ntile`, `cume_dist`, `rowsBetween` | Ranking, tendencias, medias móviles, análisis analítico |
| **UDFs** | `@udf`, `@pandas_udf`, `spark.udf.register()` | Lógica de negocio personalizada, comparación de rendimiento |
| **Catalyst** | `explain()` con modos `simple`, `extended`, `formatted` | Predicate pushdown, column pruning, interpretación de planes |

### Lecciones Aprendizadas

1. **Prioridad de funciones:** Siempre preferir funciones nativas de `pyspark.sql.functions` sobre UDFs. Cuando se requiere UDF, preferir `@pandas_udf` sobre `@udf` por su integración con Apache Arrow.

2. **Importación con alias `F`:** La convención `from pyspark.sql import functions as F` evita conflictos con funciones nativas de Python y es la práctica estándar en proyectos profesionales.

3. **Window Functions:** Definir el `WindowSpec` fuera de la expresión de columna mejora la legibilidad y permite reutilizarlo en múltiples columnas calculadas.

4. **Catalyst Optimizer:** El motor optimiza automáticamente predicate pushdown y column pruning, pero el desarrollador puede guiarlo aplicando filtros y selecciones de columnas lo más temprano posible en el pipeline.

5. **Colecciones:** Las funciones de orden superior (`transform`, `filter`, `aggregate`) permiten operar sobre arrays sin necesidad de `explode` + `groupBy`, lo que resulta en planes de ejecución más eficientes.

### Recursos Adicionales

- [Documentación oficial: `pyspark.sql.functions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Guía de Window Functions en Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [Documentación de pandas UDF (Arrow-based)](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Catalyst Optimizer — Spark Internals](https://databricks.com/glossary/catalyst-optimizer)
- [Higher-Order Functions for Complex Types](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#higher-order-functions)

---
*Lab 10-00-01 — Práctica 9 | Curso Apache Spark con PySpark | Versión 1.0*
