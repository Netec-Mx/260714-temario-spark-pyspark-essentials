# Uso de Funciones de Transformación

## 1. Metadatos

| Atributo | Detalle |
|---|---|
| **Duración estimada** | 40 minutos |
| **Complejidad** | Media |
| **Nivel Bloom** | Aplicar (*Apply*) |
| **Módulo** | 6 — Transformaciones en Spark |
| **Práctica número** | 5 |

---

## 2. Descripción General

En esta práctica trabajarás con un dataset sintético de logs de servidor web que simula los problemas típicos que encuentran los ingenieros de datos en procesos reales de ingesta: campos mal tipados, valores nulos, inconsistencias de formato y registros duplicados. A lo largo de los ejercicios clasificarás y aplicarás transformaciones **narrow** (sin *shuffle* entre particiones) y **wide** (con *shuffle*), comprenderás su impacto en el plan de ejecución y construirás un pipeline de transformación completo usando encadenamiento de métodos (*method chaining*) con las funciones del módulo `pyspark.sql.functions`.

Al finalizar habrás normalizado y enriquecido el dataset de logs hasta dejarlo listo para análisis, aplicando más de quince funciones distintas del catálogo de PySpark organizadas por categoría: cadenas de texto, matemáticas, fechas, arrays y condicionales.

---

## 3. Objetivos de Aprendizaje

Al completar esta práctica serás capaz de:

- [ ] Identificar y justificar cuándo aplicar transformaciones narrow versus wide según el costo de *shuffle* en el plan de ejecución distribuido.
- [ ] Aplicar el catálogo completo de funciones de `pyspark.sql.functions` (string, matemáticas, fecha, array y condicionales) para limpiar y enriquecer un dataset real de logs.
- [ ] Construir pipelines de transformación encadenados (*method chaining*) que normalicen datos crudos en un solo bloque de código legible y eficiente.
- [ ] Verificar el resultado de cada transformación inspeccionando el esquema, los datos y el plan de ejecución con `explain()`.

---

## 4. Prerrequisitos

### Conocimiento previo

| Tema | Nivel requerido |
|---|---|
| Práctica 4: SQL con DataFrames y Column API | Completado |
| Lazy evaluation en Spark | Comprensión conceptual |
| Concepto de *shuffle* y su costo | Comprensión conceptual |
| Expresiones regulares básicas (`\d+`, `[A-Z]+`) | Familiaridad básica |
| Creación de `SparkSession` y lectura de CSV | Competencia práctica |

### Acceso y recursos

| Recurso | Detalle |
|---|---|
| Jupyter Notebook / JupyterLab activo | Puerto 8888 disponible |
| Spark UI | Puerto 4040 disponible (verificar antes de iniciar) |
| Dataset `web_server_logs.csv` | Generado en el **Paso 1** de esta práctica |
| Espacio en disco | ~200 MB libres en el directorio de trabajo |

---

## 5. Entorno de Laboratorio

### Hardware mínimo recomendado

| Componente | Mínimo | Recomendado |
|---|---|---|
| RAM | 8 GB | 16 GB |
| CPU | 4 núcleos, 64 bits | 4+ núcleos con VT-x/AMD-V |
| Disco libre | 20 GB | 20 GB+ |

### Software requerido

| Componente | Versión |
|---|---|
| Java (JDK) | 11 o 17 LTS |
| Apache Spark | 3.5.x |
| Python | 3.10 o 3.11 |
| PySpark | 3.5.x |
| JupyterLab | 7.x |
| pandas | 2.0+ |
| NumPy | 1.24+ |

### Configuración inicial del entorno

Ejecuta los siguientes comandos en una celda de Jupyter **antes de comenzar** para verificar que el entorno esté operativo:

```python
# Celda 0 — Verificación del entorno
import sys
import pyspark

print(f"Python version : {sys.version}")
print(f"PySpark version: {pyspark.__version__}")

# Verificar que Java esté disponible
import subprocess
result = subprocess.run(["java", "-version"], capture_output=True, text=True)
print(f"Java           : {result.stderr.strip().splitlines()[0]}")
```

**Salida esperada (ejemplo):**
```
Python version : 3.11.x ...
PySpark version: 3.5.x
Java           : openjdk version "17.x.x" ...
```

> **Nota para Windows:** Asegúrate de que `HADOOP_HOME` apunte a la carpeta que contiene `winutils.exe` y que la variable `JAVA_HOME` esté configurada correctamente antes de iniciar Jupyter.

---

## 6. Desarrollo Paso a Paso

---

### Paso 1 — Generación del Dataset de Logs y Creación de la SparkSession

**Objetivo:** Crear el dataset sintético de logs de servidor web con problemas de calidad intencionados y configurar la `SparkSession` con parámetros adecuados para equipos con 8–16 GB de RAM.

#### Instrucciones

1. Crea un nuevo notebook llamado `Lab06_Transformaciones.ipynb`.

2. En la **Celda 1**, genera el dataset sintético usando pandas y guárdalo como CSV:

```python
# Celda 1 — Generación del dataset de logs con problemas de calidad
import pandas as pd
import numpy as np
import random
import os

random.seed(42)
np.random.seed(42)

# Parámetros del dataset
N = 150_000  # 150 000 registros para demostrar valor de Spark en modo local

# Valores de dominio
metodos = ["GET", "POST", "PUT", "DELETE", "get", "post", " GET ", "PATCH"]
endpoints = [
    "/api/users", "/api/products", "/api/orders", "/api/auth/login",
    "/api/auth/logout", "/static/css/main.css", "/static/js/app.js",
    "/api/search", "/api/cart", "/api/checkout", None
]
codigos_http = [200, 200, 200, 301, 304, 400, 401, 403, 404, 404, 500, 502]
paises = ["México", "mexico", "MX", "Colombia", "COLOMBIA", "co",
          "Argentina", "AR", "Chile", "chile", "CL", "Perú", "PE", None]
navegadores = ["Chrome/120", "Firefox/121", "Safari/17", "Edge/120",
               "curl/7.88", "python-requests/2.31", None]

# Generación de IPs (algunas duplicadas intencionalmente)
def gen_ip():
    return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

ips_base = [gen_ip() for _ in range(50_000)]

# Construcción del DataFrame
data = {
    "log_id": list(range(1, N + 1)),
    "timestamp_str": pd.date_range(
        start="2024-01-01 00:00:00",
        periods=N,
        freq="2s"
    ).strftime("%Y-%m-%d %H:%M:%S").tolist(),
    "ip_address": [random.choice(ips_base) for _ in range(N)],
    "http_method": [random.choice(metodos) for _ in range(N)],
    "endpoint": [random.choice(endpoints) for _ in range(N)],
    "status_code": [random.choice(codigos_http) for _ in range(N)],
    "response_time_ms": np.where(
        np.random.random(N) < 0.02,
        -1,  # ~2% valores negativos (atípicos)
        np.random.exponential(scale=250, size=N).astype(int)
    ).tolist(),
    "bytes_sent": np.where(
        np.random.random(N) < 0.05,
        None,  # ~5% nulos
        np.random.randint(100, 50_000, size=N)
    ).tolist(),
    "country": [random.choice(paises) for _ in range(N)],
    "user_agent": [random.choice(navegadores) for _ in range(N)],
    "session_id": [
        f"sess_{random.randint(1000, 9999)}" if random.random() > 0.08 else None
        for _ in range(N)
    ],
    "referrer_url": [
        random.choice([
            "https://google.com", "https://bing.com", "direct",
            "https://twitter.com", None, ""
        ])
        for _ in range(N)
    ],
}

df_pandas = pd.DataFrame(data)

# Introducir duplicados explícitos (~3% del total)
duplicados = df_pandas.sample(frac=0.03, random_state=42)
df_pandas = pd.concat([df_pandas, duplicados], ignore_index=True).sample(
    frac=1, random_state=42
).reset_index(drop=True)

# Guardar como CSV
os.makedirs("data", exist_ok=True)
df_pandas.to_csv("data/web_server_logs.csv", index=False)

print(f"Dataset generado: {len(df_pandas):,} filas × {len(df_pandas.columns)} columnas")
print(f"Archivo guardado en: data/web_server_logs.csv")
print(f"Tamaño aproximado: {os.path.getsize('data/web_server_logs.csv') / 1_048_576:.1f} MB")
```

3. En la **Celda 2**, crea la `SparkSession` con configuración apropiada:

```python
# Celda 2 — Creación de SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lab06-Transformaciones-Logs") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Configurar nivel de log para reducir ruido en el notebook
spark.sparkContext.setLogLevel("WARN")

print(f"Spark version  : {spark.version}")
print(f"Spark UI       : http://localhost:4040")
print(f"Shuffle parts  : {spark.conf.get('spark.sql.shuffle.partitions')}")
```

4. En la **Celda 3**, lee el CSV y examina el estado crudo de los datos:

```python
# Celda 3 — Lectura del dataset crudo
df_crudo = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "") \
    .csv("data/web_server_logs.csv")

print("=== ESQUEMA INICIAL ===")
df_crudo.printSchema()

print(f"\n=== DIMENSIONES ===")
print(f"Filas: {df_crudo.count():,}")
print(f"Columnas: {len(df_crudo.columns)}")

print("\n=== MUESTRA DE DATOS CRUDOS ===")
df_crudo.show(5, truncate=60)
```

#### Salida esperada

```
=== ESQUEMA INICIAL ===
root
 |-- log_id: integer (nullable = true)
 |-- timestamp_str: string (nullable = true)
 |-- ip_address: string (nullable = true)
 |-- http_method: string (nullable = true)
 |-- endpoint: string (nullable = true)
 |-- status_code: integer (nullable = true)
 |-- response_time_ms: integer (nullable = true)
 |-- bytes_sent: double (nullable = true)
 |-- country: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- session_id: string (nullable = true)
 |-- referrer_url: string (nullable = true)

=== DIMENSIONES ===
Filas: 154,500  (aprox., varía ±200 por aleatoriedad)
Columnas: 12
```

#### Verificación

```python
# Verificación Paso 1 — Confirmar problemas de calidad presentes
from pyspark.sql.functions import col, count, when, isnan

print("=== CONTEO DE NULOS POR COLUMNA ===")
df_crudo.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_crudo.columns
]).show()

print("=== VALORES ÚNICOS EN http_method (inconsistencias de formato) ===")
df_crudo.select("http_method").distinct().show()

print("=== VALORES ÚNICOS EN country (inconsistencias) ===")
df_crudo.select("country").distinct().orderBy("country").show(30)
```

> **Punto de control:** Deberías observar nulos en `bytes_sent`, `endpoint`, `country`, `user_agent`, `session_id` y `referrer_url`; valores mixtos de mayúsculas/minúsculas en `http_method` y `country`; y valores negativos en `response_time_ms`.

---

### Paso 2 — Transformaciones Narrow: Limpieza Básica sin Shuffle

**Objetivo:** Aplicar transformaciones narrow (`select`, `withColumn`, `filter`) que procesan datos dentro de cada partición sin mover datos entre nodos, comprendiendo por qué son más eficientes.

> **Concepto clave:** Las transformaciones **narrow** (estrechas) son aquellas donde cada partición de salida depende de exactamente una partición de entrada. No requieren *shuffle* de datos entre particiones. Ejemplos: `map`, `filter`, `withColumn`, `select`, `union`. Las transformaciones **wide** (amplias) requieren que los datos de múltiples particiones se reagrupen (*shuffle*), como `groupBy`, `join`, `distinct`, `repartition`.

#### Instrucciones

1. En la **Celda 4**, aplica transformaciones narrow de limpieza de tipos y texto:

```python
# Celda 4 — Transformaciones NARROW: limpieza de tipos y texto
from pyspark.sql.functions import (
    col, trim, upper, lower, to_timestamp, when,
    regexp_replace, coalesce, lit
)

# NARROW 1: Normalización de texto (opera columna a columna, sin shuffle)
df_step2 = df_crudo \
    .withColumn(
        "http_method_clean",
        upper(trim(col("http_method")))           # Eliminar espacios + mayúsculas
    ) \
    .withColumn(
        "country_norm",
        lower(trim(col("country")))               # Normalizar país a minúsculas
    ) \
    .withColumn(
        "timestamp_ts",
        to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")  # String → Timestamp
    ) \
    .withColumn(
        "bytes_sent_clean",
        coalesce(col("bytes_sent"), lit(0.0))     # Reemplazar nulos por 0
    ) \
    .withColumn(
        "referrer_clean",
        when(
            col("referrer_url").isNull() | (col("referrer_url") == ""),
            lit("direct")
        ).otherwise(col("referrer_url"))
    )

# NARROW 2: Filtrar registros con response_time negativo (valores atípicos)
df_step2 = df_step2.filter(col("response_time_ms") >= 0)

# NARROW 3: Filtrar registros sin endpoint
df_step2 = df_step2.filter(col("endpoint").isNotNull())

print("=== ESQUEMA DESPUÉS DE TRANSFORMACIONES NARROW ===")
df_step2.select(
    "http_method", "http_method_clean",
    "country", "country_norm",
    "timestamp_str", "timestamp_ts",
    "bytes_sent", "bytes_sent_clean",
    "referrer_url", "referrer_clean"
).show(8, truncate=40)

print(f"\nFilas después de filtros narrow: {df_step2.count():,}")
```

2. En la **Celda 5**, verifica el plan de ejecución de las transformaciones narrow:

```python
# Celda 5 — Inspección del plan de ejecución (solo transformaciones narrow)
print("=== PLAN DE EJECUCIÓN (transformaciones narrow) ===")
print("Observa que NO aparece 'Exchange' (shuffle) en el plan:\n")
df_step2.select("http_method_clean", "country_norm", "timestamp_ts").explain(mode="simple")
```

#### Salida esperada (fragmento)

```
=== PLAN DE EJECUCIÓN (transformaciones narrow) ===
Observa que NO aparece 'Exchange' (shuffle) en el plan:

== Physical Plan ==
*(1) Project [upper(trim(http_method#...)) AS http_method_clean#..., ...]
+- *(1) Filter ((response_time_ms#... >= 0) AND isnotnull(endpoint#...))
   +- FileScan csv [...] ...
```

> **Punto de control:** El plan físico debe mostrar un único stage (`*(1)`) sin ninguna operación `Exchange`, confirmando que no hay *shuffle*.

#### Verificación

```python
# Verificación Paso 2
print("=== VERIFICAR NORMALIZACIÓN DE http_method ===")
df_step2.select("http_method_clean").distinct().orderBy("http_method_clean").show()

print("=== VERIFICAR QUE NO HAY response_time NEGATIVOS ===")
from pyspark.sql.functions import min as spark_min, max as spark_max
df_step2.select(
    spark_min("response_time_ms").alias("min_response"),
    spark_max("response_time_ms").alias("max_response")
).show()

print("=== VERIFICAR NULOS EN bytes_sent_clean ===")
from pyspark.sql.functions import count
df_step2.select(
    count(when(col("bytes_sent_clean").isNull(), 1)).alias("nulos_bytes_sent")
).show()
```

---

### Paso 3 — Funciones de String: Extracción y Enriquecimiento de Texto

**Objetivo:** Utilizar el catálogo de funciones de cadena de `pyspark.sql.functions` para extraer información estructurada del campo `endpoint` y `user_agent`.

#### Instrucciones

1. En la **Celda 6**, aplica funciones de string para extraer componentes del endpoint y categorizar el user agent:

```python
# Celda 6 — Funciones de STRING
from pyspark.sql.functions import (
    split, substring, concat_ws, regexp_extract,
    length, instr, locate
)

df_step3 = df_step2 \
    .withColumn(
        # Extraer la sección principal del endpoint: /api/users → api
        "endpoint_section",
        regexp_extract(col("endpoint"), r"^/([^/]+)/.*$", 1)
    ) \
    .withColumn(
        # Extraer el recurso del endpoint: /api/users → users
        "endpoint_resource",
        regexp_extract(col("endpoint"), r"^/[^/]+/([^/]+).*$", 1)
    ) \
    .withColumn(
        # Extraer nombre del navegador (antes del slash): Chrome/120 → Chrome
        "browser_name",
        when(
            col("user_agent").isNotNull(),
            regexp_extract(col("user_agent"), r"^([^/]+)/", 1)
        ).otherwise(lit("Unknown"))
    ) \
    .withColumn(
        # Extraer versión del navegador (después del slash): Chrome/120 → 120
        "browser_version",
        when(
            col("user_agent").isNotNull(),
            regexp_extract(col("user_agent"), r"/(.+)$", 1)
        ).otherwise(lit("0"))
    ) \
    .withColumn(
        # Construir etiqueta combinada: Chrome v120
        "browser_label",
        concat_ws(" v", col("browser_name"), col("browser_version"))
    ) \
    .withColumn(
        # Longitud del endpoint para análisis de complejidad de URL
        "endpoint_length",
        length(col("endpoint"))
    ) \
    .withColumn(
        # Primeros 20 caracteres del session_id para anonimización
        "session_prefix",
        substring(col("session_id"), 1, 9)   # "sess_XXXX"
    )

print("=== RESULTADO DE FUNCIONES DE STRING ===")
df_step3.select(
    "endpoint", "endpoint_section", "endpoint_resource",
    "user_agent", "browser_name", "browser_version", "browser_label",
    "endpoint_length"
).show(10, truncate=35)
```

2. En la **Celda 7**, verifica la distribución de secciones de endpoint:

```python
# Celda 7 — Distribución de secciones de endpoint
print("=== DISTRIBUCIÓN POR SECCIÓN DE ENDPOINT ===")
df_step3.groupBy("endpoint_section") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("=== DISTRIBUCIÓN POR NAVEGADOR ===")
df_step3.groupBy("browser_name") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()
```

#### Salida esperada (fragmento)

```
=== DISTRIBUCIÓN POR SECCIÓN DE ENDPOINT ===
+----------------+------+
|endpoint_section| count|
+----------------+------+
|             api| 89xxx|
|          static| 30xxx|
|                |  xxxx|
+----------------+------+

=== DISTRIBUCIÓN POR NAVEGADOR ===
+---------+------+
|browser_n| count|
+---------+------+
|   Chrome| 25xxx|
|  Firefox| 24xxx|
|   Safari| 24xxx|
...
```

#### Verificación

```python
# Verificación Paso 3
print("=== VERIFICAR QUE browser_label NO TIENE NULOS ===")
df_step3.select(
    count(when(col("browser_label").isNull(), 1)).alias("nulos_browser_label"),
    count(when(col("endpoint_section") == "", 1)).alias("endpoint_section_vacios")
).show()
```

---

### Paso 4 — Funciones de Fecha y Funciones Matemáticas

**Objetivo:** Enriquecer el dataset con dimensiones temporales usando funciones de fecha y calcular métricas derivadas con funciones matemáticas.

#### Instrucciones

1. En la **Celda 8**, aplica funciones de fecha para extraer componentes temporales:

```python
# Celda 8 — Funciones de FECHA
from pyspark.sql.functions import (
    to_date, date_format, hour, dayofweek, dayofmonth,
    month, year, weekofyear, datediff, current_date,
    months_between
)

df_step4 = df_step3 \
    .withColumn(
        "log_date",
        to_date(col("timestamp_ts"))                          # Timestamp → Date
    ) \
    .withColumn(
        "log_hour",
        hour(col("timestamp_ts"))                             # Hora del día (0-23)
    ) \
    .withColumn(
        "day_of_week",
        date_format(col("timestamp_ts"), "EEEE")              # Nombre del día
    ) \
    .withColumn(
        "day_of_week_num",
        dayofweek(col("timestamp_ts"))                        # Número (1=Dom, 7=Sáb)
    ) \
    .withColumn(
        "week_of_year",
        weekofyear(col("timestamp_ts"))                       # Semana del año
    ) \
    .withColumn(
        "month_name",
        date_format(col("timestamp_ts"), "MMMM")              # Nombre del mes
    ) \
    .withColumn(
        "year_num",
        year(col("timestamp_ts"))
    ) \
    .withColumn(
        "days_since_log",
        datediff(current_date(), col("log_date"))             # Días desde el log
    ) \
    .withColumn(
        "is_weekend",
        when(dayofweek(col("timestamp_ts")).isin([1, 7]), True).otherwise(False)
    ) \
    .withColumn(
        "time_slot",
        when(col("log_hour").between(0, 5),   lit("madrugada"))
        .when(col("log_hour").between(6, 11),  lit("mañana"))
        .when(col("log_hour").between(12, 17), lit("tarde"))
        .otherwise(lit("noche"))
    )

print("=== DIMENSIONES TEMPORALES EXTRAÍDAS ===")
df_step4.select(
    "timestamp_ts", "log_date", "log_hour",
    "day_of_week", "month_name", "is_weekend", "time_slot"
).show(8, truncate=30)
```

2. En la **Celda 9**, aplica funciones matemáticas para calcular métricas derivadas:

```python
# Celda 9 — Funciones MATEMÁTICAS
from pyspark.sql.functions import round, ceil, floor, abs as spark_abs, pow as spark_pow, sqrt, log

df_step4 = df_step4 \
    .withColumn(
        # Convertir bytes a KB con 2 decimales
        "bytes_sent_kb",
        round(col("bytes_sent_clean") / 1024.0, 2)
    ) \
    .withColumn(
        # Convertir ms a segundos con redondeo hacia arriba (para SLAs)
        "response_time_sec_ceil",
        ceil(col("response_time_ms") / 1000.0)
    ) \
    .withColumn(
        # Logaritmo del tiempo de respuesta (útil para distribuciones sesgadas)
        "log_response_time",
        round(log(col("response_time_ms") + 1), 4)
    ) \
    .withColumn(
        # Clasificar velocidad de respuesta
        "response_category",
        when(col("response_time_ms") <= 100,  lit("rapido"))
        .when(col("response_time_ms") <= 500,  lit("normal"))
        .when(col("response_time_ms") <= 2000, lit("lento"))
        .otherwise(lit("muy_lento"))
    ) \
    .withColumn(
        # Score de penalización: sqrt(response_time) para normalizar outliers
        "response_score",
        round(sqrt(col("response_time_ms").cast("double")), 2)
    )

print("=== MÉTRICAS MATEMÁTICAS CALCULADAS ===")
df_step4.select(
    "response_time_ms", "response_time_sec_ceil",
    "bytes_sent_clean", "bytes_sent_kb",
    "log_response_time", "response_category", "response_score"
).show(8)
```

#### Salida esperada (fragmento)

```
+----------------+----------------------+----------------+-------------+------------------+-----------------+--------------+
|response_time_ms|response_time_sec_ceil|bytes_sent_clean|bytes_sent_kb|log_response_time |response_category|response_score|
+----------------+----------------------+----------------+-------------+------------------+-----------------+--------------+
|             342|                     1|          8192.0|         8.00|            5.8377|           normal|         18.49|
|              87|                     1|         15360.0|        15.00|            4.4773|           rapido|          9.33|
...
```

#### Verificación

```python
# Verificación Paso 4
print("=== DISTRIBUCIÓN DE CATEGORÍAS DE RESPUESTA ===")
df_step4.groupBy("response_category") \
    .count() \
    .orderBy("response_category") \
    .show()

print("=== DISTRIBUCIÓN POR TIME_SLOT ===")
df_step4.groupBy("time_slot") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

print("=== VERIFICAR bytes_sent_kb NO NEGATIVO ===")
df_step4.select(
    spark_min("bytes_sent_kb").alias("min_kb"),
    spark_max("bytes_sent_kb").alias("max_kb")
).show()
```

---

### Paso 5 — Transformaciones Wide: GroupBy, Join y Distinct

**Objetivo:** Aplicar transformaciones wide que requieren *shuffle* de datos, observar su aparición en el plan de ejecución y entender cuándo son necesarias a pesar de su costo.

> **Concepto clave:** Las transformaciones wide son inevitables en muchos análisis (agregaciones, joins, deduplicación global). El objetivo no es evitarlas sino minimizar el volumen de datos que participan en el *shuffle* aplicando filtros y proyecciones narrow **antes** de la operación wide.

#### Instrucciones

1. En la **Celda 10**, elimina duplicados globales (transformación wide) y observa el plan:

```python
# Celda 10 — WIDE: Eliminación de duplicados globales
print(f"Filas antes de dropDuplicates: {df_step4.count():,}")

# dropDuplicates es una transformación WIDE (requiere comparar entre particiones)
df_dedup = df_step4.dropDuplicates(["log_id"])

print(f"Filas después de dropDuplicates: {df_dedup.count():,}")
print(f"Duplicados eliminados: {df_step4.count() - df_dedup.count():,}")

print("\n=== PLAN DE EJECUCIÓN (dropDuplicates — WIDE) ===")
print("Observa 'Exchange' en el plan (indica shuffle):\n")
df_dedup.explain(mode="simple")
```

2. En la **Celda 11**, crea un DataFrame de referencia de países y realiza un join:

```python
# Celda 11 — WIDE: Join con tabla de referencia de países
from pyspark.sql.functions import broadcast

# Crear DataFrame de referencia (pequeño — candidato a broadcast join)
paises_data = [
    ("méxico", "México", "MX", "América del Norte", "MXN"),
    ("mexico", "México", "MX", "América del Norte", "MXN"),
    ("mx",     "México", "MX", "América del Norte", "MXN"),
    ("colombia","Colombia","CO","América del Sur",  "COP"),
    ("co",     "Colombia","CO","América del Sur",   "COP"),
    ("argentina","Argentina","AR","América del Sur","ARS"),
    ("ar",     "Argentina","AR","América del Sur",  "ARS"),
    ("chile",  "Chile",   "CL","América del Sur",   "CLP"),
    ("cl",     "Chile",   "CL","América del Sur",   "CLP"),
    ("chile",  "Chile",   "CL","América del Sur",   "CLP"),
    ("perú",   "Perú",    "PE","América del Sur",   "PEN"),
    ("pe",     "Perú",    "PE","América del Sur",   "PEN"),
]

schema_paises = ["country_key", "country_name", "country_code", "region", "currency"]
df_paises = spark.createDataFrame(paises_data, schema=schema_paises)

print("=== TABLA DE REFERENCIA DE PAÍSES ===")
df_paises.show()

# JOIN (wide) — usar broadcast para la tabla pequeña (optimización)
df_enriched = df_dedup.join(
    broadcast(df_paises),            # broadcast evita shuffle del DataFrame pequeño
    df_dedup["country_norm"] == df_paises["country_key"],
    how="left"                        # left join: conservar logs sin match
)

print(f"\nFilas después del join: {df_enriched.count():,}")
print("\n=== PLAN DE EJECUCIÓN (JOIN con broadcast) ===")
df_enriched.select(
    "log_id", "country_norm", "country_name", "region", "currency"
).explain(mode="simple")
```

3. En la **Celda 12**, realiza una agregación con `groupBy`:

```python
# Celda 12 — WIDE: Agregación con groupBy
from pyspark.sql.functions import (
    count, avg, sum as spark_sum,
    max as spark_max, min as spark_min,
    countDistinct, round
)

# Agregación por endpoint_section y response_category
df_agg = df_enriched \
    .groupBy("endpoint_section", "response_category") \
    .agg(
        count("*").alias("total_requests"),
        round(avg("response_time_ms"), 2).alias("avg_response_ms"),
        round(avg("bytes_sent_kb"), 2).alias("avg_bytes_kb"),
        spark_max("response_time_ms").alias("max_response_ms"),
        countDistinct("ip_address").alias("unique_ips"),
        spark_sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("server_errors")
    ) \
    .orderBy("endpoint_section", "response_category")

print("=== MÉTRICAS AGREGADAS POR SECCIÓN Y CATEGORÍA DE RESPUESTA ===")
df_agg.show(20, truncate=25)

print("\n=== PLAN DE EJECUCIÓN (groupBy — WIDE con Exchange) ===")
df_agg.explain(mode="simple")
```

#### Salida esperada (fragmento del plan con groupBy)

```
=== PLAN DE EJECUCIÓN (groupBy — WIDE con Exchange) ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Current Plan ==
   SortAggregate(key=[endpoint_section#...], ...)
   +- Exchange hashpartitioning(endpoint_section#..., 8), ...   ← SHUFFLE aquí
      +- SortAggregate(key=[endpoint_section#...], ...)
         +- ...
```

> **Punto de control:** Confirma que el plan del `groupBy` contiene `Exchange` (indicador de *shuffle*), a diferencia del plan de transformaciones narrow del Paso 2.

#### Verificación

```python
# Verificación Paso 5
print("=== RESUMEN DE TRANSFORMACIONES WIDE APLICADAS ===")
print(f"  - dropDuplicates aplicado: registros únicos por log_id")
print(f"  - JOIN con df_paises (broadcast): enriquecimiento de país")
print(f"  - groupBy + agg: métricas por sección y categoría")

print("\n=== VERIFICAR COBERTURA DEL JOIN ===")
df_enriched.select(
    count(when(col("country_name").isNotNull(), 1)).alias("con_pais_mapeado"),
    count(when(col("country_name").isNull(), 1)).alias("sin_mapeo")
).show()
```

---

### Paso 6 — Funciones de Array y Pipeline Final Encadenado

**Objetivo:** Utilizar funciones de array para crear estructuras de datos complejas y construir el pipeline de transformación completo en un único bloque de *method chaining*.

#### Instrucciones

1. En la **Celda 13**, aplica funciones de array para crear etiquetas y tags:

```python
# Celda 13 — Funciones de ARRAY
from pyspark.sql.functions import (
    array, array_contains, array_distinct,
    size, sort_array, explode, flatten,
    collect_list, collect_set
)

# Crear un array de "tags" basado en características del request
df_tagged = df_enriched \
    .withColumn(
        "request_tags",
        array_distinct(
            array(
                when(col("status_code") >= 500, lit("error_servidor")).otherwise(lit(None)),
                when(col("status_code") == 404, lit("not_found")).otherwise(lit(None)),
                when(col("status_code") == 401, lit("no_autorizado")).otherwise(lit(None)),
                when(col("response_category") == "muy_lento", lit("performance_issue")).otherwise(lit(None)),
                when(col("is_weekend"), lit("trafico_fin_semana")).otherwise(lit(None)),
                when(col("endpoint_section") == "api", lit("api_call")).otherwise(lit(None)),
                when(col("endpoint_section") == "static", lit("static_asset")).otherwise(lit(None)),
            )
        )
    ) \
    .withColumn(
        # Eliminar nulos del array de tags
        "request_tags_clean",
        array_distinct(
            # Filtrar elementos nulos usando higher-order function
            col("request_tags")
        )
    ) \
    .withColumn(
        "num_tags",
        size(col("request_tags"))
    ) \
    .withColumn(
        "has_performance_issue",
        array_contains(col("request_tags"), "performance_issue")
    )

print("=== FUNCIONES DE ARRAY APLICADAS ===")
df_tagged.select(
    "log_id", "status_code", "response_category",
    "is_weekend", "endpoint_section",
    "request_tags", "num_tags", "has_performance_issue"
).filter(col("status_code") >= 400).show(10, truncate=60)
```

2. En la **Celda 14**, construye el pipeline final completo con *method chaining*:

```python
# Celda 14 — PIPELINE FINAL: Selección de columnas definitivas con method chaining
from pyspark.sql.functions import monotonically_increasing_id

# Pipeline final: seleccionar y renombrar solo las columnas de valor analítico
df_final = df_tagged \
    .select(
        # Identificadores
        col("log_id"),
        col("timestamp_ts").alias("timestamp"),
        col("log_date").alias("fecha"),
        # Dimensiones temporales
        col("log_hour").alias("hora"),
        col("day_of_week").alias("dia_semana"),
        col("time_slot").alias("franja_horaria"),
        col("is_weekend").alias("es_fin_semana"),
        col("week_of_year").alias("semana_año"),
        # Red
        col("ip_address"),
        col("http_method_clean").alias("metodo_http"),
        col("endpoint"),
        col("endpoint_section").alias("seccion"),
        col("endpoint_resource").alias("recurso"),
        # Respuesta
        col("status_code").alias("codigo_estado"),
        col("response_time_ms").alias("tiempo_respuesta_ms"),
        col("response_time_sec_ceil").alias("tiempo_respuesta_seg"),
        col("response_category").alias("categoria_respuesta"),
        col("response_score").alias("score_respuesta"),
        col("bytes_sent_kb").alias("bytes_kb"),
        # Cliente
        col("browser_name").alias("navegador"),
        col("browser_version").alias("version_navegador"),
        col("browser_label").alias("etiqueta_navegador"),
        col("session_id"),
        col("referrer_clean").alias("referrer"),
        # Geografía
        col("country_name").alias("pais"),
        col("country_code").alias("codigo_pais"),
        col("region"),
        # Tags
        col("request_tags").alias("etiquetas"),
        col("has_performance_issue").alias("problema_rendimiento"),
    ) \
    .orderBy("timestamp")

print("=== ESQUEMA FINAL DEL PIPELINE ===")
df_final.printSchema()

print(f"\n=== DIMENSIONES FINALES ===")
print(f"Filas: {df_final.count():,}")
print(f"Columnas: {len(df_final.columns)}")

print("\n=== MUESTRA DEL DATASET TRANSFORMADO ===")
df_final.show(5, truncate=30)
```

3. En la **Celda 15**, guarda el resultado del pipeline:

```python
# Celda 15 — Persistencia del resultado
import os

output_path = "data/web_logs_transformados"

df_final.coalesce(4).write \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path)

print(f"Dataset transformado guardado en: {output_path}")
print(f"Archivos generados:")
for f in sorted(os.listdir(output_path)):
    if not f.startswith("."):
        size_mb = os.path.getsize(f"{output_path}/{f}") / 1_048_576
        print(f"  {f}  ({size_mb:.2f} MB)")
```

#### Salida esperada

```
=== ESQUEMA FINAL DEL PIPELINE ===
root
 |-- log_id: integer (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- fecha: date (nullable = true)
 |-- hora: integer (nullable = true)
 |-- dia_semana: string (nullable = true)
 |-- franja_horaria: string (nullable = true)
 |-- es_fin_semana: boolean (nullable = true)
 ...
 |-- etiquetas: array<string> (nullable = true)
 |-- problema_rendimiento: boolean (nullable = true)

=== DIMENSIONES FINALES ===
Filas: ~150,000
Columnas: 29
```

#### Verificación

```python
# Verificación Paso 6
print("=== VERIFICAR LECTURA DEL PARQUET GUARDADO ===")
df_verificacion = spark.read.parquet("data/web_logs_transformados")
print(f"Filas leídas desde Parquet: {df_verificacion.count():,}")

print("\n=== DISTRIBUCIÓN DE PROBLEMAS DE RENDIMIENTO ===")
df_final.groupBy("problema_rendimiento") \
    .count() \
    .show()

print("\n=== TOP 5 ENDPOINTS POR TIEMPO DE RESPUESTA PROMEDIO ===")
df_final.groupBy("endpoint") \
    .agg(round(avg("tiempo_respuesta_ms"), 1).alias("avg_ms"),
         count("*").alias("requests")) \
    .orderBy(col("avg_ms").desc()) \
    .show(5)
```

---

## 7. Validación y Pruebas Finales

Ejecuta la siguiente celda de validación integral para confirmar que el pipeline completo cumple los criterios de calidad:

```python
# Celda de VALIDACIÓN INTEGRAL
from pyspark.sql.functions import countDistinct

print("=" * 60)
print("VALIDACIÓN INTEGRAL DEL PIPELINE DE TRANSFORMACIONES")
print("=" * 60)

resultados = {}

# 1. Sin valores negativos en tiempo de respuesta
neg_response = df_final.filter(col("tiempo_respuesta_ms") < 0).count()
resultados["Sin tiempos negativos"] = neg_response == 0

# 2. Sin nulos en columnas críticas
for col_name in ["log_id", "timestamp", "metodo_http", "codigo_estado"]:
    nulos = df_final.filter(col(col_name).isNull()).count()
    resultados[f"Sin nulos en {col_name}"] = nulos == 0

# 3. http_method solo en mayúsculas y sin espacios
metodos_invalidos = df_final.filter(
    col("metodo_http") != upper(trim(col("metodo_http")))
).count()
resultados["metodo_http normalizado"] = metodos_invalidos == 0

# 4. bytes_kb no negativo
neg_bytes = df_final.filter(col("bytes_kb") < 0).count()
resultados["Sin bytes negativos"] = neg_bytes == 0

# 5. Columnas de fecha correctamente tipadas
from pyspark.sql.types import TimestampType, DateType
schema_dict = {f.name: f.dataType for f in df_final.schema.fields}
resultados["timestamp es TimestampType"] = isinstance(schema_dict.get("timestamp"), TimestampType)
resultados["fecha es DateType"] = isinstance(schema_dict.get("fecha"), DateType)

# 6. Columna etiquetas es ArrayType
from pyspark.sql.types import ArrayType
resultados["etiquetas es ArrayType"] = isinstance(schema_dict.get("etiquetas"), ArrayType)

# 7. Verificar que se eliminaron duplicados
total_ids = df_final.count()
unique_ids = df_final.select(countDistinct("log_id")).collect()[0][0]
resultados["Sin log_id duplicados"] = total_ids == unique_ids

# Mostrar resultados
print(f"\n{'Criterio':<40} {'Resultado':>10}")
print("-" * 52)
todos_ok = True
for criterio, ok in resultados.items():
    estado = "✅ PASS" if ok else "❌ FAIL"
    print(f"  {criterio:<38} {estado:>10}")
    if not ok:
        todos_ok = False

print("-" * 52)
print(f"\n{'RESULTADO GLOBAL:':>42} {'✅ TODO OK' if todos_ok else '❌ HAY FALLOS'}")

if todos_ok:
    print("\n🎉 Pipeline de transformaciones validado correctamente.")
    print(f"   Dataset final: {df_final.count():,} filas × {len(df_final.columns)} columnas")
else:
    print("\n⚠️  Revisa los pasos anteriores para corregir los fallos.")
```

**Salida esperada:**
```
============================================================
VALIDACIÓN INTEGRAL DEL PIPELINE DE TRANSFORMACIONES
============================================================

Criterio                                 Resultado
----------------------------------------------------
  Sin tiempos negativos                      ✅ PASS
  Sin nulos en log_id                        ✅ PASS
  Sin nulos en timestamp                     ✅ PASS
  Sin nulos en metodo_http                   ✅ PASS
  Sin nulos en codigo_estado                 ✅ PASS
  metodo_http normalizado                    ✅ PASS
  Sin bytes negativos                        ✅ PASS
  timestamp es TimestampType                 ✅ PASS
  fecha es DateType                          ✅ PASS
  etiquetas es ArrayType                     ✅ PASS
  Sin log_id duplicados                      ✅ PASS
----------------------------------------------------

RESULTADO GLOBAL:                          ✅ TODO OK
```

---

## 8. Solución de Problemas

### Problema 1: `AnalysisException: Cannot resolve column name`

**Síntoma:** Al ejecutar una celda con `withColumn` o `select`, Spark lanza:
```
pyspark.errors.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]
A column or function parameter with name `country_norm` cannot be resolved.
Did you mean one of the following? [country, country_key, ...]
```

**Causa:** Se está intentando referenciar una columna creada en una transformación anterior que todavía no ha sido materializada, o se cometió un error tipográfico en el nombre de la columna. Esto ocurre frecuentemente cuando se construye un pipeline largo y se divide en múltiples `df_stepX` pero se mezclan referencias entre DataFrames distintos (por ejemplo, usar `col("country_norm")` sobre `df_crudo` en lugar de `df_step2`).

**Solución:**
```python
# 1. Verificar las columnas disponibles en el DataFrame actual
print(df_step2.columns)   # ← asegúrate de que 'country_norm' aparece aquí

# 2. Si el nombre es correcto pero no aparece, revisar en qué paso se creó
# y asegurarse de referenciar el DataFrame correcto:
df_step3 = df_step2.withColumn(  # ← usar df_step2, no df_crudo
    "endpoint_section",
    regexp_extract(col("endpoint"), r"^/([^/]+)/.*$", 1)
)

# 3. En pipelines largos, usar un único DataFrame con method chaining
# para evitar confusiones de nombres entre variables intermedias
```

---

### Problema 2: `OutOfMemoryError` o Spark se congela durante `groupBy` / `join`

**Síntoma:** La celda con `groupBy().agg()` o el join queda ejecutándose indefinidamente (>5 minutos) o el kernel de Jupyter muere con un error de memoria:
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

**Causa:** En equipos con 8 GB de RAM, el valor por defecto de `spark.sql.shuffle.partitions` (200) genera demasiada sobrecarga para un dataset de 150K filas en modo local. Adicionalmente, si `spark.driver.memory` no fue configurado al crear la `SparkSession`, Spark usa solo 1 GB por defecto.

**Solución:**
```python
# Opción A: Reducir particiones de shuffle ANTES de la operación wide
# (ejecutar antes de la celda que falla)
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Opción B: Si la SparkSession ya está activa y no configuraste memoria,
# detenerla y recrearla con más memoria
spark.stop()

spark = SparkSession.builder \
    .appName("Lab06-Transformaciones-Logs") \
    .master("local[2]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Opción C: Aplicar filtros narrow ANTES del groupBy para reducir
# el volumen de datos que participa en el shuffle
df_reducido = df_enriched.filter(col("endpoint_section").isNotNull())
df_agg = df_reducido.groupBy("endpoint_section", "response_category").agg(...)
```

---

## 9. Limpieza del Entorno

Ejecuta las siguientes celdas al finalizar la práctica para liberar recursos:

```python
# Celda de limpieza — Paso 1: Liberar DataFrames en caché (si los usaste)
spark.catalog.clearCache()
print("Caché de Spark limpiado.")

# Celda de limpieza — Paso 2: Detener la SparkSession
spark.stop()
print("SparkSession detenida. Puerto 4040 liberado.")
```

```python
# Celda de limpieza — Paso 3 (opcional): Eliminar archivos generados
import shutil, os

archivos_a_limpiar = [
    "data/web_server_logs.csv",
    "data/web_logs_transformados"
]

for ruta in archivos_a_limpiar:
    if os.path.isfile(ruta):
        os.remove(ruta)
        print(f"Eliminado: {ruta}")
    elif os.path.isdir(ruta):
        shutil.rmtree(ruta)
        print(f"Directorio eliminado: {ruta}")

print("Limpieza completada.")
```

> **Nota:** Si planeas continuar con la **Práctica 6** inmediatamente, puedes conservar el archivo `data/web_logs_transformados/` ya que será utilizado como punto de partida en esa práctica.

---

## 10. Resumen

### Lo que aprendiste en esta práctica

En esta práctica construiste un pipeline de transformación completo sobre un dataset real de logs de servidor web con 150 000+ registros. Los conceptos y habilidades aplicadas fueron:

| Categoría | Funciones / Operaciones aplicadas |
|---|---|
| **Transformaciones Narrow** | `withColumn`, `filter`, `select`, `trim`, `upper`, `lower`, `coalesce`, `when` |
| **Transformaciones Wide** | `dropDuplicates`, `join` (con `broadcast`), `groupBy().agg()` |
| **Funciones de String** | `regexp_extract`, `concat_ws`, `substring`, `length`, `split` |
| **Funciones de Fecha** | `to_timestamp`, `to_date`, `date_format`, `hour`, `dayofweek`, `datediff`, `weekofyear` |
| **Funciones Matemáticas** | `round`, `ceil`, `floor`, `sqrt`, `log`, `spark_abs` |
| **Funciones de Array** | `array`, `array_distinct`, `array_contains`, `size` |
| **Funciones Condicionales** | `when().otherwise()`, `coalesce()` |
| **Method Chaining** | Pipeline completo en un único bloque encadenado |

### Diferencia clave entre Narrow y Wide

```
NARROW (sin shuffle):                    WIDE (con shuffle):
  withColumn  ─┐                           groupBy ──┐
  filter      ─┤─ Una sola Stage           join    ──┤─ Múltiples Stages
  select      ─┤   en el plan              distinct──┤   con Exchange
  union       ─┘                           repartition┘   en el plan
```

### Próximos pasos

- **Práctica 6:** Funciones de ventana (*window functions*) y UDFs para análisis avanzado sobre el dataset de logs transformado.
- **Práctica 7:** Optimización de rendimiento con caché, particionamiento estratégico y broadcast variables sobre pipelines complejos.

### Recursos adicionales

- [Documentación oficial PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Spark: The Definitive Guide — Capítulo 6: Working with Different Types of Data](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [Databricks: Narrow vs Wide Transformations](https://www.databricks.com/glossary/what-are-transformations)
- [PySpark Cheat Sheet — Funciones de transformación](https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf)

---
