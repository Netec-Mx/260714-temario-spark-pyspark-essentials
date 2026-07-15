# Uso de Acciones y Funciones Ejecutables en PySpark

## Metadatos

| Atributo         | Valor                                      |
|------------------|--------------------------------------------|
| **Duración**     | 45 minutos                                 |
| **Complejidad**  | Media                                      |
| **Nivel Bloom**  | Aplicar (Apply)                            |
| **Práctica**     | 6 de 9                                     |
| **Lab ID**       | 07-00-01                                   |

---

## Descripción General

Esta práctica se centra en el lado de la **ejecución** del modelo de programación de Spark: las **acciones** que desencadenan el procesamiento real sobre el grafo de transformaciones acumulado (DAG). Trabajarás con un dataset sintético de transacciones financieras para aplicar el conjunto completo de acciones disponibles en la DataFrame API y la RDD API, explorar la escritura de datos en múltiples formatos y medir el impacto computacional de cada acción mediante benchmarking con Python. Al finalizar, comprenderás cuándo usar cada acción de forma segura y eficiente en un pipeline de datos distribuido.

---

## Objetivos de Aprendizaje

Al completar esta práctica serás capaz de:

- [ ] Distinguir con precisión entre transformaciones (lazy) y acciones (eager) analizando el comportamiento del DAG en la Spark UI.
- [ ] Aplicar las principales acciones de la DataFrame API (`show`, `collect`, `take`, `first`, `count`, `describe`, `summary`, `toPandas`, `toLocalIterator`) y de la RDD API (`reduce`, `fold`, `aggregate`, `countByValue`, `foreach`).
- [ ] Escribir datos en múltiples formatos de almacenamiento (CSV, JSON, Parquet, ORC) utilizando la Writer API con distintos `SaveMode`.
- [ ] Medir y comparar el costo computacional de diferentes acciones usando el módulo `time` de Python y el plan de ejecución de `explain()`.
- [ ] Implementar `foreach()` y `foreachPartition()` para procesamiento distribuido personalizado sin mover datos al driver.

---

## Prerrequisitos

### Conocimiento Previo

| Requisito | Descripción |
|-----------|-------------|
| Práctica 5 completada | Dominio de funciones de transformación (`filter`, `groupBy`, `agg`, `join`, `withColumn`) |
| Lazy evaluation | Comprensión del modelo de evaluación perezosa y el DAG de Spark |
| Formatos de datos | Familiaridad con CSV, JSON, Parquet y sus diferencias estructurales |
| Particiones | Concepto de particiones en sistemas distribuidos y su relación con el paralelismo |

### Acceso y Recursos

| Recurso | Detalle |
|---------|---------|
| Entorno Jupyter | JupyterLab 7.x operativo con kernel PySpark |
| Dataset de práctica | `transacciones_financieras.csv` (≥ 500 K filas) — ver Sección 5 |
| Puerto 4040 disponible | Para acceder a la Spark UI durante la práctica |
| Directorio de salida | `~/spark-labs/lab06/output/` con permisos de escritura |

---

## Entorno de Laboratorio

### Especificaciones de Hardware Recomendadas

| Componente | Mínimo | Recomendado |
|------------|--------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 núcleos (64 bits, VT-x/AMD-V) | 6–8 núcleos |
| Almacenamiento libre | 20 GB | 30 GB |

### Stack de Software

| Componente | Versión |
|------------|---------|
| Java (JDK) | 11 o 17 LTS |
| Apache Spark | 3.5.x |
| Python | 3.10 o 3.11 |
| PySpark | 3.5.x |
| JupyterLab | 7.x |
| pandas | 2.0+ |
| NumPy | 1.24+ |
| findspark | 2.0.1+ |

### Preparación del Entorno

Ejecuta los siguientes comandos en una terminal **antes** de abrir JupyterLab para verificar que el entorno está correctamente configurado:

```bash
# Verificar versión de Java
java -version
# Salida esperada: openjdk version "11.x.x" o "17.x.x"

# Verificar versión de Python
python --version
# Salida esperada: Python 3.10.x o 3.11.x

# Verificar instalación de PySpark
python -c "import pyspark; print(pyspark.__version__)"
# Salida esperada: 3.5.x

# Crear estructura de directorios para el laboratorio
mkdir -p ~/spark-labs/lab06/{data,output,notebooks}
cd ~/spark-labs/lab06
```

### Generación del Dataset de Práctica

Si no dispones del dataset distribuido por el instructor, ejecuta el siguiente script para generarlo. Abre una terminal y ejecuta:

```bash
cd ~/spark-labs/lab06/data
python3 - <<'EOF'
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

np.random.seed(42)
random.seed(42)

n = 600_000
regiones = ["Norte", "Sur", "Este", "Oeste", "Centro", "Internacional"]
tipos = ["Compra", "Venta", "Transferencia", "Retiro", "Depósito"]
monedas = ["USD", "EUR", "MXN", "COP", "ARS"]
canales = ["Online", "Sucursal", "ATM", "Móvil", "Telefónico"]

fechas_base = datetime(2022, 1, 1)
fechas = [fechas_base + timedelta(days=random.randint(0, 730)) for _ in range(n)]

df = pd.DataFrame({
    "id_transaccion": range(1, n + 1),
    "fecha": [f.strftime("%Y-%m-%d") for f in fechas],
    "region": np.random.choice(regiones, n),
    "tipo_transaccion": np.random.choice(tipos, n),
    "monto": np.round(np.random.exponential(scale=500, size=n), 2),
    "moneda": np.random.choice(monedas, n),
    "canal": np.random.choice(canales, n),
    "id_cliente": np.random.randint(1000, 9999, n),
    "aprobada": np.random.choice([True, False], n, p=[0.85, 0.15]),
    "comision": np.round(np.random.uniform(0, 50, n), 2)
})

df.to_csv("transacciones_financieras.csv", index=False)
print(f"Dataset generado: {len(df):,} filas, {len(df.columns)} columnas")
print(f"Tamaño aproximado: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB en memoria")
EOF
```

**Salida esperada:**
```
Dataset generado: 600,000 filas, 10 columnas
Tamaño aproximado: ~45 MB en memoria
```

---

## Pasos del Laboratorio

### Paso 1: Inicialización de la SparkSession y Carga del Dataset

**Objetivo:** Configurar la SparkSession con parámetros adecuados para el laboratorio y cargar el dataset de transacciones financieras, verificando su estructura antes de ejecutar cualquier acción.

#### Instrucciones

1. Abre JupyterLab y crea un nuevo notebook llamado `Lab06_Acciones_PySpark.ipynb` en `~/spark-labs/lab06/notebooks/`.

2. En la primera celda, importa las dependencias y configura la SparkSession:

```python
# Celda 1 — Importaciones y configuración de SparkSession
import time
import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DateType

# Configurar SparkSession con parámetros para entorno local
spark = SparkSession.builder \
    .appName("Lab06-Acciones-PySpark") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Configurar nivel de log para reducir verbosidad
spark.sparkContext.setLogLevel("WARN")

print(f"✅ SparkSession iniciada correctamente")
print(f"   Versión de Spark: {spark.version}")
print(f"   Master: {spark.sparkContext.master}")
print(f"   App ID: {spark.sparkContext.applicationId}")
print(f"   Spark UI disponible en: http://localhost:4040")
```

3. En la segunda celda, define la ruta del dataset y cárgalo con esquema explícito:

```python
# Celda 2 — Carga del dataset con esquema explícito
DATA_PATH = os.path.expanduser("~/spark-labs/lab06/data/transacciones_financieras.csv")
OUTPUT_PATH = os.path.expanduser("~/spark-labs/lab06/output")

# Definir esquema explícito (mejor práctica: evita inferSchema en producción)
schema = StructType([
    StructField("id_transaccion",   IntegerType(), nullable=False),
    StructField("fecha",            StringType(),  nullable=True),
    StructField("region",           StringType(),  nullable=True),
    StructField("tipo_transaccion", StringType(),  nullable=True),
    StructField("monto",            DoubleType(),  nullable=True),
    StructField("moneda",           StringType(),  nullable=True),
    StructField("canal",            StringType(),  nullable=True),
    StructField("id_cliente",       IntegerType(), nullable=True),
    StructField("aprobada",         BooleanType(), nullable=True),
    StructField("comision",         DoubleType(),  nullable=True)
])

# Cargar CSV — ESTO ES UNA TRANSFORMACIÓN, no una acción
df_tx = spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv(DATA_PATH)

# Verificar que el DataFrame se creó (sin ejecutar el plan)
print(f"✅ DataFrame creado (plan registrado, datos NO leídos aún)")
print(f"   Esquema del DataFrame:")
df_tx.printSchema()
```

4. Observa que `printSchema()` **no** es una acción de datos: no lee el CSV. El esquema proviene de la definición que proporcionamos.

#### Salida Esperada

```
✅ DataFrame creado (plan registrado, datos NO leídos aún)
   Esquema del DataFrame:
root
 |-- id_transaccion: integer (nullable = false)
 |-- fecha: string (nullable = true)
 |-- region: string (nullable = true)
 |-- tipo_transaccion: string (nullable = true)
 |-- monto: double (nullable = true)
 |-- moneda: string (nullable = true)
 |-- canal: string (nullable = true)
 |-- id_cliente: integer (nullable = true)
 |-- aprobada: boolean (nullable = true)
 |-- comision: double (nullable = true)
```

#### Verificación

Abre `http://localhost:4040` en tu navegador. En la pestaña **Jobs**, confirma que **no hay ningún job ejecutado** todavía. Esto confirma que la carga con esquema explícito y `printSchema()` no dispararon ninguna acción.

---

### Paso 2: Acciones de Inspección y Exploración de Datos

**Objetivo:** Aplicar las acciones de inspección (`show`, `count`, `describe`, `summary`, `first`, `take`) y analizar las diferencias en su comportamiento, tipo de retorno y costo computacional.

#### Instrucciones

1. Aplica `show()` para una primera inspección visual:

```python
# Celda 3 — Acción show(): inspección visual (NO devuelve objeto Python)
print("=" * 60)
print("ACCIÓN 1: show() — Inspección visual en consola")
print("=" * 60)

t_inicio = time.time()
df_tx.show(5, truncate=False)
t_fin = time.time()

print(f"\n⏱️  Tiempo de ejecución de show(5): {t_fin - t_inicio:.3f} segundos")
print(f"📌 Tipo de retorno: {type(df_tx.show(1))}")  # Devuelve None
```

2. Aplica `count()` y mide su tiempo:

```python
# Celda 4 — Acción count(): conteo total de filas
print("=" * 60)
print("ACCIÓN 2: count() — Conteo total de filas")
print("=" * 60)

t_inicio = time.time()
total_filas = df_tx.count()
t_fin = time.time()

print(f"\n✅ Total de transacciones en el dataset: {total_filas:,}")
print(f"⏱️  Tiempo de ejecución: {t_fin - t_inicio:.3f} segundos")
print(f"📌 Tipo de retorno: {type(total_filas)} — valor escalar en el driver")
```

3. Aplica `first()` y `take()` y compara sus tipos de retorno:

```python
# Celda 5 — Acciones first() y take()
print("=" * 60)
print("ACCIÓN 3: first() y take() — Recolección parcial")
print("=" * 60)

# first() — devuelve un objeto Row
primera_fila = df_tx.first()
print(f"\nfirst() devuelve tipo: {type(primera_fila)}")
print(f"id_transaccion: {primera_fila['id_transaccion']}")
print(f"region:         {primera_fila['region']}")
print(f"monto:          {primera_fila['monto']}")

# take(n) — devuelve una lista de Row
cinco_filas = df_tx.take(5)
print(f"\ntake(5) devuelve tipo: {type(cinco_filas)}")
print(f"Número de elementos: {len(cinco_filas)}")
print("\nMontos de las primeras 5 transacciones:")
for i, fila in enumerate(cinco_filas, 1):
    print(f"  [{i}] id={fila['id_transaccion']:>7} | region={fila['region']:<15} | monto={fila['monto']:>10.2f} | aprobada={fila['aprobada']}")
```

4. Aplica `describe()` y `summary()` para estadísticas descriptivas:

```python
# Celda 6 — Acciones describe() y summary()
print("=" * 60)
print("ACCIÓN 4: describe() — Estadísticas básicas")
print("=" * 60)

# describe() calcula count, mean, stddev, min, max
t_inicio = time.time()
df_tx.select("monto", "comision", "id_cliente").describe().show()
t_fin = time.time()
print(f"⏱️  Tiempo describe(): {t_fin - t_inicio:.3f} segundos")

print("\n" + "=" * 60)
print("ACCIÓN 5: summary() — Estadísticas extendidas con percentiles")
print("=" * 60)

t_inicio = time.time()
df_tx.select("monto", "comision").summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
).show()
t_fin = time.time()
print(f"⏱️  Tiempo summary(): {t_fin - t_inicio:.3f} segundos")
```

5. Abre la Spark UI (`http://localhost:4040`) y observa la pestaña **Jobs**. Cada acción ejecutada hasta ahora debe aparecer como un job independiente.

#### Salida Esperada

```
ACCIÓN 2: count() — Conteo total de filas
✅ Total de transacciones en el dataset: 600,000
⏱️  Tiempo de ejecución: 1.8xx segundos

ACCIÓN 3: first() y take() — Recolección parcial
first() devuelve tipo: <class 'pyspark.sql.types.Row'>
take(5) devuelve tipo: <class 'list'>
Número de elementos: 5
```

#### Verificación

En la Spark UI → **Jobs**, deberías ver al menos 5 jobs completados (uno por cada acción ejecutada). Haz clic en cualquier job y verifica la pestaña **Stages** para observar cómo se dividió el trabajo en tareas paralelas.

---

### Paso 3: Acción `collect()` vs `toPandas()` — Análisis de Riesgo

**Objetivo:** Comprender el riesgo de traer datos masivos al driver con `collect()` y `toPandas()`, y aplicar el patrón seguro de filtrar/agregar antes de recolectar.

#### Instrucciones

1. Demuestra el patrón **inseguro** con un subconjunto controlado (simulando lo que ocurriría con el dataset completo):

```python
# Celda 7 — Patrón INSEGURO vs SEGURO con collect()
print("=" * 60)
print("DEMOSTRACIÓN: Riesgo de collect() con datos masivos")
print("=" * 60)

# ⚠️ PATRÓN INSEGURO: collect() sobre el dataset completo
# En producción con millones de filas esto puede crashear el driver
# Lo ejecutamos aquí porque nuestro dataset cabe en memoria, 
# pero medimos el costo para entender el impacto

print("\n⚠️  PATRÓN INSEGURO: collect() sobre 600K filas")
t_inicio = time.time()
todas_las_filas = df_tx.collect()  # Trae 600K Row objects al driver
t_fin = time.time()
print(f"   Filas recolectadas: {len(todas_las_filas):,}")
print(f"   Tiempo: {t_fin - t_inicio:.3f} segundos")
print(f"   Memoria estimada en driver: ~{len(todas_las_filas) * 200 / 1024**2:.1f} MB (aprox)")

# ✅ PATRÓN SEGURO: Agregar primero, luego collect()
print("\n✅ PATRÓN SEGURO: Agregar primero, luego collect()")
t_inicio = time.time()
resumen_por_region = df_tx \
    .filter(F.col("aprobada") == True) \
    .groupBy("region") \
    .agg(
        F.sum("monto").alias("total_monto"),
        F.count("*").alias("num_transacciones"),
        F.avg("comision").alias("comision_promedio")
    ) \
    .orderBy(F.col("total_monto").desc()) \
    .collect()  # Solo 6 filas (una por región)
t_fin = time.time()

print(f"   Filas recolectadas: {len(resumen_por_region)}")
print(f"   Tiempo: {t_fin - t_inicio:.3f} segundos")
print("\n   Resultados del resumen:")
for fila in resumen_por_region:
    print(f"   {fila['region']:<15} | Total: {fila['total_monto']:>15,.2f} | Tx: {fila['num_transacciones']:>7,} | Comisión prom: {fila['comision_promedio']:>6.2f}")
```

2. Compara `collect()` con `toPandas()` para análisis con Pandas:

```python
# Celda 8 — toPandas() para integración con el ecosistema Python
print("=" * 60)
print("ACCIÓN: toPandas() — Integración con Pandas")
print("=" * 60)

# Preparar un DataFrame agregado pequeño antes de convertir
df_resumen = df_tx \
    .groupBy("region", "tipo_transaccion") \
    .agg(
        F.sum("monto").alias("total_monto"),
        F.count("*").alias("num_transacciones")
    )

t_inicio = time.time()
pdf_resumen = df_resumen.toPandas()
t_fin = time.time()

print(f"✅ DataFrame de Spark convertido a Pandas")
print(f"   Tipo resultante: {type(pdf_resumen)}")
print(f"   Forma (filas, columnas): {pdf_resumen.shape}")
print(f"   Tiempo toPandas(): {t_fin - t_inicio:.3f} segundos")
print(f"\n   Primeras 5 filas del DataFrame Pandas:")
print(pdf_resumen.head())

# Usar Pandas para visualización o análisis adicional
print(f"\n   Estadísticas rápidas con Pandas:")
print(pdf_resumen["total_monto"].describe())
```

#### Salida Esperada

```
⚠️  PATRÓN INSEGURO: collect() sobre 600K filas
   Filas recolectadas: 600,000
   Tiempo: 3.5xx segundos

✅ PATRÓN SEGURO: Agregar primero, luego collect()
   Filas recolectadas: 6
   Tiempo: 1.2xx segundos
```

#### Verificación

Compara los tiempos registrados. El `collect()` sobre el dataset completo debe ser significativamente más lento que el `collect()` sobre el resultado agregado (6 filas). Este contraste ilustra el principio fundamental: **reducir el volumen antes de recolectar**.

---

### Paso 4: Acciones sobre RDDs — `reduce`, `fold`, `aggregate` y `countByValue`

**Objetivo:** Aplicar las acciones especializadas de la RDD API para cómputos distribuidos que requieren lógica de combinación personalizada.

#### Instrucciones

1. Obtén el SparkContext y crea RDDs de trabajo desde el DataFrame existente:

```python
# Celda 9 — Acciones RDD: reduce, fold, aggregate
print("=" * 60)
print("ACCIONES RDD: reduce, fold, aggregate, countByValue")
print("=" * 60)

sc = spark.sparkContext

# Crear RDD de montos de transacciones aprobadas
rdd_montos = df_tx \
    .filter(F.col("aprobada") == True) \
    .select("monto") \
    .rdd \
    .map(lambda row: row["monto"])

print(f"✅ RDD de montos creado")
print(f"   Particiones: {rdd_montos.getNumPartitions()}")
```

2. Aplica `reduce()` para suma total:

```python
# Celda 10 — reduce()
print("\n--- ACCIÓN: reduce() ---")
t_inicio = time.time()
suma_total = rdd_montos.reduce(lambda a, b: a + b)
t_fin = time.time()
print(f"Suma total de montos aprobados: {suma_total:,.2f}")
print(f"⏱️  Tiempo: {t_fin - t_inicio:.3f} segundos")
```

3. Aplica `fold()` con valor inicial:

```python
# Celda 11 — fold()
print("\n--- ACCIÓN: fold() ---")
# fold() requiere un valor neutro (identidad de la operación)
# Para suma, el valor neutro es 0
t_inicio = time.time()
suma_con_fold = rdd_montos.fold(0.0, lambda a, b: a + b)
t_fin = time.time()
print(f"Suma con fold(): {suma_con_fold:,.2f}")
print(f"⏱️  Tiempo: {t_fin - t_inicio:.3f} segundos")
print(f"✅ Resultado idéntico a reduce(): {abs(suma_total - suma_con_fold) < 0.01}")
```

4. Aplica `aggregate()` para calcular suma y conteo simultáneamente (promedio en una sola pasada):

```python
# Celda 12 — aggregate() — el más poderoso de los tres
print("\n--- ACCIÓN: aggregate() ---")
print("Objetivo: calcular promedio, mínimo y máximo en una sola pasada")

# zeroValue: (suma, conteo, mínimo, máximo)
zero = (0.0, 0, float('inf'), float('-inf'))

def seq_op(acc, valor):
    """Combina un acumulador con un nuevo valor dentro de una partición."""
    return (
        acc[0] + valor,                      # suma acumulada
        acc[1] + 1,                          # conteo
        min(acc[2], valor),                  # mínimo
        max(acc[3], valor)                   # máximo
    )

def comb_op(acc1, acc2):
    """Combina dos acumuladores de distintas particiones."""
    return (
        acc1[0] + acc2[0],                   # sumas
        acc1[1] + acc2[1],                   # conteos
        min(acc1[2], acc2[2]),               # mínimo global
        max(acc1[3], acc2[3])                # máximo global
    )

t_inicio = time.time()
resultado = rdd_montos.aggregate(zero, seq_op, comb_op)
t_fin = time.time()

suma, conteo, minimo, maximo = resultado
promedio = suma / conteo

print(f"\n  Resultados calculados con aggregate():")
print(f"  ├─ Suma total:  {suma:>15,.2f}")
print(f"  ├─ Conteo:      {conteo:>15,}")
print(f"  ├─ Promedio:    {promedio:>15,.2f}")
print(f"  ├─ Mínimo:      {minimo:>15,.2f}")
print(f"  └─ Máximo:      {maximo:>15,.2f}")
print(f"\n⏱️  Tiempo aggregate(): {t_fin - t_inicio:.3f} segundos")
```

5. Aplica `countByValue()` para contar frecuencias:

```python
# Celda 13 — countByValue()
print("\n--- ACCIÓN: countByValue() ---")

rdd_regiones = df_tx.select("region").rdd.map(lambda r: r["region"])

t_inicio = time.time()
conteo_por_region = rdd_regiones.countByValue()
t_fin = time.time()

print(f"Distribución de transacciones por región:")
for region, conteo in sorted(conteo_por_region.items(), key=lambda x: -x[1]):
    barra = "█" * (conteo // 10000)
    print(f"  {region:<15} | {conteo:>7,} | {barra}")
print(f"\n⏱️  Tiempo countByValue(): {t_fin - t_inicio:.3f} segundos")
print(f"📌 Tipo de retorno: {type(conteo_por_region)}")
```

#### Salida Esperada

```
  Resultados calculados con aggregate():
  ├─ Suma total:    xxx,xxx,xxx.xx
  ├─ Conteo:            510,xxx
  ├─ Promedio:              xxx.xx
  ├─ Mínimo:                  0.00
  └─ Máximo:              x,xxx.xx
```

#### Verificación

Compara el promedio calculado con `aggregate()` contra el resultado de `df_tx.filter(F.col("aprobada")==True).select(F.avg("monto")).first()[0]`. Los valores deben ser iguales (diferencia < 0.01 por precisión de punto flotante).

---

### Paso 5: Escritura de Datos con la Writer API — Múltiples Formatos y SaveModes

**Objetivo:** Utilizar la DataFrame Writer API para persistir datos en formatos CSV, JSON, Parquet y ORC, aplicando los cuatro modos de escritura disponibles y verificando los archivos generados.

#### Instrucciones

1. Prepara el DataFrame de resultados que será escrito en distintos formatos:

```python
# Celda 14 — Preparar DataFrame de resultados para escritura
print("=" * 60)
print("WRITER API: Escritura en múltiples formatos")
print("=" * 60)

# DataFrame de resumen por región, tipo y canal (resultado analítico)
df_para_escribir = df_tx \
    .filter(F.col("aprobada") == True) \
    .groupBy("region", "tipo_transaccion", "canal") \
    .agg(
        F.round(F.sum("monto"), 2).alias("monto_total"),
        F.count("*").alias("num_transacciones"),
        F.round(F.avg("comision"), 4).alias("comision_promedio"),
        F.round(F.max("monto"), 2).alias("monto_maximo")
    ) \
    .orderBy("region", "tipo_transaccion")

# Aplicar caché porque escribiremos el mismo DataFrame varias veces
df_para_escribir.cache()

# Forzar materialización del caché con count()
total_registros = df_para_escribir.count()
print(f"✅ DataFrame de resultados preparado y cacheado")
print(f"   Registros a escribir: {total_registros}")
df_para_escribir.show(5)
```

2. Escribe en formato CSV:

```python
# Celda 15 — Escritura en CSV
print("\n--- ESCRITURA: Formato CSV ---")

ruta_csv = f"{OUTPUT_PATH}/resultados_csv"

t_inicio = time.time()
df_para_escribir.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("sep", ",") \
    .option("encoding", "UTF-8") \
    .csv(ruta_csv)
t_fin = time.time()

# Verificar archivos generados
archivos_csv = [f for f in os.listdir(ruta_csv) if f.endswith(".csv")]
print(f"✅ CSV escrito en: {ruta_csv}")
print(f"   Archivos generados: {len(archivos_csv)} (uno por partición)")
print(f"   Nombres: {archivos_csv[:3]}...")
print(f"⏱️  Tiempo escritura CSV: {t_fin - t_inicio:.3f} segundos")
```

3. Escribe en formato JSON:

```python
# Celda 16 — Escritura en JSON
print("\n--- ESCRITURA: Formato JSON ---")

ruta_json = f"{OUTPUT_PATH}/resultados_json"

t_inicio = time.time()
df_para_escribir.write \
    .mode("overwrite") \
    .json(ruta_json)
t_fin = time.time()

archivos_json = [f for f in os.listdir(ruta_json) if f.endswith(".json")]
print(f"✅ JSON escrito en: {ruta_json}")
print(f"   Archivos generados: {len(archivos_json)}")
print(f"⏱️  Tiempo escritura JSON: {t_fin - t_inicio:.3f} segundos")

# Leer de vuelta para verificar integridad
df_json_verificacion = spark.read.json(ruta_json)
print(f"   Filas leídas de vuelta: {df_json_verificacion.count():,} (debe ser {total_registros})")
```

4. Escribe en formato Parquet (formato columnar recomendado):

```python
# Celda 17 — Escritura en Parquet
print("\n--- ESCRITURA: Formato Parquet (recomendado para producción) ---")

ruta_parquet = f"{OUTPUT_PATH}/resultados_parquet"

t_inicio = time.time()
df_para_escribir.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(ruta_parquet)
t_fin = time.time()

archivos_parquet = [f for f in os.listdir(ruta_parquet) if f.endswith(".parquet")]
print(f"✅ Parquet escrito en: {ruta_parquet}")
print(f"   Archivos generados: {len(archivos_parquet)}")
print(f"⏱️  Tiempo escritura Parquet: {t_fin - t_inicio:.3f} segundos")

# Comparar tamaños de archivo
import glob
size_csv     = sum(os.path.getsize(f) for f in glob.glob(f"{ruta_csv}/*.csv"))
size_json    = sum(os.path.getsize(f) for f in glob.glob(f"{ruta_json}/*.json"))
size_parquet = sum(os.path.getsize(f) for f in glob.glob(f"{ruta_parquet}/*.parquet"))

print(f"\n📊 Comparación de tamaños de archivo:")
print(f"   CSV:     {size_csv/1024:.1f} KB")
print(f"   JSON:    {size_json/1024:.1f} KB")
print(f"   Parquet: {size_parquet/1024:.1f} KB  ← Más compacto")
```

5. Demuestra los cuatro modos de escritura (`SaveMode`):

```python
# Celda 18 — Demostración de SaveModes
print("\n--- SAVE MODES: overwrite, append, ignore, error ---")

ruta_test_modes = f"{OUTPUT_PATH}/test_savemodes"

# 1. overwrite — reemplaza si existe
df_para_escribir.limit(10).write.mode("overwrite").parquet(f"{ruta_test_modes}/overwrite")
print("✅ mode('overwrite') — Directorio reemplazado si existía")

# 2. append — agrega datos al destino existente
df_para_escribir.limit(5).write.mode("append").parquet(f"{ruta_test_modes}/overwrite")
filas_tras_append = spark.read.parquet(f"{ruta_test_modes}/overwrite").count()
print(f"✅ mode('append')    — Filas totales tras append: {filas_tras_append} (10 + 5 = 15)")

# 3. ignore — no hace nada si el destino existe
df_para_escribir.limit(100).write.mode("ignore").parquet(f"{ruta_test_modes}/overwrite")
filas_tras_ignore = spark.read.parquet(f"{ruta_test_modes}/overwrite").count()
print(f"✅ mode('ignore')    — Filas totales sin cambio: {filas_tras_ignore} (sigue siendo 15)")

# 4. error (default) — lanza excepción si el destino existe
print("ℹ️  mode('error')     — Lanzaría AnalysisException si el destino existe (comportamiento por defecto)")
```

#### Salida Esperada

```
📊 Comparación de tamaños de archivo:
   CSV:     xxx.x KB
   JSON:    xxx.x KB
   Parquet: xx.x KB  ← Más compacto
```

#### Verificación

Verifica que el directorio `~/spark-labs/lab06/output/` contiene los subdirectorios `resultados_csv`, `resultados_json` y `resultados_parquet`. Confirma que Parquet ocupa significativamente menos espacio que CSV o JSON para el mismo contenido.

---

### Paso 6: Análisis del Plan de Ejecución con `explain()`

**Objetivo:** Interpretar el plan de ejecución lógico y físico generado por el Catalyst Optimizer utilizando `explain()` en modo simple y extendido.

#### Instrucciones

1. Construye una consulta compleja y analiza su plan:

```python
# Celda 19 — explain() en modo simple
print("=" * 60)
print("ANÁLISIS DE PLAN DE EJECUCIÓN: explain()")
print("=" * 60)

# Consulta compleja: filtro + join simulado + agregación + ordenamiento
df_aprobadas = df_tx.filter(F.col("aprobada") == True)
df_rechazadas = df_tx.filter(F.col("aprobada") == False)

# Contar rechazadas por región para un join
df_rechazo_region = df_rechazadas \
    .groupBy("region") \
    .agg(F.count("*").alias("rechazadas"))

# Join con aprobadas y cálculo de tasa de aprobación
df_analisis = df_aprobadas \
    .groupBy("region") \
    .agg(F.count("*").alias("aprobadas")) \
    .join(df_rechazo_region, on="region", how="left") \
    .withColumn("tasa_aprobacion", 
                F.round(F.col("aprobadas") / (F.col("aprobadas") + F.col("rechazadas")) * 100, 2)) \
    .orderBy(F.col("tasa_aprobacion").desc())

print("\n🔍 PLAN SIMPLE (explain() sin argumentos):")
print("-" * 60)
df_analisis.explain()

print("\n🔍 PLAN EXTENDIDO (explain('extended')):")
print("-" * 60)
df_analisis.explain("extended")
```

2. Analiza el plan con formato `cost` y `codegen`:

```python
# Celda 20 — explain() modos adicionales
print("\n🔍 PLAN CON COSTOS ESTIMADOS (explain('cost')):")
print("-" * 60)
df_analisis.explain("cost")

print("\n🔍 PLAN FORMATEADO (explain('formatted')):")
print("-" * 60)
df_analisis.explain("formatted")
```

3. Ejecuta la consulta y observa el resultado:

```python
# Celda 21 — Ejecutar la consulta y observar resultado
print("\n🚀 EJECUTANDO LA CONSULTA (acción: show):")
print("-" * 60)
df_analisis.show(truncate=False)

print("\n📌 Observaciones del plan de ejecución:")
print("  • El Catalyst Optimizer reordena las operaciones para eficiencia")
print("  • Los 'Exchange' en el plan físico indican shuffles de datos entre particiones")
print("  • Los 'Sort' después de 'Exchange' son necesarios para el join ordenado")
print("  • El 'BroadcastHashJoin' aparece cuando una tabla es pequeña (< spark.sql.autoBroadcastJoinThreshold)")
```

#### Salida Esperada

El plan físico mostrará nodos como `*(1) Filter`, `Exchange hashpartitioning`, `*(2) HashAggregate`, y posiblemente `BroadcastHashJoin` o `SortMergeJoin` dependiendo del tamaño de los DataFrames.

#### Verificación

En la Spark UI → **SQL/DataFrame** tab, localiza la consulta ejecutada y haz clic en ella para ver el plan de ejecución visual con estadísticas de filas procesadas en cada nodo. Compara el plan visual con la salida de `explain()`.

---

### Paso 7: `foreach()` y `foreachPartition()` — Procesamiento Distribuido Personalizado

**Objetivo:** Implementar `foreach()` y `foreachPartition()` para ejecutar lógica personalizada de forma distribuida, entendiendo la diferencia de eficiencia entre ambas cuando se trabaja con conexiones a sistemas externos.

#### Instrucciones

1. Implementa `foreach()` para procesamiento elemento a elemento:

```python
# Celda 22 — foreach() sobre DataFrame
print("=" * 60)
print("ACCIÓN: foreach() — Procesamiento distribuido sin retorno al driver")
print("=" * 60)

# Simular envío de alertas para transacciones de alto valor
# En producción, aquí se conectaría a un sistema de mensajería (Kafka, SQS, etc.)
df_alto_valor = df_tx \
    .filter((F.col("monto") > 2000) & (F.col("aprobada") == True)) \
    .select("id_transaccion", "region", "monto", "tipo_transaccion") \
    .limit(20)  # Limitamos para no saturar la salida

print(f"Transacciones de alto valor a procesar: {df_alto_valor.count()}")

# Contador de alertas (nota: print() en foreach se ejecuta en el ejecutor,
# puede no aparecer en el driver en modo clúster real)
def procesar_transaccion(fila):
    """Simula el envío de una alerta para transacciones de alto valor."""
    mensaje = (
        f"[ALERTA] TX#{fila['id_transaccion']} | "
        f"Región: {fila['region']} | "
        f"Monto: {fila['monto']:.2f} | "
        f"Tipo: {fila['tipo_transaccion']}"
    )
    # En modo local, print() es visible; en clúster real, iría a logs del ejecutor
    print(mensaje)

print("\nEjecutando foreach() — salida en logs del ejecutor:")
df_alto_valor.foreach(procesar_transaccion)
print("✅ foreach() completado")
```

2. Implementa `foreachPartition()` para procesamiento eficiente con conexiones por lotes:

```python
# Celda 23 — foreachPartition() — patrón eficiente para conexiones externas
print("=" * 60)
print("ACCIÓN: foreachPartition() — Patrón eficiente para conexiones externas")
print("=" * 60)

print("""
📌 DIFERENCIA CLAVE:
   foreach():           abre una conexión por cada fila → N conexiones
   foreachPartition():  abre UNA conexión por partición → P conexiones (P << N)
   
   Con 600,000 filas y 8 particiones:
   - foreach():          600,000 conexiones (ineficiente)
   - foreachPartition():       8 conexiones (eficiente)
""")

# Preparar DataFrame de transacciones rechazadas para "notificar"
df_rechazadas_muestra = df_tx \
    .filter(F.col("aprobada") == False) \
    .select("id_transaccion", "region", "monto") \
    .repartition(4)  # Forzar 4 particiones para el ejemplo

# Contadores para verificar el patrón
from pyspark import AccumulatorParam

contador_particiones = sc.accumulator(0)
contador_registros   = sc.accumulator(0)

def procesar_particion(iterador):
    """
    Simula el procesamiento por lotes de una partición completa.
    En producción: abrir conexión BD aquí, insertar todos los registros,
    cerrar conexión al final.
    """
    # Simular apertura de conexión (una vez por partición)
    contador_particiones.add(1)
    registros_en_particion = 0
    
    # Simular inserción en lote
    lote = []
    for fila in iterador:
        lote.append({
            "id": fila["id_transaccion"],
            "region": fila["region"],
            "monto": fila["monto"]
        })
        registros_en_particion += 1
    
    contador_registros.add(registros_en_particion)
    # Simular cierre de conexión (una vez por partición)

t_inicio = time.time()
df_rechazadas_muestra.foreachPartition(procesar_particion)
t_fin = time.time()

print(f"✅ foreachPartition() completado")
print(f"   Particiones procesadas:   {contador_particiones.value}")
print(f"   Registros procesados:     {contador_registros.value:,}")
print(f"   Conexiones abiertas:      {contador_particiones.value} (vs {contador_registros.value:,} con foreach)")
print(f"⏱️  Tiempo: {t_fin - t_inicio:.3f} segundos")
```

#### Salida Esperada

```
✅ foreachPartition() completado
   Particiones procesadas:   4
   Registros procesados:     xx,xxx
   Conexiones abiertas:      4 (vs xx,xxx con foreach)
```

#### Verificación

Confirma que `contador_particiones.value == 4` (igual al número de particiones definido con `repartition(4)`). Esto demuestra que `foreachPartition()` abre exactamente una "conexión" por partición, independientemente del número de registros.

---

### Paso 8: Benchmarking Comparativo de Acciones

**Objetivo:** Medir y comparar el costo computacional de las principales acciones para desarrollar intuición sobre cuáles son más costosas y por qué.

#### Instrucciones

1. Ejecuta el benchmark completo:

```python
# Celda 24 — Benchmark comparativo de acciones
print("=" * 60)
print("BENCHMARK: Comparación de costo computacional de acciones")
print("=" * 60)

resultados_benchmark = []

def medir_accion(nombre, funcion):
    """Ejecuta una función y registra su tiempo de ejecución."""
    t_inicio = time.time()
    resultado = funcion()
    t_fin = time.time()
    duracion = t_fin - t_inicio
    resultados_benchmark.append({"accion": nombre, "tiempo_seg": round(duracion, 3)})
    print(f"  ✅ {nombre:<35} → {duracion:.3f}s")
    return resultado

print("\nEjecutando benchmarks (puede tomar 1-2 minutos)...\n")

# Acciones sobre el DataFrame completo (sin caché)
medir_accion("count() — sin caché",          lambda: df_tx.count())
medir_accion("first() — sin caché",          lambda: df_tx.first())
medir_accion("take(10) — sin caché",         lambda: df_tx.take(10))
medir_accion("show(5) — sin caché",          lambda: df_tx.show(5))
medir_accion("describe() — 3 columnas",      lambda: df_tx.select("monto","comision").describe().collect())

# Aplicar caché y repetir
df_tx.cache()
df_tx.count()  # Forzar materialización del caché
print("\n  [caché activado — segunda pasada]\n")

medir_accion("count() — CON caché",          lambda: df_tx.count())
medir_accion("first() — CON caché",          lambda: df_tx.first())
medir_accion("take(10) — CON caché",         lambda: df_tx.take(10))

# Acciones de escritura
medir_accion("write.parquet() overwrite",    
             lambda: df_para_escribir.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/bench_parquet"))
medir_accion("write.csv() overwrite",        
             lambda: df_para_escribir.write.mode("overwrite").option("header",True).csv(f"{OUTPUT_PATH}/bench_csv"))
medir_accion("toPandas() — df agregado",     lambda: df_para_escribir.toPandas())

# Mostrar tabla de resultados
print("\n" + "=" * 60)
print("RESULTADOS DEL BENCHMARK")
print("=" * 60)
print(f"{'Acción':<40} {'Tiempo (s)':>10}")
print("-" * 52)
for r in sorted(resultados_benchmark, key=lambda x: x["tiempo_seg"], reverse=True):
    barra = "▓" * int(r["tiempo_seg"] * 5)
    print(f"{r['accion']:<40} {r['tiempo_seg']:>10.3f}  {barra}")
```

2. Libera el caché al terminar el benchmark:

```python
# Celda 25 — Liberar caché
df_tx.unpersist()
df_para_escribir.unpersist()
print("✅ Caché liberado")
```

#### Salida Esperada

La tabla de benchmark mostrará que:
- Las acciones **con caché** son significativamente más rápidas que sin él.
- `write.csv()` es más lento que `write.parquet()` por la serialización de texto.
- `count()` es más rápido que `describe()` porque no requiere calcular estadísticas.

#### Verificación

Compara el tiempo de `count()` sin caché vs con caché. La diferencia debe ser notable (típicamente 2x–5x más rápido con caché en modo local). Este resultado justifica el uso de `cache()` cuando el mismo DataFrame se usa en múltiples acciones.

---

## Validación y Pruebas

Ejecuta la siguiente celda de validación al final del notebook para verificar que todos los pasos se completaron correctamente:

```python
# Celda 26 — Validación final del laboratorio
print("=" * 60)
print("VALIDACIÓN FINAL DEL LABORATORIO")
print("=" * 60)

validaciones = []

def check(nombre, condicion, detalle=""):
    estado = "✅ PASS" if condicion else "❌ FAIL"
    validaciones.append({"test": nombre, "resultado": estado, "detalle": detalle})
    print(f"  {estado} | {nombre}" + (f" — {detalle}" if detalle else ""))

# Verificar que el dataset fue cargado correctamente
filas_totales = df_tx.count()
check("Dataset cargado", filas_totales >= 500_000, f"{filas_totales:,} filas")

# Verificar escritura en distintos formatos
import os, glob
check("CSV escrito", len(glob.glob(f"{OUTPUT_PATH}/resultados_csv/*.csv")) > 0,
      f"{len(glob.glob(f'{OUTPUT_PATH}/resultados_csv/*.csv'))} archivos")
check("JSON escrito", len(glob.glob(f"{OUTPUT_PATH}/resultados_json/*.json")) > 0,
      f"{len(glob.glob(f'{OUTPUT_PATH}/resultados_json/*.json'))} archivos")
check("Parquet escrito", len(glob.glob(f"{OUTPUT_PATH}/resultados_parquet/*.parquet")) > 0,
      f"{len(glob.glob(f'{OUTPUT_PATH}/resultados_parquet/*.parquet'))} archivos")

# Verificar integridad de datos escritos en Parquet
df_parquet_leido = spark.read.parquet(f"{OUTPUT_PATH}/resultados_parquet")
filas_parquet = df_parquet_leido.count()
check("Parquet íntegro", filas_parquet > 0, f"{filas_parquet} filas leídas")

# Verificar que Parquet es más pequeño que CSV
size_csv_v     = sum(os.path.getsize(f) for f in glob.glob(f"{OUTPUT_PATH}/resultados_csv/*.csv"))
size_parquet_v = sum(os.path.getsize(f) for f in glob.glob(f"{OUTPUT_PATH}/resultados_parquet/*.parquet"))
check("Parquet más compacto que CSV", size_parquet_v < size_csv_v,
      f"Parquet: {size_parquet_v//1024}KB vs CSV: {size_csv_v//1024}KB")

# Verificar que aggregate() produjo resultado correcto
check("aggregate() ejecutado", 'resultado' in dir() or promedio > 0,
      f"Promedio calculado: {promedio:.2f}")

# Verificar acumuladores de foreachPartition
check("foreachPartition() ejecutado", contador_particiones.value == 4,
      f"{contador_particiones.value} particiones procesadas")

# Resumen
pasados = sum(1 for v in validaciones if "PASS" in v["resultado"])
total   = len(validaciones)
print(f"\n{'='*60}")
print(f"Resultado: {pasados}/{total} validaciones exitosas")
if pasados == total:
    print("🎉 ¡Laboratorio completado exitosamente!")
else:
    print("⚠️  Revisa los items marcados con ❌ FAIL")
```

---

## Resolución de Problemas

### Problema 1: Error `OutOfMemoryError` al ejecutar `collect()` o `toPandas()`

**Síntomas:**
```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.collect...
```
O el kernel de Jupyter se reinicia inesperadamente al ejecutar celdas con `collect()`.

**Causa:**
La memoria asignada al driver de Spark (`spark.driver.memory`) es insuficiente para almacenar el volumen de datos que `collect()` o `toPandas()` intentan traer al nodo conductor. En máquinas con 8 GB RAM, el valor predeterminado de 1 GB para el driver puede ser insuficiente incluso para datasets medianos.

**Solución:**
```python
# OPCIÓN 1: Aumentar la memoria del driver al crear la SparkSession
# (debe hacerse ANTES de crear la sesión; reinicia el kernel si ya existe)
spark = SparkSession.builder \
    .appName("Lab06-Acciones-PySpark") \
    .master("local[*]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# OPCIÓN 2: Reducir el volumen ANTES de collect() (práctica recomendada)
# En lugar de:
#   df_tx.collect()  ← 600K filas = peligroso
# Usar:
df_tx \
    .groupBy("region") \
    .agg(F.sum("monto").alias("total")) \
    .collect()  # Solo 6 filas = seguro

# OPCIÓN 3: Usar toLocalIterator() para procesar en streaming sin cargar todo
for fila in df_tx.toLocalIterator():
    # Procesar fila a fila sin cargar todo el dataset en memoria
    pass
```

---

### Problema 2: `AnalysisException` al escribir con `write.mode("error")` en un directorio existente

**Síntomas:**
```
pyspark.errors.exceptions.captured.AnalysisException: 
[PATH_ALREADY_EXISTS] Path file:/home/usuario/spark-labs/lab06/output/resultados_csv 
already exists. Set mode as "overwrite" to overwrite the existing path.
```

**Causa:**
El modo de escritura predeterminado de la DataFrame Writer API es `"error"` (también llamado `"errorifexists"`). Si el directorio de destino ya existe —por ejemplo, porque la celda fue ejecutada en una iteración anterior— Spark lanza esta excepción para evitar sobrescrituras accidentales de datos.

**Solución:**
```python
# OPCIÓN 1: Usar mode("overwrite") para reemplazar el destino (más común en desarrollo)
df_para_escribir.write \
    .mode("overwrite") \
    .parquet(ruta_parquet)

# OPCIÓN 2: Usar mode("ignore") si quieres conservar datos existentes
df_para_escribir.write \
    .mode("ignore") \
    .parquet(ruta_parquet)

# OPCIÓN 3: Eliminar el directorio manualmente antes de escribir
import shutil
if os.path.exists(ruta_parquet):
    shutil.rmtree(ruta_parquet)
    print(f"Directorio eliminado: {ruta_parquet}")
df_para_escribir.write.parquet(ruta_parquet)

# NOTA IMPORTANTE para Windows:
# Si el error persiste en Windows incluso con mode("overwrite"),
# puede ser un problema de permisos con winutils.exe.
# Verificar que HADOOP_HOME apunta a la carpeta con winutils.exe:
# import os; print(os.environ.get("HADOOP_HOME"))
```

---

## Limpieza del Entorno

Ejecuta las siguientes celdas al finalizar el laboratorio para liberar recursos y detener la SparkSession:

```python
# Celda de limpieza — Ejecutar siempre al terminar
print("Iniciando limpieza del entorno...")

# 1. Liberar cualquier caché pendiente
try:
    df_tx.unpersist()
    df_para_escribir.unpersist()
    print("✅ Caché liberado")
except:
    print("ℹ️  No hay caché que liberar")

# 2. Limpiar archivos de prueba temporales (mantener resultados finales)
import shutil
dirs_temporales = [
    f"{OUTPUT_PATH}/bench_parquet",
    f"{OUTPUT_PATH}/bench_csv",
    f"{OUTPUT_PATH}/test_savemodes"
]
for d in dirs_temporales:
    if os.path.exists(d):
        shutil.rmtree(d)
        print(f"✅ Eliminado: {d}")

# 3. Detener la SparkSession
spark.stop()
print("✅ SparkSession detenida")
print("\n🏁 Laboratorio finalizado. Recursos liberados correctamente.")
```

**Archivos conservados tras la limpieza:**

| Archivo/Directorio | Descripción |
|--------------------|-------------|
| `~/spark-labs/lab06/data/transacciones_financieras.csv` | Dataset original |
| `~/spark-labs/lab06/output/resultados_csv/` | Resultados en CSV |
| `~/spark-labs/lab06/output/resultados_json/` | Resultados en JSON |
| `~/spark-labs/lab06/output/resultados_parquet/` | Resultados en Parquet |
| `~/spark-labs/lab06/notebooks/Lab06_Acciones_PySpark.ipynb` | Notebook completo |

---

## Resumen

En esta práctica aplicaste el conjunto completo de acciones disponibles en PySpark sobre un dataset real de 600,000 transacciones financieras. Los conceptos clave que debes retener son:

| Concepto | Aprendizaje Clave |
|----------|-------------------|
| **Lazy vs Eager** | Las transformaciones construyen el DAG; solo las acciones ejecutan el plan y producen resultados concretos |
| **Acciones de recolección** | `collect()` es poderoso pero peligroso; `take()`, `first()` y `show()` son alternativas seguras para inspección |
| **Acciones de cómputo RDD** | `aggregate()` es el más flexible: permite tipo de retorno diferente al de los elementos y cómputo en una sola pasada |
| **Writer API** | Parquet es el formato recomendado para producción: más compacto y con mejor rendimiento de lectura que CSV o JSON |
| **SaveModes** | `overwrite` para desarrollo, `append` para acumulación incremental, `ignore` para idempotencia, `error` como salvaguarda |
| **explain()** | Herramienta esencial para entender y optimizar el plan de ejecución antes de lanzar trabajos costosos |
| **foreachPartition vs foreach** | `foreachPartition()` abre una conexión por partición (eficiente); `foreach()` abre una por elemento (ineficiente para BD externas) |
| **Benchmarking** | El caché reduce significativamente el tiempo de acciones repetidas sobre el mismo DataFrame |

### Próximos Pasos

La **Práctica 7** profundizará en técnicas avanzadas de optimización de rendimiento: uso estratégico de `cache()` y `persist()` con distintos niveles de almacenamiento, particionamiento personalizado con `repartition()` y `coalesce()`, uso de variables broadcast para optimizar joins, y acumuladores para métricas distribuidas. Aplicarás estas técnicas sobre datasets de 1M+ filas y medirás el impacto real en el Spark UI.

### Referencias Adicionales

- [Apache Spark — DataFrame API: Actions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Apache Spark — RDD Programming Guide: Actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)
- [Apache Spark — Data Sources (Writer API)](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [Karau, H. et al. — *Learning Spark*, 2ª ed., O'Reilly (Cap. 3: Structured APIs)](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
- [Databricks — Best Practices: DataFrames](https://docs.databricks.com/en/getting-started/dataframes.html)

---
*Lab 07-00-01 · Práctica 6 · Curso Apache Spark con PySpark · Versión 1.0*
