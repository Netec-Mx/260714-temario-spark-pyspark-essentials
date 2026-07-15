# DataFrames en PySpark

## Metadatos

| Campo            | Detalle                                      |
|------------------|----------------------------------------------|
| **Duración**     | 60 minutos                                   |
| **Complejidad**  | Fácil                                        |
| **Nivel Bloom**  | Aplicar (Apply)                              |
| **Módulo**       | 4 — DataFrames y Spark SQL                   |
| **Práctica**     | 3 de 9                                       |

---

## Descripción General

En esta práctica explorarás los **DataFrames de PySpark** como la abstracción de alto nivel recomendada para el procesamiento de datos estructurados y semiestructurados en Apache Spark. Crearás DataFrames desde múltiples fuentes —listas Python, CSV, JSON y Parquet— y aplicarás operaciones fundamentales de exploración, selección, filtrado y ordenamiento sobre un dataset de ventas con al menos 10 000 registros. Al finalizar, comprenderás cuándo y por qué preferir DataFrames sobre RDDs, y habrás utilizado el Catalyst Optimizer de forma transparente para ejecutar consultas eficientes.

---

## Objetivos de Aprendizaje

Al completar esta práctica serás capaz de:

- [ ] Crear DataFrames en PySpark a partir de listas Python, archivos CSV, JSON y Parquet, definiendo esquemas explícitos con `StructType` y `StructField`.
- [ ] Explorar la estructura y el esquema de un DataFrame utilizando `printSchema()`, `dtypes`, `describe()`, `show()` y `count()`.
- [ ] Aplicar operaciones fundamentales sobre DataFrames: selección de columnas (`select`, `col`, `alias`), filtrado (`filter`, `where`), ordenamiento (`orderBy`) y columnas calculadas (`withColumn`).
- [ ] Comparar el modelo de programación de DataFrames con RDDs e identificar las ventajas del Catalyst Optimizer en términos de concisión y rendimiento.

---

## Prerrequisitos

### Conocimiento Previo

- Práctica 2 completada: comprensión de RDDs, SparkContext y SparkSession.
- Familiaridad con estructuras tabulares (tablas relacionales, archivos CSV).
- Comprensión básica del concepto de esquema de datos y tipos de datos primitivos.
- Experiencia básica con pandas DataFrames como referencia conceptual (no es obligatorio tener pandas instalado).

### Acceso y Software

| Componente          | Versión requerida          |
|---------------------|---------------------------|
| Java (JDK)          | 11 o 17 LTS               |
| Apache Spark        | 3.5.x                     |
| Python              | 3.10 o 3.11               |
| PySpark             | 3.5.x                     |
| JupyterLab          | 7.x                       |
| findspark           | 2.0.1+                    |
| Repositorio/datos   | Acceso al repo de la práctica |

> **Windows:** Asegúrate de tener `winutils.exe` configurado y la variable `HADOOP_HOME` apuntando al directorio correcto antes de iniciar (consulta la guía complementaria de configuración Windows del curso).

---

## Entorno de Laboratorio

### Configuración de Hardware Recomendada

| Recurso       | Mínimo      | Recomendado |
|---------------|-------------|-------------|
| RAM           | 8 GB        | 16 GB       |
| CPU           | 4 núcleos   | 4+ núcleos  |
| Disco libre   | 20 GB       | 20 GB+      |

### Variables de Entorno Necesarias

Verifica que las siguientes variables estén configuradas en tu sistema antes de iniciar:

```bash
# Verificar variables de entorno (Linux/macOS)
echo $JAVA_HOME
echo $SPARK_HOME
echo $PYSPARK_PYTHON

# En Windows (PowerShell)
echo $env:JAVA_HOME
echo $env:SPARK_HOME
echo $env:HADOOP_HOME
```

### Preparación del Entorno

Ejecuta los siguientes comandos en una terminal para preparar el directorio de trabajo y generar los datasets de práctica:

```bash
# 1. Crear estructura de directorios para la práctica
mkdir -p ~/spark-labs/practica3/{data/raw,data/processed,notebooks}
cd ~/spark-labs/practica3

# 2. Verificar que PySpark está disponible
python -c "import pyspark; print(f'PySpark versión: {pyspark.__version__}')"

# 3. Verificar que findspark está disponible
python -c "import findspark; findspark.init(); print('findspark OK')"
```

Luego, ejecuta el siguiente script Python **una sola vez** para generar los datasets de práctica. Guárdalo como `generar_datasets.py` y ejecútalo con `python generar_datasets.py`:

```python
# generar_datasets.py
# Script de preparación de datasets para la Práctica 3
# Ejecutar UNA VEZ antes de iniciar el notebook principal

import os
import json
import random
import csv
from datetime import datetime, timedelta

random.seed(42)

BASE_DIR = os.path.expanduser("~/spark-labs/practica3/data/raw")
os.makedirs(BASE_DIR, exist_ok=True)

# ── Dataset 1: ventas.csv (12 000 registros) ──────────────────────────────────
productos = ["Laptop", "Monitor", "Teclado", "Mouse", "Auriculares",
             "Webcam", "SSD", "RAM", "Tablet", "Smartphone"]
categorias = {"Laptop": "Computadoras", "Monitor": "Periféricos",
              "Teclado": "Periféricos", "Mouse": "Periféricos",
              "Auriculares": "Audio", "Webcam": "Video",
              "SSD": "Almacenamiento", "RAM": "Componentes",
              "Tablet": "Móviles", "Smartphone": "Móviles"}
regiones = ["Norte", "Sur", "Este", "Oeste", "Centro"]
vendedores = [f"V{str(i).zfill(3)}" for i in range(1, 51)]

fecha_inicio = datetime(2023, 1, 1)

ventas_path = os.path.join(BASE_DIR, "ventas.csv")
with open(ventas_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["venta_id", "fecha", "producto", "categoria",
                     "region", "vendedor_id", "cantidad", "precio_unitario",
                     "descuento", "total"])
    for i in range(1, 12001):
        producto = random.choice(productos)
        cantidad = random.randint(1, 20)
        precio = round(random.uniform(15.0, 1500.0), 2)
        descuento = round(random.choice([0.0, 0.05, 0.10, 0.15, 0.20]), 2)
        total = round(cantidad * precio * (1 - descuento), 2)
        fecha = (fecha_inicio + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")
        writer.writerow([i, fecha, producto, categorias[producto],
                         random.choice(regiones), random.choice(vendedores),
                         cantidad, precio, descuento, total])

print(f"✔ ventas.csv generado: 12 000 filas → {ventas_path}")

# ── Dataset 2: usuarios.json (500 registros) ─────────────────────────────────
paises = ["México", "Argentina", "Colombia", "España", "Chile",
          "Perú", "Venezuela", "Ecuador"]
planes = ["free", "basic", "premium", "enterprise"]
nombres = ["Lucía", "Carlos", "Ana", "Pedro", "María", "Jorge",
           "Sofía", "Miguel", "Elena", "Roberto"]
apellidos = ["García", "López", "Martínez", "Rodríguez", "González",
             "Fernández", "Torres", "Díaz", "Pérez", "Sánchez"]

usuarios = []
for i in range(1, 501):
    nombre = f"{random.choice(nombres)} {random.choice(apellidos)}"
    edad = random.randint(18, 65)
    pais = random.choice(paises)
    plan = random.choice(planes)
    activo = random.choice([True, True, True, False])
    registro = (fecha_inicio + timedelta(days=random.randint(0, 729))).strftime("%Y-%m-%d")
    usuarios.append({
        "usuario_id": i,
        "nombre": nombre,
        "edad": edad,
        "pais": pais,
        "plan": plan,
        "activo": activo,
        "fecha_registro": registro,
        "compras_totales": random.randint(0, 150)
    })

usuarios_path = os.path.join(BASE_DIR, "usuarios.json")
with open(usuarios_path, "w", encoding="utf-8") as f:
    for u in usuarios:
        f.write(json.dumps(u, ensure_ascii=False) + "\n")

print(f"✔ usuarios.json generado: 500 registros → {usuarios_path}")

print("\n✅ Todos los datasets están listos. Inicia JupyterLab para comenzar la práctica.")
```

```bash
# Ejecutar el script generador
python ~/spark-labs/practica3/generar_datasets.py
```

---

## Pasos de la Práctica

> **Instrucción general:** Crea un nuevo notebook en JupyterLab llamado `practica3_dataframes.ipynb` dentro de `~/spark-labs/practica3/notebooks/`. Ejecuta cada celda de forma secuencial. Los bloques de código de cada paso corresponden a celdas individuales del notebook.

---

### Paso 1 — Inicializar la SparkSession y Configurar el Entorno

**Objetivo:** Establecer la SparkSession con configuración adecuada de memoria y verificar que el entorno está listo para trabajar con DataFrames.

#### Instrucciones

1. Crea la primera celda del notebook e importa las dependencias necesarias.
2. Inicializa la SparkSession con los parámetros de memoria apropiados para equipos de 8 GB RAM.
3. Verifica que la sesión se creó correctamente imprimiendo la versión de Spark.

```python
# Celda 1 — Importaciones y configuración inicial
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
    BooleanType, DateType, LongType
)
from pyspark.sql.functions import col, lit, round as spark_round, upper, when
import os

# Ruta base de los datos
BASE_DIR = os.path.expanduser("~/spark-labs/practica3/data/raw")

# Crear la SparkSession con configuración de memoria
spark = SparkSession.builder \
    .appName("Practica3_DataFrames") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Establecer nivel de log para reducir ruido en el notebook
spark.sparkContext.setLogLevel("WARN")

print(f"✅ SparkSession iniciada correctamente")
print(f"   Versión de Spark : {spark.version}")
print(f"   Modo de ejecución: {spark.sparkContext.master}")
print(f"   Nombre de la app : {spark.sparkContext.appName}")
print(f"\n🌐 Spark UI disponible en: http://localhost:4040")
```

**Salida esperada:**

```
✅ SparkSession iniciada correctamente
   Versión de Spark : 3.5.x
   Modo de ejecución: local[*]
   Nombre de la app : Practica3_DataFrames

🌐 Spark UI disponible en: http://localhost:4040
```

**Verificación:** Abre un navegador y navega a `http://localhost:4040`. Deberías ver la interfaz de Spark UI con la aplicación `Practica3_DataFrames` activa.

---

### Paso 2 — Crear un DataFrame desde una Lista Python

**Objetivo:** Comprender la forma más directa de crear un DataFrame usando `spark.createDataFrame()` con datos en memoria, primero con inferencia de esquema y luego con esquema explícito.

#### Instrucciones

1. Crea un DataFrame pequeño a partir de una lista de tuplas usando inferencia de esquema automática.
2. Inspecciona el esquema inferido con `printSchema()` y `dtypes`.
3. Crea el mismo DataFrame pero con un **esquema explícito** usando `StructType` y `StructField`.
4. Compara ambos esquemas y observa las diferencias en los tipos inferidos vs. declarados.

```python
# Celda 2 — DataFrame desde lista Python (inferencia de esquema)

# Datos de ejemplo: 10 empleados del departamento de ventas
datos_empleados = [
    (1, "Ana García",     34, "Madrid",    52000.0, True),
    (2, "Carlos López",   28, "Barcelona", 47500.0, True),
    (3, "María Torres",   41, "Sevilla",   61000.0, True),
    (4, "Pedro Ruiz",     25, "Valencia",  38000.0, False),
    (5, "Laura Sánchez",  36, "Madrid",    55000.0, True),
    (6, "Miguel Díaz",    30, "Bilbao",    49000.0, True),
    (7, "Elena Martínez", 45, "Madrid",    68000.0, True),
    (8, "Roberto Pérez",  27, "Málaga",    41000.0, False),
    (9, "Sofía González", 33, "Barcelona", 53500.0, True),
    (10,"Jorge Fernández",38, "Madrid",    59000.0, True)
]

columnas = ["empleado_id", "nombre", "edad", "ciudad", "salario", "activo"]

# Crear DataFrame con inferencia automática de esquema
df_empleados_inferido = spark.createDataFrame(datos_empleados, columnas)

print("── Esquema INFERIDO automáticamente ──")
df_empleados_inferido.printSchema()

print("── dtypes (lista de tuplas nombre/tipo) ──")
print(df_empleados_inferido.dtypes)
```

```python
# Celda 3 — DataFrame desde lista Python (esquema EXPLÍCITO)

# Definir esquema explícito con StructType y StructField
# Formato: StructField("nombre_columna", TipoDeDato(), admite_nulos)
esquema_empleados = StructType([
    StructField("empleado_id", IntegerType(),  nullable=False),
    StructField("nombre",      StringType(),   nullable=False),
    StructField("edad",        IntegerType(),  nullable=True),
    StructField("ciudad",      StringType(),   nullable=True),
    StructField("salario",     DoubleType(),   nullable=True),
    StructField("activo",      BooleanType(),  nullable=True)
])

# Crear DataFrame con esquema explícito
df_empleados = spark.createDataFrame(datos_empleados, esquema_empleados)

print("── Esquema EXPLÍCITO definido por el desarrollador ──")
df_empleados.printSchema()

print("── Vista tabular de los datos ──")
df_empleados.show(truncate=False)

print(f"\nTotal de filas  : {df_empleados.count()}")
print(f"Total de columnas: {len(df_empleados.columns)}")
print(f"Nombres de columnas: {df_empleados.columns}")
```

**Salida esperada (Celda 3):**

```
── Esquema EXPLÍCITO definido por el desarrollador ──
root
 |-- empleado_id: integer (nullable = false)
 |-- nombre: string (nullable = false)
 |-- edad: integer (nullable = true)
 |-- ciudad: string (nullable = true)
 |-- salario: double (nullable = true)
 |-- activo: boolean (nullable = true)

── Vista tabular de los datos ──
+-----------+----------------+----+---------+-------+------+
|empleado_id|          nombre|edad|   ciudad|salario|activo|
+-----------+----------------+----+---------+-------+------+
|          1|      Ana García|  34|   Madrid|52000.0|  true|
|          2|    Carlos López|  28|Barcelona|47500.0|  true|
...
+-----------+----------------+----+---------+-------+------+

Total de filas  : 10
Total de columnas: 6
Nombres de columnas: ['empleado_id', 'nombre', 'edad', 'ciudad', 'salario', 'activo']
```

**Verificación:** Confirma que en el esquema explícito `empleado_id` aparece como `integer (nullable = false)`, mientras que en el esquema inferido aparecía como `long (nullable = true)`. Esta diferencia ilustra por qué definir esquemas explícitos es una buena práctica en producción.

---

### Paso 3 — Leer un DataFrame desde un Archivo CSV

**Objetivo:** Cargar el dataset de ventas desde un archivo CSV utilizando `spark.read.csv()`, con y sin esquema explícito, y explorar las opciones de lectura disponibles.

#### Instrucciones

1. Lee el archivo `ventas.csv` sin definir esquema (inferencia automática) y observa el resultado.
2. Define un esquema explícito para el CSV y léelo nuevamente.
3. Usa `describe()` para obtener estadísticas descriptivas de las columnas numéricas.
4. Verifica el número total de registros.

```python
# Celda 4 — Leer CSV sin esquema (inferencia)

ruta_csv = os.path.join(BASE_DIR, "ventas.csv")

# Lectura básica con inferencia de esquema
# inferSchema=True hace que Spark lea el archivo dos veces (más lento)
df_ventas_inferido = spark.read.csv(
    ruta_csv,
    header=True,       # La primera fila contiene los nombres de columnas
    inferSchema=True,  # Inferir tipos automáticamente
    encoding="UTF-8"
)

print(f"Registros cargados: {df_ventas_inferido.count()}")
print("\n── Esquema inferido ──")
df_ventas_inferido.printSchema()
df_ventas_inferido.show(5)
```

```python
# Celda 5 — Leer CSV con esquema EXPLÍCITO (recomendado en producción)

esquema_ventas = StructType([
    StructField("venta_id",        IntegerType(), nullable=False),
    StructField("fecha",           StringType(),  nullable=True),
    StructField("producto",        StringType(),  nullable=True),
    StructField("categoria",       StringType(),  nullable=True),
    StructField("region",          StringType(),  nullable=True),
    StructField("vendedor_id",     StringType(),  nullable=True),
    StructField("cantidad",        IntegerType(), nullable=True),
    StructField("precio_unitario", DoubleType(),  nullable=True),
    StructField("descuento",       DoubleType(),  nullable=True),
    StructField("total",           DoubleType(),  nullable=True)
])

# Lectura con esquema explícito: más rápida y predecible
df_ventas = spark.read.csv(
    ruta_csv,
    header=True,
    schema=esquema_ventas,
    encoding="UTF-8"
)

print(f"✅ Dataset de ventas cargado: {df_ventas.count()} registros")
print(f"   Particiones: {df_ventas.rdd.getNumPartitions()}")
print("\n── Esquema explícito ──")
df_ventas.printSchema()

print("\n── Primeras 5 filas ──")
df_ventas.show(5)

print("\n── Estadísticas descriptivas (columnas numéricas) ──")
df_ventas.describe("cantidad", "precio_unitario", "descuento", "total").show()
```

**Salida esperada (fragmento):**

```
✅ Dataset de ventas cargado: 12000 registros
   Particiones: 1

── Estadísticas descriptivas (columnas numéricas) ──
+-------+------------------+------------------+-------------------+------------------+
|summary|          cantidad|   precio_unitario|          descuento|             total|
+-------+------------------+------------------+-------------------+------------------+
|  count|             12000|             12000|              12000|             12000|
|   mean|10.498333333333333| 757.5531666666...|0.09791666666666...|7942.2421666666...|
|  stddev| 5.76...          | 428.1...         | 0.0866...         | 6123.4...        |
|    min|                 1|             15.02|                0.0|             15.02|
|    max|                20|           1499.99|                0.2|          29999.8 |
+-------+------------------+------------------+-------------------+------------------+
```

**Verificación:** El conteo debe devolver exactamente **12 000** registros. Si obtienes 11 999 o 12 001, verifica que el CSV se generó correctamente re-ejecutando `generar_datasets.py`.

---

### Paso 4 — Leer DataFrames desde JSON y Parquet

**Objetivo:** Demostrar la versatilidad de la API de lectura de Spark cargando datos desde formato JSON (líneas) y generando un archivo Parquet para ilustrar su uso.

#### Instrucciones

1. Lee el archivo `usuarios.json` (formato JSON Lines) con `spark.read.json()`.
2. Guarda el DataFrame de ventas en formato Parquet.
3. Lee el archivo Parquet resultante y compara su esquema con el original.

```python
# Celda 6 — Leer JSON

ruta_json = os.path.join(BASE_DIR, "usuarios.json")

# Spark infiere el esquema del JSON automáticamente
# Formato: JSON Lines (un objeto JSON por línea)
df_usuarios = spark.read.json(ruta_json)

print(f"✅ Usuarios cargados: {df_usuarios.count()} registros")
print("\n── Esquema inferido desde JSON ──")
df_usuarios.printSchema()

print("\n── Muestra de datos ──")
df_usuarios.show(5, truncate=False)
```

```python
# Celda 7 — Guardar como Parquet y releer

ruta_parquet = os.path.expanduser("~/spark-labs/practica3/data/processed/ventas.parquet")

# Guardar el DataFrame de ventas en formato Parquet (columnar, comprimido)
df_ventas.write.mode("overwrite").parquet(ruta_parquet)
print(f"✅ Archivo Parquet guardado en: {ruta_parquet}")

# Releer desde Parquet — el esquema se preserva automáticamente
df_ventas_parquet = spark.read.parquet(ruta_parquet)

print(f"\n✅ Parquet leído: {df_ventas_parquet.count()} registros")
print("\n── Esquema desde Parquet (sin necesidad de definirlo) ──")
df_ventas_parquet.printSchema()

# Comparar tamaños aproximados
import os as _os
csv_size = _os.path.getsize(os.path.join(BASE_DIR, "ventas.csv"))
parquet_size = sum(
    _os.path.getsize(_os.path.join(root, f))
    for root, dirs, files in _os.walk(ruta_parquet)
    for f in files if f.endswith(".parquet")
)
print(f"\n📊 Comparación de tamaño:")
print(f"   CSV    : {csv_size / 1024:.1f} KB")
print(f"   Parquet: {parquet_size / 1024:.1f} KB")
print(f"   Reducción: {(1 - parquet_size/csv_size)*100:.1f}%")
```

**Salida esperada (fragmento):**

```
✅ Archivo Parquet guardado en: ~/spark-labs/practica3/data/processed/ventas.parquet
✅ Parquet leído: 12000 registros

📊 Comparación de tamaño:
   CSV    : 620.3 KB
   Parquet: 148.7 KB
   Reducción: 76.0%
```

**Verificación:** El esquema del DataFrame leído desde Parquet debe ser **idéntico** al esquema explícito que definiste en el Paso 3, incluyendo los tipos de datos. Parquet almacena el esquema dentro del archivo, eliminando la necesidad de redefinirlo al leer.

---

### Paso 5 — Explorar la Estructura del DataFrame

**Objetivo:** Dominar los métodos de exploración de DataFrames para entender rápidamente cualquier dataset nuevo antes de procesarlo.

#### Instrucciones

1. Utiliza todos los métodos de exploración disponibles sobre el DataFrame de ventas.
2. Identifica columnas con posibles valores nulos.
3. Explora la distribución de valores categóricos.

```python
# Celda 8 — Exploración completa del DataFrame de ventas

print("═" * 60)
print("EXPLORACIÓN DEL DATASET DE VENTAS")
print("═" * 60)

# 1. Dimensiones
print(f"\n1. DIMENSIONES")
print(f"   Filas   : {df_ventas.count():,}")
print(f"   Columnas: {len(df_ventas.columns)}")

# 2. Nombres y tipos de columnas
print(f"\n2. COLUMNAS Y TIPOS (dtypes)")
for nombre, tipo in df_ventas.dtypes:
    print(f"   {nombre:<20} → {tipo}")

# 3. Esquema detallado
print(f"\n3. ESQUEMA DETALLADO (printSchema)")
df_ventas.printSchema()

# 4. Muestra de datos
print("4. MUESTRA DE DATOS (primeras 3 filas)")
df_ventas.show(3, truncate=True)

# 5. Estadísticas descriptivas
print("5. ESTADÍSTICAS DESCRIPTIVAS")
df_ventas.describe().show()

# 6. Contar valores nulos por columna
print("6. VALORES NULOS POR COLUMNA")
from pyspark.sql.functions import count, isnan, when, col as fcol

nulos = df_ventas.select([
    count(when(fcol(c).isNull(), c)).alias(c)
    for c in df_ventas.columns
])
nulos.show()

# 7. Valores únicos en columnas categóricas
print("7. VALORES ÚNICOS EN COLUMNAS CATEGÓRICAS")
for columna in ["producto", "categoria", "region"]:
    valores = df_ventas.select(columna).distinct().count()
    print(f"   {columna:<12}: {valores} valores únicos")
```

**Salida esperada (fragmento):**

```
═══════════════════════════════════════════════════════════
EXPLORACIÓN DEL DATASET DE VENTAS
═══════════════════════════════════════════════════════════

1. DIMENSIONES
   Filas   : 12,000
   Columnas: 10

2. COLUMNAS Y TIPOS (dtypes)
   venta_id             → integer
   fecha                → string
   ...

6. VALORES NULOS POR COLUMNA
+--------+-----+--------+---------+------+----------+--------+---------------+---------+-----+
|venta_id|fecha|producto|categoria|region|vendedor_id|cantidad|precio_unitario|descuento|total|
+--------+-----+--------+---------+------+----------+--------+---------------+---------+-----+
|       0|    0|       0|        0|     0|          0|       0|              0|        0|    0|
+--------+-----+--------+---------+------+----------+--------+---------------+---------+-----+

7. VALORES ÚNICOS EN COLUMNAS CATEGÓRICAS
   producto    : 10 valores únicos
   categoria   : 6 valores únicos
   region      : 5 valores únicos
```

**Verificación:** La fila de nulos debe mostrar **0** en todas las columnas, ya que el dataset fue generado sin valores faltantes. Guarda este patrón de exploración: es el estándar profesional para auditar cualquier dataset nuevo.

---

### Paso 6 — Selección de Columnas y Proyecciones

**Objetivo:** Aplicar operaciones de selección usando `select()`, la función `col()` y alias para renombrar columnas, trabajando sobre el dataset de ventas real.

#### Instrucciones

1. Selecciona subconjuntos de columnas usando diferentes sintaxis.
2. Usa `col()` para referenciar columnas de forma explícita.
3. Aplica `alias()` para renombrar columnas en la proyección.
4. Crea una columna calculada con `withColumn()`.

```python
# Celda 9 — Selección básica de columnas

print("── Selección por nombre de columna (sintaxis string) ──")
df_ventas.select("fecha", "producto", "region", "total").show(5)

print("── Selección usando col() (sintaxis objeto Column) ──")
df_ventas.select(
    col("fecha"),
    col("producto"),
    col("total")
).show(5)

print("── Selección con alias para renombrar columnas ──")
df_ventas.select(
    col("venta_id").alias("id"),
    col("producto").alias("nombre_producto"),
    col("precio_unitario").alias("precio"),
    col("total").alias("importe_total")
).show(5)
```

```python
# Celda 10 — Columnas calculadas con withColumn()

# withColumn() agrega o reemplaza una columna en el DataFrame
# NO modifica el DataFrame original (recuerda: los DataFrames son inmutables)
df_ventas_enriquecido = df_ventas \
    .withColumn(
        "precio_con_iva",
        spark_round(col("precio_unitario") * 1.21, 2)
    ) \
    .withColumn(
        "importe_descuento",
        spark_round(col("precio_unitario") * col("cantidad") * col("descuento"), 2)
    ) \
    .withColumn(
        "tiene_descuento",
        when(col("descuento") > 0, True).otherwise(False)
    ) \
    .withColumn(
        "producto_mayusculas",
        upper(col("producto"))
    )

print("── DataFrame enriquecido con columnas calculadas ──")
df_ventas_enriquecido.select(
    "venta_id", "producto", "producto_mayusculas",
    "precio_unitario", "precio_con_iva",
    "descuento", "importe_descuento", "tiene_descuento"
).show(8, truncate=False)

print(f"\nColumnas totales ahora: {len(df_ventas_enriquecido.columns)}")
print(f"Columnas: {df_ventas_enriquecido.columns}")
```

**Salida esperada (fragmento):**

```
── DataFrame enriquecido con columnas calculadas ──
+--------+----------+-------------------+---------------+--------------+---------+-----------------+---------------+
|venta_id|  producto|producto_mayusculas|precio_unitario|precio_con_iva|descuento|importe_descuento|tiene_descuento|
+--------+----------+-------------------+---------------+--------------+---------+-----------------+---------------+
|       1|    Laptop|             LAPTOP|         850.50|       1029.11|     0.10|           1700.0|           true|
...
+--------+----------+-------------------+---------------+--------------+---------+-----------------+---------------+

Columnas totales ahora: 14
```

**Verificación:** Confirma que `df_ventas` original sigue teniendo **10 columnas** después de ejecutar `withColumn()`. Esto demuestra la inmutabilidad de los DataFrames: `withColumn()` retorna un *nuevo* DataFrame.

```python
# Celda 11 — Verificar inmutabilidad
print(f"df_ventas original    : {len(df_ventas.columns)} columnas → {df_ventas.columns}")
print(f"df_ventas_enriquecido : {len(df_ventas_enriquecido.columns)} columnas")
```

---

### Paso 7 — Filtrado de Filas

**Objetivo:** Aplicar condiciones de filtrado usando `filter()` y `where()` con condiciones simples y compuestas, y verificar los resultados sobre el dataset de 12 000 registros.

#### Instrucciones

1. Aplica filtros simples sobre columnas numéricas y de texto.
2. Combina condiciones con operadores `&` (AND) y `|` (OR).
3. Usa la sintaxis alternativa `where()` (equivalente a `filter()`).
4. Filtra con `isin()` para listas de valores.

```python
# Celda 12 — Filtros simples

print("── Ventas con total mayor a 10,000 ──")
df_ventas_altas = df_ventas.filter(col("total") > 10000)
print(f"Registros: {df_ventas_altas.count():,}")
df_ventas_altas.show(5)

print("\n── Ventas de la región Norte (usando where, equivalente a filter) ──")
df_norte = df_ventas.where(col("region") == "Norte")
print(f"Registros región Norte: {df_norte.count():,}")

print("\n── Ventas con descuento aplicado ──")
df_con_descuento = df_ventas.filter(col("descuento") > 0)
print(f"Registros con descuento: {df_con_descuento.count():,}")
print(f"Porcentaje del total  : {df_con_descuento.count()/df_ventas.count()*100:.1f}%")
```

```python
# Celda 13 — Filtros compuestos

print("── Ventas de Laptops en región Norte con descuento ──")
df_filtro_compuesto = df_ventas.filter(
    (col("producto") == "Laptop") &
    (col("region") == "Norte") &
    (col("descuento") > 0)
)
print(f"Registros: {df_filtro_compuesto.count():,}")
df_filtro_compuesto.select("venta_id","fecha","producto","region","descuento","total").show(5)

print("\n── Ventas de Smartphones O Tablets ──")
df_moviles = df_ventas.filter(
    (col("producto") == "Smartphone") | (col("producto") == "Tablet")
)
print(f"Registros móviles: {df_moviles.count():,}")

print("\n── Filtro con isin(): productos de categoría Periféricos ──")
productos_perifericos = ["Monitor", "Teclado", "Mouse"]
df_perifericos = df_ventas.filter(col("producto").isin(productos_perifericos))
print(f"Registros periféricos: {df_perifericos.count():,}")

print("\n── Filtro con between(): ventas de precio entre 100 y 500 ──")
df_precio_medio = df_ventas.filter(col("precio_unitario").between(100, 500))
print(f"Registros precio 100-500: {df_precio_medio.count():,}")
```

**Salida esperada (valores aproximados):**

```
── Ventas con total mayor a 10,000 ──
Registros: 4,523

── Ventas con descuento aplicado ──
Registros con descuento: 9,003
Porcentaje del total  : 75.0%

── Ventas de Laptops en región Norte con descuento ──
Registros: 187
```

**Verificación:** La suma de registros de `df_norte` + registros de las otras 4 regiones debe ser igual a **12 000**. Verifica esto con:

```python
# Celda 14 — Verificación de partición de datos por región
from pyspark.sql.functions import count as fcount

print("── Distribución por región (debe sumar 12,000) ──")
df_ventas.groupBy("region").agg(fcount("*").alias("total_ventas")).orderBy("region").show()
```

---

### Paso 8 — Ordenamiento y Eliminación de Duplicados

**Objetivo:** Aplicar `orderBy()` y `sort()` para ordenar resultados, y `dropDuplicates()` para eliminar registros duplicados.

#### Instrucciones

1. Ordena el DataFrame por columnas numéricas en orden ascendente y descendente.
2. Aplica ordenamiento multi-columna.
3. Demuestra `dropDuplicates()` creando un DataFrame con duplicados artificiales.

```python
# Celda 15 — Ordenamiento

print("── Top 10 ventas por importe total (descendente) ──")
df_ventas.select("venta_id", "fecha", "producto", "region", "total") \
    .orderBy(col("total").desc()) \
    .show(10)

print("\n── Ventas más baratas (precio_unitario ascendente) ──")
df_ventas.select("venta_id", "producto", "precio_unitario", "cantidad") \
    .orderBy(col("precio_unitario").asc()) \
    .show(5)

print("\n── Ordenamiento multi-columna: región ASC, total DESC ──")
df_ventas.select("venta_id", "region", "producto", "total") \
    .orderBy(
        col("region").asc(),
        col("total").desc()
    ) \
    .show(10)
```

```python
# Celda 16 — Eliminación de duplicados

# Crear DataFrame con filas duplicadas artificialmente para demostración
from pyspark.sql.functions import lit as flit

df_con_duplicados = df_ventas.limit(100).union(df_ventas.limit(100))
print(f"DataFrame con duplicados: {df_con_duplicados.count()} filas")

# dropDuplicates() sin argumentos: elimina filas completamente idénticas
df_sin_duplicados = df_con_duplicados.dropDuplicates()
print(f"Después de dropDuplicates(): {df_sin_duplicados.count()} filas")

# dropDuplicates() con columnas específicas: mantiene primera ocurrencia
# Útil para obtener el catálogo único de productos
df_catalogo_productos = df_ventas.dropDuplicates(["producto", "categoria"])
print(f"\nCatálogo único de productos: {df_catalogo_productos.count()} productos")
df_catalogo_productos.select("producto", "categoria") \
    .orderBy("categoria", "producto") \
    .show(truncate=False)
```

**Salida esperada:**

```
DataFrame con duplicados: 200 filas
Después de dropDuplicates(): 100 filas

Catálogo único de productos: 10 productos
+------------+---------------+
|     producto|      categoria|
+------------+---------------+
|         RAM|    Componentes|
|      Laptop|   Computadoras|
|  Auriculares|          Audio|
|       Mouse|    Periféricos|
|     Monitor|    Periféricos|
|     Teclado|    Periféricos|
|Smartphone  |        Móviles|
|      Tablet|        Móviles|
|         SSD|   Almacenamiento|
|      Webcam|          Video|
+------------+---------------+
```

**Verificación:** `dropDuplicates()` sobre el DataFrame con 200 filas debe devolver exactamente **100** (la mitad), ya que las 100 filas adicionales son copias exactas de las primeras 100.

---

### Paso 9 — Comparación DataFrame vs. RDD

**Objetivo:** Realizar la misma operación analítica usando RDDs y DataFrames para contrastar la concisión del código y comprender cuándo usar cada abstracción.

#### Instrucciones

1. Implementa el cálculo del total de ventas por región usando RDD (bajo nivel).
2. Implementa el mismo cálculo usando la API de DataFrames (alto nivel).
3. Compara el código y reflexiona sobre las diferencias.

```python
# Celda 17 — Misma operación: RDD vs. DataFrame

print("═" * 60)
print("COMPARACIÓN: RDD vs. DataFrame")
print("Tarea: Total de ventas por región")
print("═" * 60)

# ── ENFOQUE 1: Con RDD (bajo nivel) ──────────────────────────
print("\n[ENFOQUE 1 — RDD]")
from pyspark.sql.functions import sum as fsum

# Convertir a RDD y calcular manualmente
rdd_ventas = df_ventas.rdd

# map: extraer (region, total) | reduceByKey: sumar totales
resultado_rdd = rdd_ventas \
    .map(lambda row: (row["region"], row["total"])) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .collect()

print("Resultado con RDD:")
for region, total in resultado_rdd:
    print(f"  {region:<10}: ${total:>15,.2f}")

# ── ENFOQUE 2: Con DataFrame API (alto nivel) ─────────────────
print("\n[ENFOQUE 2 — DataFrame API]")

resultado_df = df_ventas \
    .groupBy("region") \
    .agg(fsum("total").alias("total_ventas")) \
    .orderBy(col("total_ventas").desc())

print("Resultado con DataFrame:")
resultado_df.show()

# ── Reflexión ─────────────────────────────────────────────────
print("\n[REFLEXIÓN]")
print("Líneas de código RDD       : ~5 líneas de lógica explícita")
print("Líneas de código DataFrame : ~4 líneas declarativas")
print("\nVentajas del DataFrame:")
print("  ✔ Código más conciso y legible")
print("  ✔ Catalyst Optimizer optimiza automáticamente el plan")
print("  ✔ Soporta SQL nativo (veremos esto en prácticas posteriores)")
print("  ✔ Mejor rendimiento en la mayoría de los escenarios")
print("\nCuándo usar RDDs:")
print("  → Procesamiento de datos no estructurados (texto libre, binarios)")
print("  → Lógica de transformación muy personalizada sin equivalente en API")
print("  → Compatibilidad con código legacy de Spark 1.x")
```

**Verificación:** Los totales por región deben ser **idénticos** en ambos enfoques (RDD y DataFrame). Si hay diferencias, indica un error en la lógica del RDD. Verifica que los 5 valores de región sumen aproximadamente el total global de ventas.

```python
# Celda 18 — Verificación cruzada
total_global = df_ventas.agg(fsum("total").alias("total")).collect()[0]["total"]
total_por_region = resultado_df.agg(fsum("total_ventas").alias("total")).collect()[0]["total"]

print(f"Total global          : ${total_global:>15,.2f}")
print(f"Suma totales x región : ${total_por_region:>15,.2f}")
print(f"¿Coinciden?           : {'✅ SÍ' if abs(total_global - total_por_region) < 0.01 else '❌ NO'}")
```

---

## Validación y Pruebas

Ejecuta la siguiente celda de validación completa al final del notebook para confirmar que todos los pasos se completaron correctamente:

```python
# Celda de Validación Final
print("═" * 60)
print("VALIDACIÓN FINAL — PRÁCTICA 3")
print("═" * 60)

resultados = {}

# Prueba 1: SparkSession activa
resultados["spark_session"] = spark is not None and spark.version.startswith("3.5")

# Prueba 2: DataFrame desde lista con esquema explícito
resultados["df_lista"] = (
    df_empleados.count() == 10 and
    len(df_empleados.columns) == 6 and
    dict(df_empleados.dtypes)["empleado_id"] == "int"
)

# Prueba 3: CSV cargado correctamente
resultados["df_csv"] = (
    df_ventas.count() == 12000 and
    len(df_ventas.columns) == 10
)

# Prueba 4: JSON cargado correctamente
resultados["df_json"] = (
    df_usuarios.count() == 500 and
    "usuario_id" in df_usuarios.columns
)

# Prueba 5: Parquet leído correctamente
resultados["df_parquet"] = (
    df_ventas_parquet.count() == 12000 and
    df_ventas_parquet.schema == df_ventas.schema
)

# Prueba 6: withColumn agrega columna nueva
resultados["with_column"] = (
    "precio_con_iva" in df_ventas_enriquecido.columns and
    len(df_ventas_enriquecido.columns) == 14
)

# Prueba 7: Filtrado funciona correctamente
ventas_norte = df_ventas.filter(col("region") == "Norte").count()
resultados["filtrado"] = (ventas_norte > 0 and ventas_norte < 12000)

# Prueba 8: dropDuplicates funciona
resultados["drop_duplicates"] = (
    df_catalogo_productos.count() == 10
)

# Prueba 9: Totales RDD == Totales DataFrame
resultados["rdd_vs_df"] = abs(total_global - total_por_region) < 0.01

# Reporte
print(f"\n{'PRUEBA':<35} {'ESTADO'}")
print("-" * 50)
for nombre, ok in resultados.items():
    estado = "✅ PASÓ" if ok else "❌ FALLÓ"
    print(f"  {nombre:<33} {estado}")

total_ok = sum(resultados.values())
total_pruebas = len(resultados)
print(f"\nResultado: {total_ok}/{total_pruebas} pruebas pasaron")

if total_ok == total_pruebas:
    print("\n🎉 ¡Práctica 3 completada exitosamente!")
else:
    print("\n⚠️  Revisa los pasos correspondientes a las pruebas fallidas.")
```

**Resultado esperado:** `9/9 pruebas pasaron`

---

## Resolución de Problemas

### Problema 1: Error `AnalysisException: Path does not exist` al leer CSV o JSON

**Síntoma:**
```
pyspark.errors.AnalysisException: [PATH_NOT_FOUND] Path does not exist: 
file:/home/usuario/spark-labs/practica3/data/raw/ventas.csv
```

**Causa:** El script `generar_datasets.py` no se ejecutó, o se ejecutó desde un directorio diferente y los archivos se crearon en una ruta inesperada.

**Solución:**
```bash
# 1. Verificar que los archivos existen
ls -lh ~/spark-labs/practica3/data/raw/

# Si el directorio está vacío o no existe, regenerar los datasets:
python ~/spark-labs/practica3/generar_datasets.py

# 2. Verificar la ruta desde Python
import os
ruta = os.path.expanduser("~/spark-labs/practica3/data/raw/ventas.csv")
print(f"Archivo existe: {os.path.exists(ruta)}")
print(f"Ruta absoluta : {ruta}")

# 3. Si usas Windows, asegúrate de usar barras diagonales o raw strings:
# ruta_csv = r"C:\Users\tu_usuario\spark-labs\practica3\data\raw\ventas.csv"
```

---

### Problema 2: `show()` muestra columnas truncadas o caracteres especiales corruptos

**Síntoma:** Los nombres con tildes o caracteres especiales (ñ, á, é) aparecen como `?` o `â€˜` en la salida de `show()`. O las columnas de texto aparecen truncadas con `...` aunque los valores son cortos.

**Causa A (caracteres corruptos):** El archivo CSV se leyó con codificación incorrecta (por defecto `UTF-8` en Spark, pero el archivo podría haberse guardado con otra codificación en Windows).

**Causa B (truncado):** El parámetro `truncate=True` (valor por defecto) corta strings largos a 20 caracteres.

**Solución:**
```python
# Para caracteres corruptos: especificar codificación explícitamente
df_ventas = spark.read.csv(
    ruta_csv,
    header=True,
    schema=esquema_ventas,
    encoding="UTF-8"   # Forzar UTF-8 explícitamente
)

# Para Windows, si el archivo fue creado con encoding diferente:
# encoding="latin1"  o  encoding="cp1252"

# Para evitar truncado en show():
df_ventas.show(5, truncate=False)   # Sin truncado
df_ventas.show(5, truncate=50)      # Truncar a 50 caracteres en lugar de 20

# Verificar la codificación del archivo desde terminal:
# Linux/macOS:
# file -i ~/spark-labs/practica3/data/raw/ventas.csv
```

---

## Limpieza del Entorno

Ejecuta la siguiente celda al finalizar la práctica para liberar recursos:

```python
# Celda de Limpieza
print("Liberando recursos de Spark...")

# Eliminar DataFrames del caché (si se cachearon en algún momento)
# En esta práctica no usamos cache explícito, pero es buena práctica verificar
spark.catalog.clearCache()

# Detener la SparkSession
spark.stop()

print("✅ SparkSession detenida correctamente.")
print("   Los archivos de datos permanecen en ~/spark-labs/practica3/")
print("   Puedes eliminarlos manualmente si ya no los necesitas.")
```

Para eliminar los archivos generados (opcional):

```bash
# OPCIONAL: Eliminar todos los datos generados en esta práctica
# Ejecutar solo si ya no necesitas los datos para prácticas posteriores
rm -rf ~/spark-labs/practica3/data/processed/ventas.parquet

# Para eliminar TODO (incluyendo el notebook):
# rm -rf ~/spark-labs/practica3/
```

> **Nota:** Los archivos `ventas.csv` y `usuarios.json` en `data/raw/` serán reutilizados en prácticas posteriores (Práctica 4 en adelante). **No los elimines** a menos que puedas regenerarlos.

---

## Resumen

En esta práctica aplicaste los conceptos fundamentales de los DataFrames de PySpark trabajando con un dataset real de 12 000 registros de ventas. Los puntos clave que debes retener son:

| Concepto | Lo que aprendiste |
|---|---|
| **Creación de DataFrames** | Desde listas Python, CSV, JSON y Parquet usando `createDataFrame()` y `spark.read.*()` |
| **Esquemas explícitos** | `StructType` + `StructField` dan control total sobre tipos y nulabilidad, mejoran el rendimiento al evitar la inferencia |
| **Exploración** | `printSchema()`, `dtypes`, `describe()`, `show()`, `count()` son el kit estándar de auditoría de datos |
| **Inmutabilidad** | `withColumn()` retorna un nuevo DataFrame; el original no se modifica |
| **Filtrado** | `filter()` y `where()` son equivalentes; combina condiciones con `&`, `\|`, `isin()`, `between()` |
| **Ordenamiento** | `orderBy()` con `.asc()` / `.desc()` admite múltiples columnas |
| **Parquet vs CSV** | Parquet preserva el esquema, es columnar y reduce el tamaño ~75% |
| **DataFrame vs RDD** | Mismo resultado, menos código, y el Catalyst Optimizer optimiza automáticamente |

### Próximos Pasos

En la **Práctica 4** profundizarás en operaciones de agregación (`groupBy`, `agg`), joins entre DataFrames y las primeras funciones de ventana, trabajando con los mismos datasets de ventas y usuarios que generaste hoy.

### Recursos Adicionales

- [Documentación oficial PySpark — DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark — StructType y StructField](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html)
- [Chambers & Zaharia — Spark: The Definitive Guide (Cap. 5: Basic Structured Operations)](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)

---
