# Creación de RDD en PySpark

## 1. Metadatos

| Campo            | Detalle                              |
|------------------|--------------------------------------|
| **Duración**     | 60 minutos                           |
| **Complejidad**  | Fácil                                |
| **Nivel Bloom**  | Aplicar (Apply)                      |
| **Práctica**     | 2 de 9                               |
| **Módulo**       | 3 — Estructuras de datos en Spark    |

---

## 2. Descripción General

Esta práctica introduce el **Resilient Distributed Dataset (RDD)** como la abstracción de datos fundamental de Apache Spark. El estudiante aprenderá a inicializar correctamente un `SparkContext` y una `SparkSession`, y a crear RDDs utilizando tres métodos distintos: desde colecciones Python con `sc.parallelize()`, desde archivos de texto con `sc.textFile()`, y mediante transformaciones sobre RDDs existentes. Se implementará el caso clásico de **Word Count** para consolidar el uso de transformaciones y acciones, y se explorará la **Spark UI** para visualizar el DAG de ejecución generado por cada operación.

---

## 3. Objetivos de Aprendizaje

Al finalizar esta práctica, el estudiante será capaz de:

- [ ] Inicializar y configurar un `SparkContext` y una `SparkSession` para gestionar el ciclo de vida de una aplicación Spark.
- [ ] Crear RDDs utilizando `sc.parallelize()`, `sc.textFile()` y transformaciones sobre RDDs existentes.
- [ ] Distinguir las características fundamentales de los RDDs: inmutabilidad, tolerancia a fallos, evaluación perezosa (*lazy evaluation*) y particionamiento.
- [ ] Aplicar transformaciones básicas (`map`, `filter`, `flatMap`, `distinct`, `union`) y acciones (`collect`, `count`, `take`, `first`, `reduce`) sobre RDDs.
- [ ] Inspeccionar el DAG de ejecución en la Spark UI (puerto 4040) para comprender el plan de ejecución generado.

---

## 4. Prerrequisitos

### Conocimiento Previo

- Práctica 1 completada: entorno de Spark y PySpark funcional y verificado.
- Manejo de estructuras de datos en Python: listas, tuplas y diccionarios.
- Comprensión básica del paradigma MapReduce (entrada → transformación → salida).
- Familiaridad con Jupyter Notebook: ejecución de celdas, reinicio de kernel.

### Acceso y Recursos

- Equipo con Apache Spark 3.5.x y PySpark 3.5.x instalados y configurados.
- JDK 11 o 17 LTS activo en la variable de entorno `JAVA_HOME`.
- Puerto **4040** disponible en el equipo (verificar que no esté bloqueado por firewall).
- Dataset de texto para los ejercicios de `textFile()` (se genera en el paso 3 de esta práctica).
- Repositorio o carpeta del curso con los materiales de práctica accesibles.

---

## 5. Entorno de Laboratorio

### Requisitos de Hardware

| Componente       | Mínimo              | Recomendado          |
|------------------|---------------------|----------------------|
| RAM              | 8 GB                | 16 GB                |
| CPU              | 4 núcleos, 64 bits  | 4+ núcleos           |
| Almacenamiento   | 20 GB libres        | 30 GB libres         |

### Software Requerido

| Software         | Versión             |
|------------------|---------------------|
| Java (JDK)       | 11 o 17 LTS         |
| Apache Spark     | 3.5.x               |
| Python           | 3.10 o 3.11         |
| PySpark          | 3.5.x               |
| JupyterLab       | 7.x                 |
| findspark        | 2.0.1+              |

### Verificación del Entorno

Antes de comenzar, ejecuta los siguientes comandos en una terminal para confirmar que el entorno está correctamente configurado:

```bash
# Verificar versión de Java (debe mostrar 11 o 17)
java -version

# Verificar versión de Python
python --version

# Verificar que PySpark está instalado
python -c "import pyspark; print('PySpark versión:', pyspark.__version__)"

# Verificar que el puerto 4040 está disponible
# En Linux/macOS:
lsof -i :4040
# En Windows (PowerShell):
netstat -ano | findstr :4040
```

> **Nota para Windows:** Si `HADOOP_HOME` no está configurado, es posible que aparezcan advertencias al crear el `SparkContext`. Asegúrate de tener `winutils.exe` configurado según la guía de instalación del curso.

### Configuración de Memoria para Equipos con 8 GB RAM

Si tu equipo tiene exactamente 8 GB de RAM, agrega la siguiente configuración al crear la `SparkSession` en todos los ejercicios:

```python
.config("spark.driver.memory", "2g") \
.config("spark.executor.memory", "2g") \
```

### Iniciar JupyterLab

```bash
# Activar entorno virtual si corresponde
# En Linux/macOS:
source venv/bin/activate
# En Windows:
venv\Scripts\activate

# Iniciar JupyterLab
jupyter lab
```

Crea un nuevo notebook llamado **`Lab_03_00_01_RDD_PySpark.ipynb`** en tu carpeta de trabajo del curso.

---

## 6. Desarrollo Paso a Paso

---

### Paso 1: Inicialización de SparkContext y SparkSession

**Objetivo:** Crear correctamente los puntos de entrada de Spark, comprender la relación jerárquica entre `SparkSession` y `SparkContext`, y verificar que la sesión está activa.

#### Instrucciones

**1.1.** En la primera celda del notebook, importa las librerías necesarias y configura `findspark` para localizar la instalación de Spark:

```python
# Celda 1 — Importaciones y configuración de findspark
import findspark
findspark.init()  # Localiza automáticamente SPARK_HOME

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

print("Librerías importadas correctamente.")
```

**1.2.** En la siguiente celda, crea la `SparkSession` usando el patrón Builder moderno (enfoque recomendado para Spark 3.x). Ajusta la memoria si tu equipo tiene 8 GB de RAM:

```python
# Celda 2 — Creación de SparkSession (punto de entrada moderno)
spark = SparkSession.builder \
    .appName("Lab03_Creacion_RDD") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# El SparkContext está disponible a través de spark.sparkContext
sc = spark.sparkContext

print("=" * 50)
print(f"SparkSession activa: {spark is not None}")
print(f"Versión de Spark:     {spark.version}")
print(f"Nombre de la app:     {sc.appName}")
print(f"Master configurado:   {sc.master}")
print(f"SparkContext activo:  {sc is not None}")
print("=" * 50)
print(f"\nSpark UI disponible en: http://localhost:4040")
```

**1.3.** Verifica que `SparkSession` y `SparkContext` comparten el mismo objeto subyacente:

```python
# Celda 3 — Verificar relación entre SparkSession y SparkContext
sc_desde_session = spark.sparkContext

print("¿SparkContext es el mismo objeto accedido desde SparkSession?")
print(f"  sc is spark.sparkContext → {sc is sc_desde_session}")
print(f"\nID de la aplicación: {sc.applicationId}")
print(f"Directorio de logs:  {sc.getLocalProperty('spark.logDir') or 'Predeterminado'}")
```

**1.4.** Abre el navegador y navega a `http://localhost:4040` para confirmar que la Spark UI está activa.

#### Salida Esperada

```
==================================================
SparkSession activa: True
Versión de Spark:     3.5.x
Nombre de la app:     Lab03_Creacion_RDD
Master configurado:   local[*]
SparkContext activo:  True
==================================================

Spark UI disponible en: http://localhost:4040
```

```
¿SparkContext es el mismo objeto accedido desde SparkSession?
  sc is spark.sparkContext → True

ID de la aplicación: local-XXXXXXXXXX
```

#### Verificación

- [ ] La celda 2 se ejecuta sin errores ni advertencias críticas.
- [ ] `spark.version` muestra `3.5.x`.
- [ ] `sc is spark.sparkContext` devuelve `True`, confirmando la relación jerárquica estudiada en la lección.
- [ ] La Spark UI en `http://localhost:4040` muestra la aplicación **Lab03_Creacion_RDD** activa.

---

### Paso 2: Creación de RDDs con `sc.parallelize()`

**Objetivo:** Crear RDDs a partir de colecciones Python, explorar el particionamiento y verificar la propiedad de inmutabilidad.

#### Instrucciones

**2.1.** Crea un RDD básico desde una lista de números enteros:

```python
# Celda 4 — RDD desde lista de enteros
numeros = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Spark distribuye la colección en particiones automáticamente
rdd_numeros = sc.parallelize(numeros)

print(f"Tipo del objeto:         {type(rdd_numeros)}")
print(f"Número de particiones:   {rdd_numeros.getNumPartitions()}")
print(f"Primeros 5 elementos:    {rdd_numeros.take(5)}")
print(f"Total de elementos:      {rdd_numeros.count()}")
```

**2.2.** Crea un RDD especificando explícitamente el número de particiones y observa cómo se distribuyen los datos:

```python
# Celda 5 — RDD con particionamiento explícito
rdd_4_particiones = sc.parallelize(numeros, numPartitions=4)

print(f"Particiones solicitadas: 4")
print(f"Particiones reales:      {rdd_4_particiones.getNumPartitions()}")

# glom() agrupa los elementos de cada partición en una lista
distribucion = rdd_4_particiones.glom().collect()
print("\nDistribución de datos por partición:")
for i, particion in enumerate(distribucion):
    print(f"  Partición {i}: {particion}")
```

**2.3.** Crea un RDD desde una lista de tuplas (estructura clave-valor, útil para operaciones posteriores):

```python
# Celda 6 — RDD de tuplas (pares clave-valor)
ventas = [
    ("Producto_A", 150),
    ("Producto_B", 320),
    ("Producto_C", 85),
    ("Producto_A", 200),
    ("Producto_B", 175),
    ("Producto_C", 410),
]

rdd_ventas = sc.parallelize(ventas)

print(f"Tipo: {type(rdd_ventas)}")
print(f"Número de registros: {rdd_ventas.count()}")
print(f"Primer elemento: {rdd_ventas.first()}")
print(f"Todos los elementos:")
for item in rdd_ventas.collect():
    print(f"  {item}")
```

**2.4.** Demuestra la **inmutabilidad** de los RDDs: una transformación no modifica el RDD original, sino que crea uno nuevo:

```python
# Celda 7 — Demostración de inmutabilidad
rdd_original = sc.parallelize([1, 2, 3, 4, 5])

# map() crea un NUEVO RDD; rdd_original permanece sin cambios
rdd_duplicado = rdd_original.map(lambda x: x * 2)

print("RDD original (sin modificar):")
print(f"  {rdd_original.collect()}")

print("\nNuevo RDD (resultado de map x2):")
print(f"  {rdd_duplicado.collect()}")

print("\n¿Son el mismo objeto?")
print(f"  rdd_original is rdd_duplicado → {rdd_original is rdd_duplicado}")
```

#### Salida Esperada

```
Tipo del objeto:         <class 'pyspark.rdd.RDD'>
Número de particiones:   8   # varía según núcleos disponibles
Primeros 5 elementos:    [1, 2, 3, 4, 5]
Total de elementos:      10
```

```
Distribución de datos por partición:
  Partición 0: [1, 2]
  Partición 1: [3, 4]
  Partición 2: [5, 6]
  Partición 3: [7, 8, 9, 10]
```

```
RDD original (sin modificar):
  [1, 2, 3, 4, 5]

Nuevo RDD (resultado de map x2):
  [2, 4, 6, 8, 10]

¿Son el mismo objeto?
  rdd_original is rdd_duplicado → False
```

#### Verificación

- [ ] `sc.parallelize()` crea objetos de tipo `pyspark.rdd.RDD`.
- [ ] `glom().collect()` muestra que los datos están distribuidos entre las particiones.
- [ ] `rdd_original.collect()` devuelve `[1, 2, 3, 4, 5]` después de aplicar `map()`, confirmando inmutabilidad.
- [ ] `rdd_original is rdd_duplicado` devuelve `False`, confirmando que se creó un nuevo objeto.

---

### Paso 3: Creación de RDDs con `sc.textFile()`

**Objetivo:** Crear RDDs a partir de archivos de texto, comprender cómo Spark lee archivos línea por línea y preparar el dataset para el ejercicio de Word Count.

#### Instrucciones

**3.1.** Primero, crea el archivo de texto que usarás como fuente de datos. Ejecuta esta celda para generar el archivo `datos_texto.txt` en tu directorio de trabajo:

```python
# Celda 8 — Crear archivo de texto de práctica
contenido = """Apache Spark es un motor de procesamiento de datos distribuido
Spark soporta procesamiento batch y streaming en tiempo real
Los RDDs son la abstracción fundamental de Apache Spark
PySpark permite usar Spark con el lenguaje de programación Python
El procesamiento distribuido permite analizar grandes volúmenes de datos
Spark usa evaluación lazy para optimizar el plan de ejecución
Los DataFrames de Spark ofrecen una API de alto nivel sobre los RDDs
Apache Spark es más rápido que Hadoop MapReduce gracias al procesamiento en memoria
La tolerancia a fallos en Spark se logra mediante el linaje de los RDDs
PySpark integra Spark con el ecosistema de Python y sus librerías
"""

# Guardar el archivo en el directorio de trabajo actual
ruta_archivo = "datos_texto.txt"
with open(ruta_archivo, "w", encoding="utf-8") as f:
    f.write(contenido)

print(f"Archivo creado: {ruta_archivo}")
print(f"Ruta absoluta:  {os.path.abspath(ruta_archivo)}")
print(f"Líneas escritas: {len(contenido.strip().splitlines())}")
```

**3.2.** Crea un RDD leyendo el archivo con `sc.textFile()` y explora su estructura:

```python
# Celda 9 — Leer archivo de texto con sc.textFile()
ruta_absoluta = os.path.abspath("datos_texto.txt")
rdd_texto = sc.textFile(ruta_absoluta)

print(f"Tipo del RDD: {type(rdd_texto)}")
print(f"Número de particiones: {rdd_texto.getNumPartitions()}")
print(f"Total de líneas: {rdd_texto.count()}")
print("\nPrimeras 3 líneas del archivo:")
for linea in rdd_texto.take(3):
    print(f"  '{linea}'")
```

**3.3.** Crea el mismo RDD especificando el número mínimo de particiones:

```python
# Celda 10 — textFile() con número de particiones especificado
rdd_texto_2p = sc.textFile(ruta_absoluta, minPartitions=2)

print(f"Particiones con minPartitions=2: {rdd_texto_2p.getNumPartitions()}")

# Verificar que el contenido es idéntico
print(f"Total de líneas (mismo resultado): {rdd_texto_2p.count()}")
```

**3.4.** Demuestra la **evaluación perezosa (lazy evaluation)**: las transformaciones no se ejecutan hasta que se llama a una acción:

```python
# Celda 11 — Demostración de Lazy Evaluation
import time

print("Paso 1: Aplicando transformaciones (NO se ejecutan aún)...")
t0 = time.time()

# Estas transformaciones son LAZY: solo definen el plan de ejecución
rdd_paso1 = rdd_texto.flatMap(lambda linea: linea.split(" "))
rdd_paso2 = rdd_paso1.filter(lambda palabra: len(palabra) > 4)
rdd_paso3 = rdd_paso2.map(lambda palabra: palabra.lower())

t1 = time.time()
print(f"  Tiempo en definir transformaciones: {t1 - t0:.4f} segundos (casi instantáneo)")
print(f"  Tipo de rdd_paso3: {type(rdd_paso3)}")

print("\nPaso 2: Ejecutando acción count() (AQUÍ se ejecuta todo el plan)...")
t2 = time.time()
resultado = rdd_paso3.count()
t3 = time.time()

print(f"  Palabras de más de 4 caracteres: {resultado}")
print(f"  Tiempo de ejecución real: {t3 - t2:.4f} segundos")
print("\n→ La evaluación lazy permite a Spark optimizar el plan antes de ejecutar.")
```

#### Salida Esperada

```
Archivo creado: datos_texto.txt
Ruta absoluta:  /ruta/a/tu/directorio/datos_texto.txt
Líneas escritas: 10
```

```
Tipo del RDD: <class 'pyspark.rdd.RDD'>
Número de particiones: 2
Total de líneas: 10
Primeras 3 líneas del archivo:
  'Apache Spark es un motor de procesamiento de datos distribuido'
  'Spark soporta procesamiento batch y streaming en tiempo real'
  'Los RDDs son la abstracción fundamental de Apache Spark'
```

```
Paso 1: Aplicando transformaciones (NO se ejecutan aún)...
  Tiempo en definir transformaciones: 0.0012 segundos (casi instantáneo)
  Tipo de rdd_paso3: <class 'pyspark.rdd.PipelinedRDD'>

Paso 2: Ejecutando acción count() (AQUÍ se ejecuta todo el plan)...
  Palabras de más de 4 caracteres: XX
  Tiempo de ejecución real: 0.XXXX segundos

→ La evaluación lazy permite a Spark optimizar el plan antes de ejecutar.
```

#### Verificación

- [ ] El archivo `datos_texto.txt` se crea correctamente con 10 líneas.
- [ ] `sc.textFile()` crea un RDD donde cada elemento es una línea del archivo.
- [ ] El tiempo de definición de transformaciones es significativamente menor al tiempo de ejecución de la acción.
- [ ] `rdd_paso3` es de tipo `PipelinedRDD`, evidenciando que las transformaciones se encadenan sin ejecutarse.

---

### Paso 4: Transformaciones sobre RDDs

**Objetivo:** Aplicar las transformaciones fundamentales de RDDs (`map`, `filter`, `flatMap`, `distinct`, `union`) y comprender que cada una retorna un nuevo RDD sin ejecutarse inmediatamente.

#### Instrucciones

**4.1.** Aplica `map()` para transformar cada elemento de un RDD:

```python
# Celda 12 — Transformación map()
rdd_base = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# map(): aplica una función a CADA elemento, retorna un RDD del mismo tamaño
rdd_cuadrados = rdd_base.map(lambda x: x ** 2)
rdd_etiquetas = rdd_base.map(lambda x: (x, "par" if x % 2 == 0 else "impar"))

print("map() — Cuadrados de cada número:")
print(f"  {rdd_cuadrados.collect()}")

print("\nmap() — Clasificación par/impar:")
print(f"  {rdd_etiquetas.collect()}")
```

**4.2.** Aplica `filter()` para seleccionar elementos que cumplen una condición:

```python
# Celda 13 — Transformación filter()
# filter(): retiene solo los elementos donde la función devuelve True
rdd_pares = rdd_base.filter(lambda x: x % 2 == 0)
rdd_mayores_5 = rdd_base.filter(lambda x: x > 5)

print("filter() — Solo números pares:")
print(f"  {rdd_pares.collect()}")
print(f"  Cantidad: {rdd_pares.count()}")

print("\nfilter() — Solo números mayores a 5:")
print(f"  {rdd_mayores_5.collect()}")
print(f"  Cantidad: {rdd_mayores_5.count()}")
```

**4.3.** Aplica `flatMap()` para "aplanar" colecciones anidadas — clave para el procesamiento de texto:

```python
# Celda 14 — Transformación flatMap()
oraciones = [
    "Apache Spark es potente",
    "PySpark usa Python",
    "Spark procesa big data"
]
rdd_oraciones = sc.parallelize(oraciones)

# map() → lista de listas (anidado)
rdd_map_resultado = rdd_oraciones.map(lambda s: s.split(" "))
print("map() con split → lista de listas:")
print(f"  {rdd_map_resultado.collect()}")
print(f"  Elementos: {rdd_map_resultado.count()}")

# flatMap() → lista plana (aplanada)
rdd_palabras = rdd_oraciones.flatMap(lambda s: s.split(" "))
print("\nflatMap() con split → lista plana:")
print(f"  {rdd_palabras.collect()}")
print(f"  Elementos: {rdd_palabras.count()}")

print("\n→ flatMap() aplana un nivel de anidamiento.")
```

**4.4.** Aplica `distinct()` para eliminar duplicados y `union()` para combinar dos RDDs:

```python
# Celda 15 — Transformaciones distinct() y union()
rdd_con_duplicados = sc.parallelize([1, 2, 2, 3, 3, 3, 4, 5, 5])
rdd_set_a = sc.parallelize([1, 2, 3, 4])
rdd_set_b = sc.parallelize([3, 4, 5, 6])

# distinct(): elimina duplicados
rdd_unicos = rdd_con_duplicados.distinct()
print("distinct() — Valores únicos:")
print(f"  Original ({rdd_con_duplicados.count()} elementos): {sorted(rdd_con_duplicados.collect())}")
print(f"  Únicos   ({rdd_unicos.count()} elementos):   {sorted(rdd_unicos.collect())}")

# union(): combina dos RDDs (incluye duplicados entre ellos)
rdd_union = rdd_set_a.union(rdd_set_b)
print("\nunion() — Combinación de dos RDDs (con duplicados):")
print(f"  Set A: {rdd_set_a.collect()}")
print(f"  Set B: {rdd_set_b.collect()}")
print(f"  Unión: {sorted(rdd_union.collect())} ({rdd_union.count()} elementos)")

# union() + distinct() para obtener la unión de conjuntos matemática
rdd_union_unica = rdd_set_a.union(rdd_set_b).distinct()
print(f"\n  Unión sin duplicados: {sorted(rdd_union_unica.collect())}")
```

#### Salida Esperada

```
map() — Cuadrados de cada número:
  [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

map() — Clasificación par/impar:
  [(1, 'impar'), (2, 'par'), (3, 'impar'), ..., (10, 'par')]
```

```
flatMap() con split → lista plana:
  ['Apache', 'Spark', 'es', 'potente', 'PySpark', 'usa', 'Python', 'Spark', 'procesa', 'big', 'data']
  Elementos: 11
```

```
distinct() — Valores únicos:
  Original (9 elementos): [1, 2, 2, 3, 3, 3, 4, 5, 5]
  Únicos   (5 elementos): [1, 2, 3, 4, 5]

union() — Combinación de dos RDDs (con duplicados):
  Set A: [1, 2, 3, 4]
  Set B: [3, 4, 5, 6]
  Unión: [1, 2, 3, 3, 4, 4, 5, 6] (8 elementos)

  Unión sin duplicados: [1, 2, 3, 4, 5, 6]
```

#### Verificación

- [ ] `map()` retorna un RDD del mismo tamaño que el original.
- [ ] `flatMap()` retorna más elementos que `map()` al aplanar las listas internas.
- [ ] `distinct()` reduce el número de elementos eliminando duplicados.
- [ ] `union()` combina los elementos de ambos RDDs incluyendo los valores repetidos.

---

### Paso 5: Acciones sobre RDDs

**Objetivo:** Ejecutar acciones que desencadenan el plan de ejecución y retornan resultados al driver, comprendiendo la diferencia fundamental con las transformaciones.

#### Instrucciones

**5.1.** Explora las acciones fundamentales de inspección:

```python
# Celda 16 — Acciones de inspección: collect, take, first, count
rdd_datos = sc.parallelize([15, 3, 8, 22, 7, 45, 12, 1, 33, 9])

print("Acciones de inspección sobre el RDD:")
print(f"  count()      → Total de elementos: {rdd_datos.count()}")
print(f"  first()      → Primer elemento:    {rdd_datos.first()}")
print(f"  take(3)      → Primeros 3:         {rdd_datos.take(3)}")
print(f"  takeSample() → 4 muestras aleatorias: {rdd_datos.takeSample(False, 4, seed=42)}")

# collect() trae TODOS los elementos al driver — úsalo con precaución en datasets grandes
todos = rdd_datos.collect()
print(f"\n  collect()    → Todos los elementos: {todos}")
print(f"  ⚠️  collect() trae todos los datos al driver. Evitar en datasets grandes.")
```

**5.2.** Aplica la acción `reduce()` para agregar todos los elementos de un RDD:

```python
# Celda 17 — Acción reduce()
rdd_numeros = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# reduce(): aplica una función binaria asociativa a todos los elementos
suma_total = rdd_numeros.reduce(lambda a, b: a + b)
producto_total = rdd_numeros.reduce(lambda a, b: a * b)
maximo = rdd_numeros.reduce(lambda a, b: a if a > b else b)

print("reduce() — Operaciones de agregación:")
print(f"  Suma total:     {suma_total}")
print(f"  Producto total: {producto_total}")
print(f"  Máximo valor:   {maximo}")

# Equivalente más eficiente para suma y máximo
print(f"\n  sum() (acción directa):   {rdd_numeros.sum()}")
print(f"  max() (acción directa):   {rdd_numeros.max()}")
print(f"  min() (acción directa):   {rdd_numeros.min()}")
print(f"  mean() (acción directa):  {rdd_numeros.mean()}")
```

**5.3.** Usa `saveAsTextFile()` para persistir resultados — acción que escribe datos al almacenamiento:

```python
# Celda 18 — Acción saveAsTextFile()
import shutil

rdd_resultado = sc.parallelize(["línea 1 del resultado",
                                 "línea 2 del resultado",
                                 "línea 3 del resultado"])

ruta_salida = "output_rdd_resultado"

# Eliminar directorio previo si existe (Spark no sobreescribe)
if os.path.exists(ruta_salida):
    shutil.rmtree(ruta_salida)

# saveAsTextFile() es una ACCIÓN: ejecuta todo el plan y escribe al disco
rdd_resultado.saveAsTextFile(ruta_salida)

print(f"Datos guardados en: {ruta_salida}/")
print("Archivos generados:")
for archivo in sorted(os.listdir(ruta_salida)):
    print(f"  {archivo}")
```

#### Salida Esperada

```
Acciones de inspección sobre el RDD:
  count()      → Total de elementos: 10
  first()      → Primer elemento:    15
  take(3)      → Primeros 3:         [15, 3, 8]
  takeSample() → 4 muestras aleatorias: [22, 1, 45, 9]

  collect()    → Todos los elementos: [15, 3, 8, 22, 7, 45, 12, 1, 33, 9]
  ⚠️  collect() trae todos los datos al driver. Evitar en datasets grandes.
```

```
reduce() — Operaciones de agregación:
  Suma total:     55
  Producto total: 3628800
  Máximo valor:   10

  sum() (acción directa):   55
  max() (acción directa):   10
  min() (acción directa):   1
  mean() (acción directa):  5.5
```

```
Datos guardados en: output_rdd_resultado/
Archivos generados:
  _SUCCESS
  part-00000
  part-00001
```

#### Verificación

- [ ] `count()`, `first()`, `take()` y `collect()` retornan valores inmediatamente.
- [ ] `reduce()` calcula la suma correcta: 1+2+...+10 = 55.
- [ ] `saveAsTextFile()` crea un directorio con archivos `part-XXXXX` y un archivo `_SUCCESS`.
- [ ] En la Spark UI (`http://localhost:4040` → pestaña **Jobs**), cada acción ejecutada aparece como un job completado.

---

### Paso 6: Caso Práctico — Word Count con RDDs

**Objetivo:** Implementar el caso clásico de conteo de palabras (Word Count) aplicando una cadena completa de transformaciones y una acción, y visualizar el DAG generado en la Spark UI.

#### Instrucciones

**6.1.** Implementa el Word Count completo sobre el archivo `datos_texto.txt`:

```python
# Celda 19 — Word Count: implementación completa
print("=" * 55)
print("WORD COUNT — Conteo de palabras con RDDs")
print("=" * 55)

# Paso 1: Leer el archivo (ya existe desde el Paso 3)
rdd_lineas = sc.textFile(os.path.abspath("datos_texto.txt"))
print(f"\nPaso 1 — Líneas leídas: {rdd_lineas.count()}")

# Paso 2: flatMap — dividir cada línea en palabras
rdd_palabras_raw = rdd_lineas.flatMap(lambda linea: linea.split(" "))
print(f"Paso 2 — Palabras (con duplicados): {rdd_palabras_raw.count()}")

# Paso 3: map — normalizar a minúsculas y limpiar puntuación básica
import re
rdd_palabras_limpias = rdd_palabras_raw \
    .map(lambda p: re.sub(r'[^a-záéíóúüñA-ZÁÉÍÓÚÜÑ]', '', p).lower()) \
    .filter(lambda p: len(p) > 2)  # Eliminar palabras muy cortas
print(f"Paso 3 — Palabras limpias (>2 chars): {rdd_palabras_limpias.count()}")

# Paso 4: map — convertir cada palabra en par (palabra, 1)
rdd_pares = rdd_palabras_limpias.map(lambda palabra: (palabra, 1))
print(f"Paso 4 — Pares (palabra, 1): {rdd_pares.take(5)}")

# Paso 5: reduceByKey — sumar los conteos por palabra
rdd_conteos = rdd_pares.reduceByKey(lambda a, b: a + b)
print(f"Paso 5 — Palabras únicas contadas: {rdd_conteos.count()}")

# Paso 6: sortBy — ordenar por frecuencia descendente (ACCIÓN al final)
rdd_ordenado = rdd_conteos.sortBy(lambda par: par[1], ascending=False)

print("\n--- Top 15 palabras más frecuentes ---")
top_15 = rdd_ordenado.take(15)
for palabra, conteo in top_15:
    barra = "█" * conteo
    print(f"  {palabra:<20} {conteo:>3}  {barra}")
```

**6.2.** Visualiza el linaje (DAG) del RDD con `toDebugString()`:

```python
# Celda 20 — Inspeccionar el linaje del RDD
print("Linaje del RDD (DAG de transformaciones):")
print("-" * 50)
# toDebugString() muestra el plan de ejecución completo
linaje = rdd_ordenado.toDebugString().decode("utf-8")
print(linaje)
print("-" * 50)
print("\n→ Cada nivel de indentación representa una etapa del DAG.")
print("→ Abre http://localhost:4040 → Jobs → DAG Visualization para verlo gráficamente.")
```

**6.3.** Guarda el resultado del Word Count en un archivo de salida:

```python
# Celda 21 — Guardar resultados del Word Count
import shutil

ruta_wc = "output_word_count"
if os.path.exists(ruta_wc):
    shutil.rmtree(ruta_wc)

# Formatear como "palabra:conteo" antes de guardar
rdd_formateado = rdd_ordenado.map(lambda par: f"{par[0]}:{par[1]}")
rdd_formateado.saveAsTextFile(ruta_wc)

print(f"Resultados guardados en: {ruta_wc}/")
print(f"Archivos generados: {os.listdir(ruta_wc)}")
```

#### Salida Esperada

```
=======================================================
WORD COUNT — Conteo de palabras con RDDs
=======================================================

Paso 1 — Líneas leídas: 10
Paso 2 — Palabras (con duplicados): ~100
Paso 3 — Palabras limpias (>2 chars): ~80
Paso 4 — Pares (palabra, 1): [('apache', 1), ('spark', 1), ...]
Paso 5 — Palabras únicas contadas: ~50

--- Top 15 palabras más frecuentes ---
  spark                5  █████
  procesamiento        4  ████
  apache               3  ███
  datos                3  ███
  pyspark              3  ███
  ...
```

#### Verificación

- [ ] El pipeline completo de Word Count se ejecuta sin errores.
- [ ] Las palabras más frecuentes incluyen "spark", "procesamiento" y "apache" (presentes múltiples veces en el texto).
- [ ] `toDebugString()` muestra múltiples niveles de transformaciones encadenadas.
- [ ] En la Spark UI → pestaña **Jobs**, se puede observar el DAG con las etapas del Word Count.
- [ ] El directorio `output_word_count/` contiene los archivos de resultado.

---

### Paso 7: Inspección de la Spark UI

**Objetivo:** Navegar por la Spark UI para identificar jobs, stages, tasks y el DAG de ejecución generado por las operaciones anteriores.

#### Instrucciones

**7.1.** Ejecuta una operación adicional con un nombre descriptivo para identificarla fácilmente en la UI:

```python
# Celda 22 — Operación etiquetada para la Spark UI
# setJobDescription permite identificar el job en la Spark UI
sc.setJobDescription("Demo_DAG_Visualizacion")

rdd_demo = sc.parallelize(range(1, 101)) \
    .filter(lambda x: x % 3 == 0) \
    .map(lambda x: x * x) \
    .filter(lambda x: x > 100)

resultado_demo = rdd_demo.collect()
print(f"Números divisibles por 3 al cuadrado (>100): {len(resultado_demo)} elementos")
print(f"Primeros 5: {resultado_demo[:5]}")

# Limpiar la descripción
sc.setJobDescription(None)
```

**7.2.** Guía de navegación por la Spark UI:

```
GUÍA DE NAVEGACIÓN — Spark UI (http://localhost:4040)
══════════════════════════════════════════════════════

1. Pestaña [Jobs]
   ├── Lista de todos los jobs ejecutados (uno por acción)
   ├── Columna "Description": muestra el nombre del job
   ├── Columna "Duration": tiempo total de ejecución
   └── Click en un job → ver detalles de stages

2. Pestaña [Stages]
   ├── Cada stage corresponde a un conjunto de transformaciones
   │   que no requieren shuffle entre particiones
   ├── Un shuffle (ej: reduceByKey) divide el job en múltiples stages
   └── Click en un stage → ver tasks individuales y métricas

3. Pestaña [Jobs] → Click en job → [DAG Visualization]
   ├── Muestra el grafo acíclico dirigido de la ejecución
   ├── Cada nodo es una transformación
   └── Los bordes muestran el flujo de datos entre transformaciones

4. Pestaña [Storage]
   └── Muestra RDDs persistidos en caché (relevante en prácticas posteriores)

5. Pestaña [Environment]
   └── Configuración activa de Spark (memoria, particiones, etc.)
```

**7.3.** Documenta lo que observas en la Spark UI completando esta celda:

```python
# Celda 23 — Documentación de observaciones en la Spark UI
observaciones = {
    "total_jobs_ejecutados": "Completar al revisar la UI",
    "job_word_count_stages": "Completar: ¿cuántos stages tuvo el Word Count?",
    "job_word_count_duracion_ms": "Completar: duración en ms",
    "shuffle_detectado_en": "Completar: ¿qué operación causó el shuffle?",
    "particiones_observadas": "Completar: ¿cuántas tasks por stage?"
}

print("Observaciones de la Spark UI:")
for clave, valor in observaciones.items():
    print(f"  {clave}: {valor}")

print("\n→ Completa este diccionario con los valores observados en http://localhost:4040")
```

#### Verificación

- [ ] La Spark UI en `http://localhost:4040` muestra todos los jobs ejecutados durante la práctica.
- [ ] El job del Word Count muestra múltiples stages (al menos 2, por el `reduceByKey` que causa un shuffle).
- [ ] La pestaña **DAG Visualization** muestra el grafo de transformaciones del Word Count.
- [ ] Se puede identificar qué operación causó la división en stages (shuffle boundary).

---

## 7. Validación y Pruebas

Ejecuta la siguiente celda de validación al final de la práctica para confirmar que todos los objetivos se cumplieron correctamente:

```python
# Celda 24 — Validación final de la práctica
print("=" * 60)
print("VALIDACIÓN FINAL — Lab 03-00-01: Creación de RDD en PySpark")
print("=" * 60)

resultados = {}

# Test 1: SparkSession y SparkContext activos
try:
    assert spark is not None, "SparkSession no está activa"
    assert sc is not None, "SparkContext no está activo"
    assert sc is spark.sparkContext, "SparkContext no coincide con el de SparkSession"
    resultados["1. SparkSession y SparkContext"] = "✅ PASS"
except AssertionError as e:
    resultados["1. SparkSession y SparkContext"] = f"❌ FAIL: {e}"

# Test 2: sc.parallelize() crea RDDs correctamente
try:
    rdd_test = sc.parallelize([10, 20, 30, 40, 50])
    assert rdd_test.count() == 5
    assert rdd_test.first() == 10
    resultados["2. sc.parallelize()"] = "✅ PASS"
except Exception as e:
    resultados["2. sc.parallelize()"] = f"❌ FAIL: {e}"

# Test 3: sc.textFile() lee el archivo correctamente
try:
    rdd_file_test = sc.textFile(os.path.abspath("datos_texto.txt"))
    assert rdd_file_test.count() == 10, f"Esperado 10 líneas, obtuvo {rdd_file_test.count()}"
    resultados["3. sc.textFile()"] = "✅ PASS"
except Exception as e:
    resultados["3. sc.textFile()"] = f"❌ FAIL: {e}"

# Test 4: Transformaciones map, filter, flatMap
try:
    rdd_t = sc.parallelize([1, 2, 3, 4, 5])
    assert rdd_t.map(lambda x: x * 2).collect() == [2, 4, 6, 8, 10]
    assert rdd_t.filter(lambda x: x % 2 == 0).collect() == [2, 4]
    assert sc.parallelize(["a b", "c d"]).flatMap(lambda s: s.split()).count() == 4
    resultados["4. Transformaciones (map, filter, flatMap)"] = "✅ PASS"
except Exception as e:
    resultados["4. Transformaciones (map, filter, flatMap)"] = f"❌ FAIL: {e}"

# Test 5: Acciones collect, count, take, reduce
try:
    rdd_a = sc.parallelize([1, 2, 3, 4, 5])
    assert rdd_a.count() == 5
    assert rdd_a.first() == 1
    assert rdd_a.take(3) == [1, 2, 3]
    assert rdd_a.reduce(lambda a, b: a + b) == 15
    resultados["5. Acciones (collect, count, take, reduce)"] = "✅ PASS"
except Exception as e:
    resultados["5. Acciones (collect, count, take, reduce)"] = f"❌ FAIL: {e}"

# Test 6: Word Count produce resultados válidos
try:
    rdd_wc_test = sc.parallelize(["spark spark python", "spark python java"])
    conteos = rdd_wc_test \
        .flatMap(lambda l: l.split()) \
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collectAsMap()
    assert conteos["spark"] == 3, f"Esperado 3, obtuvo {conteos.get('spark')}"
    assert conteos["python"] == 2
    resultados["6. Word Count (reduceByKey)"] = "✅ PASS"
except Exception as e:
    resultados["6. Word Count (reduceByKey)"] = f"❌ FAIL: {e}"

# Test 7: Inmutabilidad — transformación no modifica el RDD original
try:
    rdd_orig = sc.parallelize([1, 2, 3])
    rdd_nuevo = rdd_orig.map(lambda x: x * 10)
    assert rdd_orig.collect() == [1, 2, 3], "El RDD original fue modificado"
    assert rdd_nuevo.collect() == [10, 20, 30]
    assert rdd_orig is not rdd_nuevo
    resultados["7. Inmutabilidad de RDDs"] = "✅ PASS"
except Exception as e:
    resultados["7. Inmutabilidad de RDDs"] = f"❌ FAIL: {e}"

# Mostrar resumen
print()
aprobados = 0
for test, resultado in resultados.items():
    print(f"  {test}: {resultado}")
    if "PASS" in resultado:
        aprobados += 1

print()
print(f"Resultado: {aprobados}/{len(resultados)} pruebas aprobadas")
if aprobados == len(resultados):
    print("🎉 ¡Todos los objetivos de la práctica completados exitosamente!")
else:
    print("⚠️  Revisa los tests fallidos antes de continuar.")
print("=" * 60)
```

#### Salida Esperada de Validación

```
============================================================
VALIDACIÓN FINAL — Lab 03-00-01: Creación de RDD en PySpark
============================================================

  1. SparkSession y SparkContext: ✅ PASS
  2. sc.parallelize(): ✅ PASS
  3. sc.textFile(): ✅ PASS
  4. Transformaciones (map, filter, flatMap): ✅ PASS
  5. Acciones (collect, count, take, reduce): ✅ PASS
  6. Word Count (reduceByKey): ✅ PASS
  7. Inmutabilidad de RDDs: ✅ PASS

Resultado: 7/7 pruebas aprobadas
🎉 ¡Todos los objetivos de la práctica completados exitosamente!
============================================================
```

---

## 8. Solución de Problemas

### Problema 1: Error al crear SparkContext — "Cannot run multiple SparkContexts at once"

**Síntoma:**
```
ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=...) 
created by ... at ...
```

**Causa:** Se intentó crear un nuevo `SparkContext` directamente (con `SparkContext(conf=conf)`) cuando ya existe uno activo, ya sea porque el kernel de Jupyter no fue reiniciado entre ejecuciones o porque se ejecutó la celda de inicialización más de una vez.

**Solución:**
```python
# Opción 1 (RECOMENDADA): Usar SparkSession.builder.getOrCreate()
# getOrCreate() reutiliza la sesión existente en lugar de crear una nueva
spark = SparkSession.builder \
    .appName("Lab03_Creacion_RDD") \
    .master("local[*]") \
    .getOrCreate()
sc = spark.sparkContext  # Acceder al SparkContext a través de SparkSession

# Opción 2: Detener el contexto existente antes de crear uno nuevo
# (solo si necesitas una configuración diferente)
try:
    sc_existente = SparkContext.getOrCreate()
    sc_existente.stop()
except Exception:
    pass
# Ahora puedes crear uno nuevo

# Opción 3: Reiniciar el kernel de Jupyter
# Kernel → Restart Kernel → ejecutar las celdas de nuevo desde el principio
```

> **Prevención:** Siempre usa `SparkSession.builder.getOrCreate()` como punto de entrada. Nunca crees un `SparkContext` directamente en entornos interactivos como Jupyter.

---

### Problema 2: `sc.textFile()` falla con "Path does not exist" o error de permisos

**Síntoma:**
```
AnalysisException: Path does not exist: file:/ruta/al/archivo/datos_texto.txt
# o en Windows:
java.io.IOException: Failed to create directory ... Access is denied
```

**Causa (Linux/macOS):** La ruta al archivo es incorrecta o el archivo no fue creado en el paso 3. Esto ocurre cuando el notebook se ejecuta desde un directorio diferente al esperado.

**Causa (Windows):** `HADOOP_HOME` no está configurado correctamente o `winutils.exe` no está presente, lo que impide que Spark gestione el sistema de archivos local.

**Solución:**
```python
# Diagnóstico: verificar el directorio de trabajo actual
import os
print("Directorio de trabajo actual:", os.getcwd())
print("Archivos en el directorio:", os.listdir("."))

# Solución 1: Usar ruta absoluta explícita
ruta_absoluta = os.path.abspath("datos_texto.txt")
print("Ruta absoluta:", ruta_absoluta)
print("¿El archivo existe?", os.path.exists(ruta_absoluta))

# Si el archivo no existe, recrearlo (volver al Paso 3, Celda 8)

# Leer con ruta absoluta verificada
rdd_texto = sc.textFile(ruta_absoluta)
print("Líneas leídas:", rdd_texto.count())

# Solución para Windows (si el problema es HADOOP_HOME):
# Agregar al inicio del notebook, ANTES de importar PySpark:
# import os
# os.environ["HADOOP_HOME"] = "C:\\hadoop"  # Ruta donde está winutils.exe
# os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
```

> **Prevención:** Siempre usa `os.path.abspath()` para construir rutas de archivo antes de pasarlas a `sc.textFile()`. En Windows, verifica que `HADOOP_HOME` apunte al directorio que contiene la carpeta `bin/winutils.exe`.

---

## 9. Limpieza del Entorno

Ejecuta las siguientes celdas al finalizar la práctica para liberar recursos y dejar el entorno en un estado limpio:

```python
# Celda 25 — Limpieza de archivos generados
import shutil
import os

archivos_a_eliminar = [
    "datos_texto.txt",
]
directorios_a_eliminar = [
    "output_rdd_resultado",
    "output_word_count",
]

print("Limpiando archivos generados durante la práctica...")

for archivo in archivos_a_eliminar:
    if os.path.exists(archivo):
        os.remove(archivo)
        print(f"  ✓ Eliminado: {archivo}")
    else:
        print(f"  - No encontrado: {archivo}")

for directorio in directorios_a_eliminar:
    if os.path.exists(directorio):
        shutil.rmtree(directorio)
        print(f"  ✓ Eliminado directorio: {directorio}/")
    else:
        print(f"  - No encontrado: {directorio}/")

print("\nLimpieza de archivos completada.")
```

```python
# Celda 26 — Detener la SparkSession
print("Deteniendo SparkSession...")
spark.stop()
print("✓ SparkSession detenida correctamente.")
print("  La Spark UI en http://localhost:4040 ya no estará disponible.")
print("\n→ Para continuar con la siguiente práctica, reinicia el kernel de Jupyter.")
```

> **Importante:** Siempre detén la `SparkSession` al finalizar. Esto libera los recursos de la JVM y evita conflictos al iniciar la siguiente práctica. En Jupyter, también es recomendable reiniciar el kernel después de `spark.stop()`.

---

## 10. Resumen

En esta práctica has explorado los fundamentos del **Resilient Distributed Dataset (RDD)** como abstracción central de Apache Spark:

| Concepto                  | Lo que aprendiste                                                                                  |
|---------------------------|-----------------------------------------------------------------------------------------------------|
| **SparkSession**          | Punto de entrada moderno (Spark 2.x+) que encapsula `SparkContext`; usar `.getOrCreate()`         |
| **SparkContext**          | Accesible vía `spark.sparkContext`; gestiona RDDs y la conexión con el entorno de ejecución        |
| **sc.parallelize()**      | Crea RDDs desde colecciones Python; permite especificar el número de particiones                   |
| **sc.textFile()**         | Crea RDDs desde archivos de texto; cada línea es un elemento del RDD                               |
| **Inmutabilidad**         | Las transformaciones nunca modifican el RDD original; siempre crean uno nuevo                      |
| **Lazy Evaluation**       | Las transformaciones definen el plan; las acciones lo ejecutan                                     |
| **Transformaciones**      | `map`, `filter`, `flatMap`, `distinct`, `union` — retornan un nuevo RDD                            |
| **Acciones**              | `collect`, `count`, `take`, `first`, `reduce`, `saveAsTextFile` — desencadenan la ejecución        |
| **Word Count**            | Patrón clásico: `flatMap` → `map` → `reduceByKey` → `sortBy`                                      |
| **Spark UI**              | Herramienta de monitoreo en `http://localhost:4040`; visualiza jobs, stages y el DAG               |

### Conceptos Clave para Recordar

- **Un solo `SparkContext` por JVM**: nunca crees dos directamente; usa `getOrCreate()`.
- **Lazy evaluation**: las transformaciones son baratas (solo definen el plan); las acciones son costosas (ejecutan el plan).
- **Particionamiento**: controla el paralelismo; más particiones = más paralelismo (hasta el límite de los núcleos disponibles).
- **`collect()` con precaución**: trae todos los datos al driver; en datasets grandes puede causar `OutOfMemoryError`.

### Recursos Adicionales

- [Guía de programación de RDDs — Apache Spark 3.x](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [API de PySpark RDD](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html)
- [Documentación de SparkContext en PySpark](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.html)
- [Databricks: Understanding Lazy Evaluation in Apache Spark](https://www.databricks.com/glossary/lazy-evaluation)

### Próxima Práctica

La **Práctica 3** profundizará en las **transformaciones avanzadas de RDDs**: operaciones de clave-valor (`groupByKey`, `reduceByKey`, `join`), operaciones de conjunto y el uso de variables compartidas (`broadcast variables` y `acumuladores`). El conocimiento de `SparkContext` y la API básica de RDDs adquirido en esta práctica será la base directa para esos ejercicios.

---
