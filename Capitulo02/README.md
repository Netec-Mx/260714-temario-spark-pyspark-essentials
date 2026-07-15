---LAB_START---
LAB_ID: 02-00-01
---MARKDOWN---
# Práctica 1 — Instalación de Ambiente (Spark, Python y Bibliotecas)

## Metadatos

| Campo            | Detalle                                      |
|------------------|----------------------------------------------|
| **Duración**     | 60 minutos                                   |
| **Complejidad**  | Fácil                                        |
| **Nivel Bloom**  | Aplicar (Apply)                              |
| **Módulo**       | 2 — Fundamentos de Python y PySpark          |
| **Lección base** | 2.1 — Generalidades de Python                |

---

## Descripción General

En esta práctica instalarás y configurarás el stack tecnológico completo que utilizarás durante todo el curso: Java JDK, Apache Spark 3.5.x, Python 3.10/3.11, PySpark y las bibliotecas de análisis de datos esenciales. Al finalizar, tendrás un entorno funcional de JupyterLab capaz de inicializar un `SparkSession` en modo local y ejecutar operaciones básicas sobre RDDs y DataFrames. También explorarás los tipos de datos y estructuras de Python que son el fundamento de cualquier pipeline PySpark, conectando directamente los conceptos de la lección 2.1 con el entorno real de trabajo.

---

## Objetivos de Aprendizaje

Al completar esta práctica serás capaz de:

- [ ] Instalar y configurar Java JDK 11/17, Apache Spark 3.5.x y Python 3.10/3.11 con las variables de entorno correctas (`JAVA_HOME`, `SPARK_HOME`, `PATH`).
- [ ] Instalar y validar PySpark, findspark, pandas, NumPy y matplotlib en un entorno virtual Python.
- [ ] Inicializar un `SparkSession` en modo local desde JupyterLab y ejecutar una operación básica de conteo sobre un RDD.
- [ ] Aplicar los tipos de datos y estructuras de Python (listas, tuplas, diccionarios) para preparar datos que serán procesados por Spark.
- [ ] Verificar la integración completa del entorno mediante un script de validación que prueba todos los componentes instalados.

---

## Prerrequisitos

### Conocimiento previo
- Manejo básico del sistema operativo (Windows, macOS o Linux).
- Experiencia con la línea de comandos: terminal en Linux/macOS o PowerShell/CMD en Windows.
- Conocimientos básicos de Python: variables, tipos de datos, funciones y estructuras de control (contenido de la lección 2.1).
- Acceso a Internet para descarga de instaladores y paquetes pip.

### Acceso y permisos
- Permisos de administrador/root en el equipo para instalar software.
- Al menos **20 GB** de espacio libre en disco.
- Mínimo **8 GB de RAM** disponibles (16 GB recomendados).
- Puerto **4040** disponible (Spark UI); si está ocupado, Spark usará 4041 o 4042 automáticamente.

---

## Entorno de Laboratorio

### Hardware recomendado

| Componente    | Mínimo              | Recomendado                    |
|---------------|---------------------|--------------------------------|
| RAM           | 8 GB                | 16 GB                          |
| CPU           | 4 núcleos, 64 bits  | 6–8 núcleos con virtualización |
| Almacenamiento| 20 GB libres        | 40 GB libres (SSD preferido)   |
| Red           | Acceso a Internet   | Conexión estable ≥ 10 Mbps     |

### Software a instalar

| Componente          | Versión objetivo     | Notas                                      |
|---------------------|----------------------|--------------------------------------------|
| Java JDK            | 11 LTS o 17 LTS      | **No usar Java 8** — incompatible con Spark 3.5.x |
| Apache Spark        | 3.5.x                | Descarga: spark.apache.org                 |
| Python              | 3.10 o 3.11          | Incluye `pip` y `venv`                     |
| PySpark             | 3.5.x                | Debe coincidir con versión de Spark        |
| JupyterLab          | 7.x                  | Interfaz principal del curso               |
| findspark           | 2.0.1+               | Localiza Spark desde Jupyter               |
| pandas              | 2.0+                 | Análisis de datos en memoria               |
| NumPy               | 1.24+                | Cómputo numérico                           |
| matplotlib          | 3.7+                 | Visualización de datos                     |

> **⚠️ Nota de compatibilidad crítica:** Las versiones de Apache Spark, PySpark y Java deben ser exactamente compatibles. Spark 3.5.x requiere Java 11 o 17. Usar Java 8 puede generar errores silenciosos difíciles de depurar.

---

## Pasos del Laboratorio

---

### Paso 1 — Instalación de Java JDK

**Objetivo:** Instalar Java JDK 11 o 17 LTS y configurar la variable de entorno `JAVA_HOME`, requisito previo indispensable para ejecutar Apache Spark.

#### Instrucciones

**Opción A — Windows**

1. Descarga el instalador de **Eclipse Temurin JDK 17** (distribución OpenJDK recomendada) desde:
   ```
   https://adoptium.net/temurin/releases/?version=17
   ```
   Selecciona: Windows → x64 → JDK → `.msi`

2. Ejecuta el instalador `.msi`. En la pantalla de opciones, asegúrate de marcar:
   - ✅ **Set JAVA_HOME variable**
   - ✅ **Add to PATH**

3. Abre **PowerShell** y verifica:
   ```powershell
   java -version
   echo $env:JAVA_HOME
   ```

4. Si `JAVA_HOME` no se configuró automáticamente, configúrala manualmente:
   ```powershell
   # Abrir Variables de Entorno del Sistema (como Administrador)
   # O ejecutar desde PowerShell (requiere reinicio):
   [System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-17.0.x.x-hotspot", "Machine")
   ```

**Opción B — macOS**

1. Instala [Homebrew](https://brew.sh) si no lo tienes:
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. Instala Temurin JDK 17:
   ```bash
   brew install --cask temurin@17
   ```

3. Configura `JAVA_HOME` en tu shell (agrega al final de `~/.zshrc` o `~/.bash_profile`):
   ```bash
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   export PATH="$JAVA_HOME/bin:$PATH"
   ```

4. Recarga el perfil y verifica:
   ```bash
   source ~/.zshrc
   java -version
   echo $JAVA_HOME
   ```

**Opción C — Ubuntu / Debian Linux**

1. Actualiza el sistema e instala OpenJDK 17:
   ```bash
   sudo apt update && sudo apt upgrade -y
   sudo apt install openjdk-17-jdk -y
   ```

2. Configura `JAVA_HOME` (agrega al final de `~/.bashrc`):
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
   export PATH="$JAVA_HOME/bin:$PATH"
   ```

3. Recarga y verifica:
   ```bash
   source ~/.bashrc
   java -version
   echo $JAVA_HOME
   ```

#### Salida esperada

```
openjdk version "17.0.x" 2024-xx-xx
OpenJDK Runtime Environment Temurin-17.0.x+x (build 17.0.x+x)
OpenJDK 64-Bit Server VM Temurin-17.0.x+x (build 17.0.x+x, mixed mode, sharing)
```

#### Verificación

```bash
# La versión debe comenzar con 17.x.x (o 11.x.x si usas JDK 11)
# JAVA_HOME debe mostrar una ruta válida, NO debe estar vacío
java -version 2>&1 | head -1
echo "JAVA_HOME: $JAVA_HOME"
```

---

### Paso 2 — Instalación de Apache Spark

**Objetivo:** Descargar e instalar Apache Spark 3.5.x y configurar `SPARK_HOME` para que PySpark pueda localizarlo.

#### Instrucciones

**Todos los sistemas operativos**

1. Descarga Apache Spark 3.5.x desde el sitio oficial. Elige la versión con Hadoop 3 incluido:
   ```
   https://spark.apache.org/downloads.html
   ```
   Selecciona:
   - Spark release: **3.5.x** (la más reciente en la rama 3.5)
   - Package type: **Pre-built for Apache Hadoop 3.3 and later**

2. **Linux / macOS** — Extrae y mueve el directorio:
   ```bash
   # Descargar (ajusta el número de versión exacto)
   wget https://downloads.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz

   # Extraer
   tar -xzf spark-3.5.3-bin-hadoop3.tgz

   # Mover a directorio estándar
   sudo mv spark-3.5.3-bin-hadoop3 /opt/spark

   # Configurar variables de entorno (agregar a ~/.bashrc o ~/.zshrc)
   echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
   echo 'export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"' >> ~/.bashrc
   echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
   source ~/.bashrc
   ```

3. **Windows** — Pasos adicionales requeridos:

   a. Extrae el archivo `.tgz` con 7-Zip o WinRAR a `C:\spark` (ruta sin espacios).

   b. Descarga `winutils.exe` para Hadoop 3.x desde:
      ```
      https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin
      ```
      Crea el directorio `C:\hadoop\bin` y coloca `winutils.exe` allí.

   c. Configura las variables de entorno del sistema:
      ```powershell
      # Ejecutar PowerShell como Administrador
      [System.Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark", "Machine")
      [System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "Machine")

      # Agregar al PATH
      $path = [System.Environment]::GetEnvironmentVariable("PATH", "Machine")
      [System.Environment]::SetEnvironmentVariable("PATH", "$path;C:\spark\bin;C:\hadoop\bin", "Machine")
      ```

   d. **Cierra y vuelve a abrir PowerShell** para que los cambios surtan efecto.

4. Verifica la instalación de Spark en todos los sistemas:
   ```bash
   spark-submit --version
   ```

#### Salida esperada

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.x
      /_/

Using Scala version 2.12.x, OpenJDK 64-Bit Server VM, 17.0.x
```

#### Verificación

```bash
# Verificar que spark-shell arranca correctamente (salir con :quit)
echo "sc.version" | spark-shell --master local 2>/dev/null | grep "res0"
```

---

### Paso 3 — Instalación de Python y Entorno Virtual

**Objetivo:** Verificar Python 3.10/3.11, crear un entorno virtual aislado para el curso e instalar todas las bibliotecas necesarias.

#### Instrucciones

1. Verifica la versión de Python instalada:
   ```bash
   python3 --version
   # En Windows: python --version
   ```
   Debe mostrar `Python 3.10.x` o `Python 3.11.x`. Si no tienes Python instalado, descárgalo desde [python.org](https://www.python.org/downloads/).

   > **Windows:** Durante la instalación, marca ✅ **"Add Python to PATH"**.

2. Crea un entorno virtual dedicado para el curso:
   ```bash
   # Linux / macOS
   python3 -m venv ~/entornos/spark_curso

   # Windows (PowerShell)
   python -m venv $env:USERPROFILE\entornos\spark_curso
   ```

3. Activa el entorno virtual:
   ```bash
   # Linux / macOS
   source ~/entornos/spark_curso/bin/activate

   # Windows (PowerShell)
   ~\entornos\spark_curso\Scripts\Activate.ps1

   # Windows (CMD)
   ~\entornos\spark_curso\Scripts\activate.bat
   ```
   El prompt del terminal debe cambiar para mostrar `(spark_curso)`.

   > **Windows PowerShell:** Si recibes error de política de ejecución, ejecuta primero:
   > ```powershell
   > Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   > ```

4. Actualiza pip e instala todas las bibliotecas del curso:
   ```bash
   # Actualizar pip
   pip install --upgrade pip

   # Instalar el stack completo del curso
   pip install \
     pyspark==3.5.3 \
     jupyterlab==4.2.5 \
     notebook==7.2.2 \
     findspark==2.0.1 \
     pandas==2.2.2 \
     numpy==1.26.4 \
     matplotlib==3.9.2

   # Verificar versiones instaladas
   pip list | grep -E "pyspark|jupyter|findspark|pandas|numpy|matplotlib"
   ```

   > **Nota:** Ajusta los números de versión exactos si pip resuelve versiones más recientes compatibles. Lo crítico es que `pyspark` y `spark` sean de la misma rama (3.5.x).

5. Verifica que PySpark puede encontrar la instalación de Spark:
   ```bash
   python3 -c "
   import pyspark
   print(f'PySpark versión: {pyspark.__version__}')
   import os
   print(f'SPARK_HOME: {os.environ.get(\"SPARK_HOME\", \"NO CONFIGURADO\")}')
   "
   ```

#### Salida esperada

```
PySpark versión: 3.5.3
SPARK_HOME: /opt/spark
```

#### Verificación

```bash
# Listar paquetes clave instalados
pip show pyspark pandas numpy matplotlib findspark | grep -E "^Name|^Version"
```

---

### Paso 4 — Configuración de JupyterLab con findspark

**Objetivo:** Configurar JupyterLab como entorno de trabajo interactivo del curso e integrar findspark para que Jupyter localice automáticamente la instalación de Spark.

#### Instrucciones

1. Con el entorno virtual activo, inicia JupyterLab:
   ```bash
   jupyter lab
   ```
   Se abrirá automáticamente el navegador en `http://localhost:8888`. Si no se abre, copia la URL que aparece en la terminal (incluye el token de seguridad).

2. En JupyterLab, crea un nuevo notebook: **File → New → Notebook → Python 3 (ipykernel)**.

3. En la primera celda del notebook, ejecuta el siguiente código de configuración inicial:
   ```python
   # Celda 1: Configuración del entorno con findspark
   import findspark
   findspark.init()  # Localiza SPARK_HOME automáticamente

   # Verificar que findspark encontró Spark
   print(f"Spark encontrado en: {findspark.find()}")
   ```

4. En la segunda celda, inicializa el `SparkSession` con configuración de memoria apropiada:
   ```python
   # Celda 2: Inicialización de SparkSession
   from pyspark.sql import SparkSession

   spark = SparkSession.builder \
       .appName("Lab01-Verificacion-Entorno") \
       .master("local[*]") \
       .config("spark.driver.memory", "2g") \
       .config("spark.executor.memory", "2g") \
       .config("spark.ui.port", "4040") \
       .getOrCreate()

   # Configurar nivel de log para reducir ruido en la salida
   spark.sparkContext.setLogLevel("WARN")

   print(f"SparkSession iniciada correctamente")
   print(f"Versión de Spark: {spark.version}")
   print(f"Modo de ejecución: {spark.sparkContext.master}")
   print(f"Spark UI disponible en: http://localhost:4040")
   ```

5. Ejecuta ambas celdas con **Shift+Enter** y verifica que no hay errores.

   > **Máquinas con 8 GB RAM:** Si recibes advertencias de memoria, reduce los valores a `"1g"` en `spark.driver.memory` y `spark.executor.memory`.

#### Salida esperada

```
Spark encontrado en: /opt/spark
SparkSession iniciada correctamente
Versión de Spark: 3.5.3
Modo de ejecución: local[*]
Spark UI disponible en: http://localhost:4040
```

#### Verificación

Abre el navegador en `http://localhost:4040`. Debes ver la **Spark UI** con el nombre de la aplicación `Lab01-Verificacion-Entorno` y sin jobs ejecutados aún.

---

### Paso 5 — Verificación con RDD: Tipos de Datos y Estructuras Python

**Objetivo:** Aplicar los tipos de datos y estructuras de Python de la lección 2.1 para crear y operar sobre un RDD básico, verificando la integración completa del entorno.

#### Instrucciones

1. En una nueva celda del notebook, crea datos usando las estructuras Python aprendidas (listas y tuplas) y paralelízalos como un RDD:
   ```python
   # Celda 3: Tipos de datos Python → RDD de Spark
   # Aplicamos lo visto en la lección 2.1: listas de tuplas como estructura de datos

   # Lista de tuplas: cada tupla representa un registro de empleado
   # (nombre: str, departamento: str, salario: float, activo: bool)
   datos_empleados = [
       ("Ana García",    "Ventas",    42000.0, True),
       ("Luis Herrera",  "TI",        68000.0, True),
       ("María López",   "RRHH",      51000.0, False),
       ("Carlos Ruiz",   "TI",        72000.0, True),
       ("Sofía Mendez",  "Ventas",    45000.0, True),
       ("Pedro Alonso",  "RRHH",      53000.0, False),
       ("Elena Torres",  "TI",        76000.0, True),
       ("Javier Mora",   "Ventas",    39000.0, True),
   ]

   # Verificar tipos de datos Python (lección 2.1)
   primer_registro = datos_empleados[0]
   print("=== Verificación de tipos Python (Lección 2.1) ===")
   print(f"Registro completo: {primer_registro}")
   print(f"Tipo del registro: {type(primer_registro)}")   # tuple
   print(f"Nombre (str):      {primer_registro[0]} → {type(primer_registro[0])}")
   print(f"Departamento (str):{primer_registro[1]} → {type(primer_registro[1])}")
   print(f"Salario (float):   {primer_registro[2]} → {type(primer_registro[2])}")
   print(f"Activo (bool):     {primer_registro[3]} → {type(primer_registro[3])}")
   ```

2. Crea el RDD y ejecuta operaciones básicas:
   ```python
   # Celda 4: Crear RDD y operaciones básicas
   sc = spark.sparkContext

   # Paralelizar la lista de tuplas en un RDD con 2 particiones
   rdd_empleados = sc.parallelize(datos_empleados, numSlices=2)

   # Operaciones básicas sobre el RDD
   print("=== Operaciones sobre RDD ===")
   print(f"Total de registros:     {rdd_empleados.count()}")
   print(f"Número de particiones:  {rdd_empleados.getNumPartitions()}")

   # Filtrar empleados activos (usando bool de Python)
   rdd_activos = rdd_empleados.filter(lambda emp: emp[3] == True)
   print(f"Empleados activos:      {rdd_activos.count()}")

   # Extraer salarios (usando float de Python)
   salarios = rdd_empleados.map(lambda emp: emp[2]).collect()
   print(f"Salarios extraídos:     {salarios}")
   print(f"Salario promedio:       ${sum(salarios)/len(salarios):,.2f}")
   ```

3. Usa diccionarios Python para agrupar datos:
   ```python
   # Celda 5: Uso de diccionarios Python para análisis por departamento
   # Aplicamos la estructura dict de la lección 2.1

   # Agrupar empleados por departamento usando RDD
   por_departamento = rdd_empleados \
       .map(lambda emp: (emp[1], emp[2])) \
       .groupByKey() \
       .mapValues(list) \
       .collect()

   # Convertir resultado a diccionario Python (estructura dict)
   resumen_deptos = {}
   for depto, salarios in por_departamento:
       resumen_deptos[depto] = {
           "cantidad": len(salarios),
           "salario_promedio": sum(salarios) / len(salarios),
           "salario_max": max(salarios),
           "salario_min": min(salarios)
       }

   # Mostrar resultados usando el diccionario
   print("=== Resumen por Departamento (dict Python) ===")
   for depto, stats in resumen_deptos.items():
       print(f"\n{depto}:")
       for metrica, valor in stats.items():
           if isinstance(valor, float):
               print(f"  {metrica}: ${valor:,.2f}")
           else:
               print(f"  {metrica}: {valor}")
   ```

#### Salida esperada

```
=== Verificación de tipos Python (Lección 2.1) ===
Registro completo: ('Ana García', 'Ventas', 42000.0, True)
Tipo del registro: <class 'tuple'>
Nombre (str):      Ana García → <class 'str'>
Departamento (str):Ventas → <class 'str'>
Salario (float):   42000.0 → <class 'float'>
Activo (bool):     True → <class 'bool'>

=== Operaciones sobre RDD ===
Total de registros:     8
Número de particiones:  2
Empleados activos:      6
Salarios extraídos:     [42000.0, 68000.0, 51000.0, 72000.0, 45000.0, 53000.0, 76000.0, 39000.0]
Salario promedio:       $55,750.00

=== Resumen por Departamento (dict Python) ===
Ventas:
  cantidad: 3
  salario_promedio: $42,000.00
  ...
```

#### Verificación

Recarga la Spark UI en `http://localhost:4040` → pestaña **Jobs**. Debes ver 3–5 jobs completados con estado **Succeeded**.

---

### Paso 6 — Exploración de Bibliotecas Complementarias (pandas, NumPy, matplotlib)

**Objetivo:** Verificar la instalación y funcionamiento de las bibliotecas de análisis de datos que complementan PySpark en el flujo de trabajo del curso.

#### Instrucciones

1. Verifica NumPy y sus tipos de datos (relacionados con los tipos Python de la lección 2.1):
   ```python
   # Celda 6: Verificación de NumPy
   import numpy as np

   print(f"NumPy versión: {np.__version__}")

   # Crear arreglo con los salarios del dataset
   salarios_np = np.array([42000.0, 68000.0, 51000.0, 72000.0,
                            45000.0, 53000.0, 76000.0, 39000.0])

   print(f"Tipo del arreglo:  {type(salarios_np)}")
   print(f"Tipo de datos:     {salarios_np.dtype}")    # float64
   print(f"Media:             ${np.mean(salarios_np):,.2f}")
   print(f"Desv. estándar:    ${np.std(salarios_np):,.2f}")
   print(f"Mínimo / Máximo:   ${salarios_np.min():,.0f} / ${salarios_np.max():,.0f}")
   ```

2. Verifica pandas y su integración con los datos del RDD:
   ```python
   # Celda 7: Verificación de pandas
   import pandas as pd

   print(f"pandas versión: {pd.__version__}")

   # Crear DataFrame pandas desde la lista de tuplas (misma estructura que usamos en RDD)
   columnas = ["nombre", "departamento", "salario", "activo"]
   df_pandas = pd.DataFrame(datos_empleados, columns=columnas)

   print("\nDataFrame pandas:")
   print(df_pandas)
   print(f"\nTipos de datos pandas:")
   print(df_pandas.dtypes)
   print(f"\nEstadísticas descriptivas:")
   print(df_pandas["salario"].describe())
   ```

3. Crea una visualización básica con matplotlib:
   ```python
   # Celda 8: Verificación de matplotlib — Gráfico de salarios por departamento
   import matplotlib.pyplot as plt
   import matplotlib
   print(f"matplotlib versión: {matplotlib.__version__}")

   # Preparar datos para la visualización
   resumen_plot = df_pandas.groupby("departamento")["salario"].mean().reset_index()

   # Crear gráfico de barras
   fig, ax = plt.subplots(figsize=(8, 5))
   barras = ax.bar(
       resumen_plot["departamento"],
       resumen_plot["salario"],
       color=["#E25A1C", "#3B82F6", "#10B981"],
       edgecolor="white",
       linewidth=1.5
   )

   # Etiquetas sobre las barras
   for barra in barras:
       altura = barra.get_height()
       ax.text(barra.get_x() + barra.get_width()/2., altura + 500,
               f'${altura:,.0f}', ha='center', va='bottom', fontweight='bold')

   ax.set_title("Salario Promedio por Departamento", fontsize=14, fontweight='bold', pad=15)
   ax.set_xlabel("Departamento", fontsize=12)
   ax.set_ylabel("Salario Promedio (USD)", fontsize=12)
   ax.set_ylim(0, max(resumen_plot["salario"]) * 1.15)
   ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'${x:,.0f}'))
   ax.grid(axis='y', alpha=0.3, linestyle='--')

   plt.tight_layout()
   plt.savefig("salarios_por_departamento.png", dpi=150, bbox_inches='tight')
   plt.show()
   print("✅ Gráfico generado y guardado como 'salarios_por_departamento.png'")
   ```

4. Convierte el DataFrame pandas a un DataFrame Spark para verificar la interoperabilidad:
   ```python
   # Celda 9: Interoperabilidad pandas ↔ Spark DataFrame
   df_spark = spark.createDataFrame(df_pandas)

   print("=== DataFrame de Spark creado desde pandas ===")
   df_spark.printSchema()
   df_spark.show()
   print(f"Registros en Spark DataFrame: {df_spark.count()}")
   ```

#### Salida esperada

```
NumPy versión: 1.26.4
Tipo del arreglo:  <class 'numpy.ndarray'>
Tipo de datos:     float64
Media:             $55,750.00
...

pandas versión: 2.2.2
...

matplotlib versión: 3.9.2
✅ Gráfico generado y guardado como 'salarios_por_departamento.png'

=== DataFrame de Spark creado desde pandas ===
root
 |-- nombre: string (nullable = true)
 |-- departamento: string (nullable = true)
 |-- salario: double (nullable = true)
 |-- activo: boolean (nullable = true)

+-------------+------------+-------+------+
|       nombre|departamento|salario|activo|
+-------------+------------+-------+------+
|   Ana García|      Ventas|42000.0|  true|
...
```

#### Verificación

Confirma que el archivo `salarios_por_departamento.png` fue creado en el directorio de trabajo del notebook:
```python
import os
assert os.path.exists("salarios_por_departamento.png"), "El gráfico no fue generado"
print("✅ Archivo de gráfico verificado correctamente")
```

---

## Validación y Prueba Final del Entorno

Ejecuta el siguiente script de validación completo en una nueva celda del notebook. Este script verifica todos los componentes instalados y genera un reporte de estado:

```python
# ============================================================
# SCRIPT DE VALIDACIÓN COMPLETA — Lab 02-00-01
# ============================================================
import sys
import os

resultados = []

def verificar(nombre, condicion, detalle=""):
    estado = "✅ OK" if condicion else "❌ FALLO"
    resultados.append((nombre, estado, detalle))
    print(f"{estado} | {nombre:<35} | {detalle}")

print("=" * 75)
print("  REPORTE DE VALIDACIÓN DEL ENTORNO — Lab 02-00-01")
print("=" * 75)

# 1. Verificar Python
version_python = sys.version_info
verificar(
    "Python 3.10+",
    version_python.major == 3 and version_python.minor >= 10,
    f"Python {version_python.major}.{version_python.minor}.{version_python.micro}"
)

# 2. Verificar JAVA_HOME
java_home = os.environ.get("JAVA_HOME", "")
verificar(
    "JAVA_HOME configurado",
    bool(java_home) and os.path.exists(java_home),
    java_home or "NO CONFIGURADO"
)

# 3. Verificar SPARK_HOME
spark_home = os.environ.get("SPARK_HOME", "")
verificar(
    "SPARK_HOME configurado",
    bool(spark_home) and os.path.exists(spark_home),
    spark_home or "NO CONFIGURADO"
)

# 4. Verificar PySpark
try:
    import pyspark
    verificar("PySpark instalado", True, f"v{pyspark.__version__}")
except ImportError as e:
    verificar("PySpark instalado", False, str(e))

# 5. Verificar findspark
try:
    import findspark
    verificar("findspark instalado", True, f"v{findspark.__version__}")
except ImportError as e:
    verificar("findspark instalado", False, str(e))

# 6. Verificar pandas
try:
    import pandas as pd
    verificar("pandas instalado", True, f"v{pd.__version__}")
except ImportError as e:
    verificar("pandas instalado", False, str(e))

# 7. Verificar NumPy
try:
    import numpy as np
    verificar("NumPy instalado", True, f"v{np.__version__}")
except ImportError as e:
    verificar("NumPy instalado", False, str(e))

# 8. Verificar matplotlib
try:
    import matplotlib
    verificar("matplotlib instalado", True, f"v{matplotlib.__version__}")
except ImportError as e:
    verificar("matplotlib instalado", False, str(e))

# 9. Verificar SparkSession activa
try:
    assert spark.version.startswith("3.5"), f"Versión inesperada: {spark.version}"
    verificar("SparkSession activa (3.5.x)", True, f"v{spark.version}")
except Exception as e:
    verificar("SparkSession activa (3.5.x)", False, str(e))

# 10. Verificar operación RDD básica
try:
    test_rdd = spark.sparkContext.parallelize(range(1000))
    suma = test_rdd.reduce(lambda a, b: a + b)
    assert suma == 499500, f"Suma incorrecta: {suma}"
    verificar("RDD: parallelize + reduce", True, f"sum(0..999) = {suma}")
except Exception as e:
    verificar("RDD: parallelize + reduce", False, str(e))

# 11. Verificar DataFrame Spark
try:
    test_df = spark.createDataFrame([(1, "spark"), (2, "python")], ["id", "nombre"])
    assert test_df.count() == 2
    verificar("DataFrame: createDataFrame", True, f"{test_df.count()} filas creadas")
except Exception as e:
    verificar("DataFrame: createDataFrame", False, str(e))

# 12. Verificar Spark UI accesible
import urllib.request
try:
    urllib.request.urlopen("http://localhost:4040", timeout=3)
    verificar("Spark UI (puerto 4040)", True, "http://localhost:4040")
except Exception:
    verificar("Spark UI (puerto 4040)", False, "Puerto no accesible (puede usar 4041/4042)")

# Resumen final
print("=" * 75)
fallos = [r for r in resultados if "FALLO" in r[1]]
print(f"\nRESUMEN: {len(resultados) - len(fallos)}/{len(resultados)} verificaciones exitosas")
if fallos:
    print("\n⚠️  Componentes con problemas:")
    for nombre, estado, detalle in fallos:
        print(f"   → {nombre}: {detalle}")
else:
    print("\n🎉 ¡Entorno completamente funcional! Listo para el curso.")
print("=" * 75)
```

**Resultado esperado de la validación:**

```
===========================================================================
  REPORTE DE VALIDACIÓN DEL ENTORNO — Lab 02-00-01
===========================================================================
✅ OK | Python 3.10+                       | Python 3.11.x
✅ OK | JAVA_HOME configurado              | /usr/lib/jvm/java-17-openjdk-amd64
✅ OK | SPARK_HOME configurado             | /opt/spark
✅ OK | PySpark instalado                  | v3.5.3
✅ OK | findspark instalado                | v2.0.1
✅ OK | pandas instalado                   | v2.2.2
✅ OK | NumPy instalado                    | v1.26.4
✅ OK | matplotlib instalado               | v3.9.2
✅ OK | SparkSession activa (3.5.x)        | v3.5.3
✅ OK | RDD: parallelize + reduce          | sum(0..999) = 499500
✅ OK | DataFrame: createDataFrame         | 2 filas creadas
✅ OK | Spark UI (puerto 4040)             | http://localhost:4040
===========================================================================

RESUMEN: 12/12 verificaciones exitosas

🎉 ¡Entorno completamente funcional! Listo para el curso.
===========================================================================
```

> **Criterio de aprobación:** Mínimo 11/12 verificaciones exitosas. El único punto que puede fallar sin impacto es la verificación de Spark UI si el puerto 4040 está bloqueado por firewall corporativo (Spark asignará 4041 o 4042 automáticamente).

---

## Solución de Problemas

### Problema 1 — Error al iniciar SparkSession: `JAVA_HOME is not set`

**Síntoma:**
```
Exception in thread "main" java.lang.Exception: JAVA_HOME is not set
# o bien:
Py4JJavaError: An error occurred while calling ... 
java.lang.RuntimeException: Java gateway process exited before sending its port number
```

**Causa:**
La variable de entorno `JAVA_HOME` no está configurada correctamente o no es visible para el proceso Python que lanza Spark. Esto es especialmente común en Windows cuando la variable se configuró en el sistema pero la sesión de terminal no se reinició, o cuando se usa un entorno virtual que no hereda las variables del sistema.

**Solución:**
1. Verifica que `JAVA_HOME` apunta a un directorio que existe y contiene el subdirectorio `bin/java`:
   ```bash
   # Linux/macOS
   ls $JAVA_HOME/bin/java

   # Windows PowerShell
   Test-Path "$env:JAVA_HOME\bin\java.exe"
   ```

2. Si la variable no está disponible en la sesión actual, configúrala directamente desde Python antes de crear el `SparkSession`:
   ```python
   import os
   # Linux/macOS — ajusta la ruta a tu instalación
   os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

   # Windows — ajusta la ruta a tu instalación
   # os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.x.x-hotspot"

   # Luego inicializa findspark y SparkSession normalmente
   import findspark
   findspark.init()
   ```

3. En Windows, cierra completamente PowerShell/CMD, ábrelo de nuevo como Administrador y ejecuta `echo %JAVA_HOME%` para confirmar que la variable persiste.

---

### Problema 2 — `OutOfMemoryError` al ejecutar operaciones en Spark

**Síntoma:**
```
java.lang.OutOfMemoryError: Java heap space
# o bien:
ERROR SparkContext: Error initializing SparkContext
java.lang.OutOfMemoryError: GC overhead limit exceeded
```
El kernel de Jupyter puede reiniciarse inesperadamente o la celda queda ejecutándose indefinidamente.

**Causa:**
En máquinas con 8 GB de RAM, la configuración por defecto de Spark puede intentar asignar demasiada memoria al driver o al executor, dejando al sistema operativo sin recursos. Esto ocurre especialmente cuando hay otras aplicaciones abiertas (navegador, IDE, etc.) consumiendo RAM simultáneamente.

**Solución:**
1. Detén el `SparkSession` actual si está activo:
   ```python
   spark.stop()
   ```

2. Reinicia el kernel de Jupyter: **Kernel → Restart Kernel**.

3. Cierra aplicaciones innecesarias (navegador con múltiples pestañas, IDE, etc.).

4. Crea el `SparkSession` con configuración de memoria reducida:
   ```python
   import findspark
   findspark.init()
   from pyspark.sql import SparkSession

   spark = SparkSession.builder \
       .appName("Lab01-Memoria-Reducida") \
       .master("local[2]") \
       .config("spark.driver.memory", "1g") \
       .config("spark.executor.memory", "1g") \
       .config("spark.driver.maxResultSize", "512m") \
       .config("spark.sql.shuffle.partitions", "4") \
       .getOrCreate()
   ```
   Usar `local[2]` en lugar de `local[*]` limita los workers a 2 hilos, reduciendo el consumo de memoria.

5. Si el problema persiste, verifica la memoria disponible:
   ```python
   import psutil
   mem = psutil.virtual_memory()
   print(f"RAM total: {mem.total/1e9:.1f} GB")
   print(f"RAM disponible: {mem.available/1e9:.1f} GB")
   print(f"RAM en uso: {mem.percent}%")
   # Si available < 3 GB, cierra más aplicaciones antes de continuar
   ```

---

## Limpieza del Entorno

Al finalizar la práctica, detén correctamente el `SparkSession` para liberar los recursos del sistema:

```python
# Celda final del notebook — Limpieza del entorno
print("Deteniendo SparkSession...")
spark.stop()
print("✅ SparkSession detenida correctamente")
print("   → Spark UI en http://localhost:4040 ya no estará disponible")
print("   → Para reiniciar, ejecuta las celdas de configuración nuevamente")
```

Guarda el notebook antes de cerrarlo: **File → Save Notebook** o `Ctrl+S`.

Para desactivar el entorno virtual al terminar la sesión de trabajo:
```bash
# Linux / macOS / Windows
deactivate
```

> **Nota:** El entorno virtual `spark_curso` y todos los paquetes instalados permanecen disponibles para las próximas prácticas. Solo necesitas activarlo nuevamente con `source ~/entornos/spark_curso/bin/activate` (Linux/macOS) o el comando equivalente en Windows.

---

## Resumen

En esta práctica has completado la instalación y configuración del stack tecnológico completo del curso:

| Componente          | Estado     | Versión instalada |
|---------------------|------------|-------------------|
| Java JDK            | ✅ Instalado | 17 LTS            |
| Apache Spark        | ✅ Instalado | 3.5.x             |
| Python              | ✅ Verificado | 3.10/3.11         |
| PySpark             | ✅ Instalado | 3.5.x             |
| JupyterLab          | ✅ Funcional | 4.x               |
| findspark           | ✅ Instalado | 2.0.1+            |
| pandas              | ✅ Instalado | 2.x               |
| NumPy               | ✅ Instalado | 1.26.x            |
| matplotlib          | ✅ Instalado | 3.9.x             |

Los conceptos clave que conectan esta práctica con la lección 2.1 son:

- **Tipos de datos Python** (`str`, `float`, `bool`, `int`, `NoneType`) se mapean directamente a tipos Spark (`StringType`, `DoubleType`, `BooleanType`, `IntegerType`).
- **Las listas de tuplas** son la estructura de datos más común para crear RDDs y DataFrames en PySpark desde Python.
- **Los diccionarios** son útiles para organizar resultados de agregaciones y configurar parámetros de Spark.
- **El entorno virtual** aísla las dependencias del proyecto y garantiza reproducibilidad entre máquinas.

### Recursos de Referencia

| Recurso | URL |
|---------|-----|
| Documentación oficial PySpark 3.5 | https://spark.apache.org/docs/3.5.0/api/python/ |
| Guía de instalación Spark | https://spark.apache.org/docs/latest/installation.html |
| Eclipse Temurin JDK | https://adoptium.net |
| winutils para Windows | https://github.com/cdarlint/winutils |
| The Zen of Python (PEP 20) | https://peps.python.org/pep-0020/ |

> **Próxima práctica:** Lab 02-01-01 — Estructuras de Datos en PySpark: RDDs, DataFrames y Datasets. Asegúrate de mantener el entorno virtual activo y JupyterLab funcionando.

---
LAB_END---
