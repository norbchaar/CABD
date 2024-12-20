# -*- coding: utf-8 -*-
"""CABD-TP3.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1p-lbpH5lmffkDTxAT7dmH9uYKDPBPmuW
"""

# Actualizamos los repositorios
!apt-get update -qq

# Instalamos Spark para Python
!pip install pyspark

# Instalamos Java 8 (versión ligera sin GUI)
!apt-get install -y openjdk-8-jdk-headless -qq > /dev/null

# Configuramos JAVA_HOME
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Seleccionamos Java 8 (si es necesario)
!echo 2 | update-alternatives --config java

# Validamos instalación de Java
!java -version

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import os, time, shutil
import threading

# "VALOR DE ANTIGUEDAD"
A = .5

# Configuración de rutas
root_path = '/content/'
input_path = root_path + "input"  # Carpeta original con los archivos
input_streaming = root_path + "input_streaming"  # Carpeta monitoreada por Spark
buffer = root_path + "buffer"

# Preparar carpetas
if not os.path.exists(input_streaming):
    os.makedirs(input_streaming)
else:
    for f in os.listdir(input_streaming):
        os.remove(os.path.join(input_streaming, f))

# Configuración de SparkContext y StreamingContext
sc = SparkContext("local[2]", "CABD-TP3")
ssc = StreamingContext(sc, 6)  # Intervalo de microbatch de 6 segundos

# Leer el stream de datos desde `input_streaming`
stream = ssc.textFileStream(input_streaming)




# Transformaciones: Cada línea debe ser del formato <id_user, id_mission, tiempo_juego>
# Separar campos por tabulación
parsed_stream = stream.map(lambda line: line.split("\t"))

# Mapeamos a RDD pareado (id_mission, (tiempo_juego, ocurrencia))
mapped_stream = parsed_stream.map(lambda fields: (int(fields[1]), (float(fields[2]), 1)))

# Acumular tiempos y ocurrencias por misión
accumulations = mapped_stream.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Calcular los promedios
averages = accumulations.mapValues(lambda x: x[0] / x[1])




# Persistencia entre ventanas
ssc.checkpoint(buffer)

# Actualización entre ventanas
def fUpdate(newValues, scoreAnterior):
    # Si el estado es None, inicializa en 0. Si no, aplica el descuento
    # de antigüedad (evitando valores negativos)
    scoreAnterior = 0 if scoreAnterior is None else max(scoreAnterior - A, 0)
    # Suma los valores nuevos si existen
    newValuesSum = sum(newValues) if newValues else 0
    # Actualiza el score
    updatedScore = scoreAnterior + newValuesSum
    # Si el estado es 0, devuelve None para evitar guardar una clave innecesaria
    return None if updatedScore == 0 else updatedScore

newScores = averages.updateStateByKey(fUpdate)
newScores.pprint()




# Simulación de la llegada de archivos
def simulate_file_arrival():
    for file_name in sorted(os.listdir(input_path)):  # Ordena los archivos alfabéticamente
        source = os.path.join(input_path, file_name)
        destination = os.path.join(input_streaming, file_name)
        shutil.copy(source, destination)  # Copiar archivo al directorio monitoreado
        print(f"Archivo {file_name} recibido en {input_streaming}")
        time.sleep(3)  # Simular llegada cada 3 segundos

# Iniciar el streaming
ssc.start()

# Iniciar el hilo de simulación de llegada de archivos
file_thread = threading.Thread(target=simulate_file_arrival)
file_thread.start()

# Ejecución contínua de Spark Streaming
ssc.awaitTermination()
