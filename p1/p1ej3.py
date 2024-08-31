from MRE import Job

##############################################

root_path = "./"
inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"

def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)
        
def fred(key, values, context):
    c=0
    for v in values:
        c=c+1
    context.write(key, c)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()

##############################################

# Abrir el archivo y procesar línea por línea
with open(outputDir + "/output.txt", "r") as f:
    conteo_palabras = {}

    # Leer cada línea del archivo
    for linea in f:
        # Separar la línea en palabra y valor
        palabra, valor = linea.rsplit(maxsplit=1)
        valor = int(valor)  # Convertir el valor a entero

        # Limpiar la palabra de puntuación al final (si la tiene)
        palabra_limpia = palabra.rstrip('.,;:')

        # Sumar el valor al conteo de la palabra limpia
        if palabra_limpia in conteo_palabras:
            conteo_palabras[palabra_limpia] += valor
        else:
            conteo_palabras[palabra_limpia] = valor

# Convertir el diccionario a una lista de tuplas y ordenarla
palabra_valores = list(conteo_palabras.items())
palabra_valores.sort(key=lambda x: x[1], reverse=True)

# Obtener el top 20 (o menos si hay menos de 20 palabras)
top_20 = palabra_valores[:20]

# Mostrar el resultado
for palabra, valor in top_20:
    print(f"{palabra}: {valor}")
