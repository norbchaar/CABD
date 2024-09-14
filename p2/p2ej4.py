root_path = "./"
inputDir = root_path + "WordCount/input/"
tmpDir = root_path + "Ej4a/output/"
outputDir = root_path + "Ej4b/output/"


def fmap1(key, value, context):
  context.write("average_length", (len(value), 1)) # sends sum and data points

def fcom1(key, values, context):
  total_length = 0
  total_data_points = 0
  for length, count in values:
    total_length += length
    total_data_points += count
  context.write("average_length", (total_length, total_data_points))

def fred1(key, values, context):
  total_length = 0
  total_data_points = 0
  for length, count in values:
    total_length += length
    total_data_points += count
  context.write("average_length", total_length/total_data_points)

# devuelva como salida todos los párrafos que tienen una 
# longitud mayor al promedio.
def fmap2(key, value, context):
  avg = context["average_length"]
  if len(value) > avg:
    context.write(key, value)

def fred2(key, values, context):
  for v in values:
    context.write(key, v)


job1 = Job(inputDir, tmpDir, fmap1, fred1)
job1.setCombiner(fcom1)
success = job1.waitForCompletion()

# Read the average length from the first job's output
average_length = 0
with open(tmpDir + "/output.txt", "r") as f:
    for line in f:
        key, value = line.strip().split("\t")
        if key == "average_length":  # Look for the exact key
            average_length = float(value)  # Convert to float
            break  # Stop once we find the average


job2 = Job(inputDir, outputDir, fmap2, fred2)
param = {"average_length": average_length}
job2.setParams(param)
success = job2.waitForCompletion()
