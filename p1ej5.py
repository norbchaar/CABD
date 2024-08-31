root_path = "./"
inputDir = root_path + "Encuesta/input/"
outputDir = root_path + "Encuesta/output/"

def fmap(key, value, context):
  context.write(value, 1)


def fred(key, values, context):
  count = 0
  for value in values:
    count += value
  context.write(key, count)


job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
