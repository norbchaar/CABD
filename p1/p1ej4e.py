root_path = "./"
inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"

def fmap(key, value, context):
    if value.strip().startswith("-"):
      context.write(1, 1)
        
def fred(key, values, context):
    count = 0
    for value in values:
      count += value
    context.write(key, count)
    

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
