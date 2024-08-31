root_path = "./"
inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"

def fmap(key, value, context):
    lines = value.splitlines()
    for line in lines:
        if line.isupper():
          context.write(line, 1)
        
def fred(key, values, context):
    context.write(key, 1)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
