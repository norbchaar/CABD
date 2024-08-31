root_path = "./"
inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"

def fmap(key, value, context):
    char_count = len(value)
    context.write(1, char_count)
        
def fred(key, values, context):
    max_char_count = -1
    for value in values:
      if value > max_char_count:
        max_char_count = value
    context.write(key, max_char_count)
    

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
