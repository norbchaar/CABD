root_path = "./"
inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"

def fmap(key, value, context):
    words = value.split()
    context.write(1, len(words))
        
def fred(key, values, context):
    
    total_words = 0
    total_paragraphs = 0
    
    for value in values:
      total_words += value
      total_paragraphs += 1

    if total_paragraphs > 0:
      context.write(key, total_words / total_paragraphs)
    else:
      context.write(key, 0)
    

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
