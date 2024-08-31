root_path = "./"
inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"

def fmap(key, value, context):
    vowels_set = set("aeiouáéíóúüAEIOUÁÉÍÓÚÜ")
    consonants_set = set("bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ")
    for letter in value:
      if letter in vowels_set:
        context.write("vowels", 1)
      elif letter in consonants_set:
        context.write("consonants", 1)
      elif letter.isdigit():
        context.write("digits", 1)
      elif letter.isspace():
        context.write("spaces", 1)
      else:
        context.write("other characters", 1)
        
def fred(key, values, context):
    c=0
    for v in values:
        c=c+1
    context.write(key, c)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
