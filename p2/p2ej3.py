import math

root_path = "./"
inputDir = root_path + "WordCount/input/"
tmpDir = root_path + "WordCount/output/"
outputDir = root_path + "WordCount-Statistics/output/"

def fmap1(key, value, context):
    words = value.split()
    for w in words:
      context.write(w, 1)

def fcom1(key, values, context):
    c = 0
    for v in values:
      c = c + v
    context.write(key, c)

def fred1(key, values, context):
    c = 0
    for v in values:
      c = c + v
    context.write(key, c)

def fmap2(key, value, context):
    word = key
    word_count = int(value)
    context.write("max_word", (word, word_count))
    context.write("min_word", (word, word_count))
    context.write("avg_stdev", (word_count, word_count ** 2, 1))

def fcom2(key, values, context):
    if key == "max_word":
      max = -float("inf")
      max_word = ""
      for v in values:
        word, count = v
        if count > max:
          max = count
          max_word = word
      context.write("max_word", (max_word, max))
    elif key == "min_word":
      min = float("inf")
      min_word = ""
      for v in values:
        word, count = v
        if count < min:
          min = count
          min_word = word
      context.write("min_word", (min_word, min))
    elif key == "avg_stdev":
        total_sum = 0
        total_squared_sum = 0
        total_words_count = 0
        for v in values:
          partial_sum, partial_squared_sum, partial_words_count = v
          total_sum += partial_sum
          total_squared_sum += partial_squared_sum
          total_words_count += partial_words_count
        context.write("avg_stdev", (total_sum, total_squared_sum, total_words_count))

def fred2(key, values, context):
    if key == "max_word":
      max = -float("inf")
      max_word = ""
      for v in values:
        word, count = v
        if count > max:
          max = count
          max_word = word
      context.write("Max: ", (max_word, max))
    elif key == "min_word":
      min = float("inf")
      min_word = ""
      for v in values:
        word, count = v
        if count < min:
          min = count
          min_word = word
      context.write("Min: ", (min_word, min))
    elif key == "avg_stdev":
        total_sum = 0
        total_squared_sum = 0
        total_words_count = 0
        for v in values:
          partial_sum, partial_squared_sum, partial_words_count = v
          total_sum += partial_sum
          total_squared_sum += partial_squared_sum
          total_words_count += partial_words_count
        avg = total_sum/total_words_count
        stdev = math.sqrt(total_squared_sum/total_words_count - avg ** 2)
        context.write("Average and standard deviation: ", (round(avg, 2), round(stdev, 2)))

job1 = Job(inputDir, tmpDir, fmap1, fred1)
job1.setCombiner(fcom1)
success1 = job1.waitForCompletion()

job2 = Job(tmpDir, outputDir, fmap2, fred2)
job2.setCombiner(fcom2)
success2 = job2.waitForCompletion()
