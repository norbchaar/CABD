from datetime import datetime
root_path = "./"
inputDir = root_path + "Inversionistas/input/"
outputDir = root_path + "Inversionistas/output/"

def fmap(key, value, context):
  value = value.split()
  name = value[0]
  day = int(value[1])
  month = int(value[2])
  year = int(value[3])
  age = get_age(year, month, day)
  investments = float(value[4])
  context.write("youngest", (name, age))
  context.write("total_investments", (investments))
  context.write("age_average", (age))


def fred(key, values, context):
  if key == "youngest":
    min_age = float("inf")
    min_age_name = ""
    for value in values:
      if value[1] < min_age:
        min_age = value[1]
        min_age_name = value[0]
    context.write(key, min_age_name)
  elif key == "total_investments":
    investments_count = 0
    for value in values:
      investments_count += value
    context.write(key, investments_count)
  elif key == "age_average":
    ages_count = 0
    people_count = 0
    for value in values:
      ages_count += value
      people_count += 1
    context.write(key, ages_count / people_count)

def get_age(year, month, day):
    birthdate = datetime(year, month, day)
    today = datetime.today()
    age = today.year - birthdate.year  
    # Adjust age if the birthday hasn't occurred yet this year
    if (today.month, today.day) < (birthdate.month, birthdate.day):
        age -= 1
    return age


job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
