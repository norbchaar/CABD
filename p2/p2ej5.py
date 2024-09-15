root_path = "./"
input_dir = root_path + "Website/input/"
output_dir_1 = root_path + "Website/output/1/"
output_dir_2 = root_path + "Website/output/2/"
output_dir_3 = root_path + "Website/output/3/"
output_dir_4 = root_path + "Website/output/4/"
output_dir_5 = root_path + "Website/output/5/"
output_dir_6 = root_path + "Website/output/6/"


def fmap1(key, value, context):
  user_id = key
  page_id, time = value.split()
  context.write(f"{user_id}-{page_id}", float(time))

def fcom1(key, values, context):
  total_time = 0
  for v in values:
    total_time += v
  context.write(key, total_time)

def fred1(key, values, context):
  total_time = 0
  for v in values:
    total_time += v
  context.write(key, total_time)

def fmap2(key, value, context):
  user_id, page_id = key.split("-")
  context.write(user_id, (page_id, float(value)))

def fcom2(key, values, context):
  max_time = -float("inf")
  max_page = None
  for page_id, time in values:
    if time > max_time:
      max_time = time
      max_page = page_id
  context.write(key, (max_page, max_time))

def fred2(key, values, context):
  max_time = -float("inf")
  max_page = None
  for page_id, time in values:
    if time > max_time:
      max_time = time
      max_page = page_id
  context.write(key, (max_page, max_time))

def fmap3(key, value, context):
  user_id, page_id = key.split("-")
  context.write(user_id, 1)

def fcom3(key, values, context):
  count = 0
  for v in values:
    count += v
  context.write(key, count)

def fred3(key, values, context):
  count = 0
  for v in values:
    count += v
  context.write(key, count)

def fmap4(key, value, context):
  user_id, count = key, value
  context.write("max_user_by_visits", (user_id, float(count)))

def fcom4(key, values, context):
  max_user = None
  max_count = -float("inf")
  for user_id, count in values:
    if count > max_count:
      max_count = count
      max_user = user_id
  context.write("max_user_by_visits", (max_user, max_count))

def fred4(key, values, context):
  max_user = None
  max_count = -float("inf")
  for user_id, count in values:
    if count > max_count:
      max_count = count
      max_user = user_id
  context.write("max_user_by_visits", (max_user, max_count))

def fmap5(key, value, context):
  user_id = key
  page_id, time = value.split()
  context.write(page_id, 1)

def fcom5(key, values, context):
  count = 0
  for v in values:
    count += v
  context.write(key, count)

def fred5(key, values, context):
  total_count = 0
  for v in values:
    total_count += v
  context.write(key, total_count)

def fmap6(key, value, context):
  page_id, total_count = key, value
  context.write("most_visited", (page_id, float(total_count)))

def fcom6(key, values, context):
  max_count = -float("inf")
  max_page = None
  for page_id, total_count in values:
    if total_count > max_count:
      max_count = total_count
      max_page = page_id
  context.write("most_visited", (max_page, max_count))

def fred6(key, values, context):
  max_count = -float("inf")
  max_page = None
  for page_id, total_count in values:
    if total_count > max_count:
      max_count = total_count
      max_page = page_id
  context.write("most_visited", (max_page, max_count))

# Total visits (in time) by user and page
job1 = Job(input_dir, output_dir_1, fmap1, fred1)
job1.setCombiner(fcom1)
success = job1.waitForCompletion()

# Top visited site (in time) by user
job2 = Job(output_dir_1, output_dir_2, fmap2, fred2)
job2.setCombiner(fcom2)
success = job2.waitForCompletion()

# Total visits by user
job3 = Job(output_dir_1, output_dir_3, fmap3, fred3)
job3.setCombiner(fcom3)
success = job3.waitForCompletion()

# Top user by visits
job4 = Job(output_dir_3, output_dir_4, fmap4, fred4)
job4.setCombiner(fcom4)
success = job4.waitForCompletion()

# Total visits per site
job5 = Job(input_dir, output_dir_5, fmap5, fred5)
job5.setCombiner(fcom5)
success = job5.waitForCompletion()

# Top site by visits
job6 = Job(output_dir_5, output_dir_6, fmap6, fred6)
job6.setCombiner(fcom6)
success = job6.waitForCompletion()
