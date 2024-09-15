root_path = "./"
input_dir = root_path + "Jacobi/input/"
output_dir = root_path + "Jacobi/output/"


def map(key, value, context):
  vars = context["incognitas"]
  coefs = value.split("\t")
  res = 0
  for i in range(4):
    res = res + vars[i] * float(coefs[i])
  context.write(key, res)

# def fcom1(key, values, context):
#   pass

def red(key, values, context):
  res = 0
  for v in values:
    res = v
  context.write(key, res)


error = 0.01
dif = 1
iteraciones = 0
max_iter = 100
while dif >= error and iteraciones < max_iter:
  iteraciones += 1
  job = Job(input_dir, output_dir, map, red)
  coefs = {"incognitas": [1] + [0] * 15}
  job.setParams(coefs)
  success = job.waitForCompletion()
  dif = # ACÁ ESTARÍA LA COMPLEJIDAD



