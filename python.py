from multiprocessing import Pool, cpu_count

def func(one_input):
  pass

def func2(input1, input2, ...):
  pass

# inputs = []
with Pool(cpu_count()-1) as pool:
  results = list(pool.map(func, inputs))
  
# inputs = [[], [], [], ...]
with Pool(cpu_count()-1) as pool:
  results = list(pool.starmap(func2, inputs))
