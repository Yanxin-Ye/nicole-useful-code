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

# Write to tempfile
f = tempfile.NamedTemporaryFile()
res_df = pd.DataFrame(m.run_anomaly_based_kda(write_to_bq=False))
res_df.to_csv(f.name, mode="a", index=False, header=False)
f.flush()
output = pd.read_csv(f.name, names=res_df.columns)
f.close()
