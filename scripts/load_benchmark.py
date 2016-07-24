import pandas as pd
import numpy as np 
from timeit import timeit
import csv


def benchmark(files,csv_filename):
   with open(csv_filename,"w") as f:
       csv_writer = csv.writer(f)
       setup_pd='import pandas as pd; files = '+str(files)
       setup_np='import numpy as np; files = '+str(files)
       csv_writer.writerow(["PD","NP_CSV","NPY","NPZ"])
       for i in xrange(7):
           print files[i]
           setup_pd_copy = setup_pd+';i='+str(i)
           setup_np_copy = setup_np+';i='+str(i)
           pd_time_elapsed = timeit("pd.read_csv(files[i])",setup=setup_pd_copy,number=3)
           np_time_elapsed = timeit("np.loadtxt(files[i].split('.csv')[0]+'_only_num.csv',delimiter=',')",setup=setup_np_copy,number=3)

           npy_str = "x=np.load('"+files[i].split('.csv')[0]+"_only_num.npy')"
           npy_time_elapsed = timeit(npy_str ,setup=setup_np_copy,number=3)
           
           npz_str = "x=np.load('"+files[i].split('.csv')[0]+"_only_num.npz')['arr_0']"
           npz_time_elapsed = timeit(npz_str ,setup=setup_np_copy,number=3)
           csv_writer.writerow([round(pd_time_elapsed,ndigits=3), round(np_time_elapsed,ndigits=3),
                               round(npy_time_elapsed,ndigits=3), round(npz_time_elapsed,ndigits=3) ])



files_multicols = ("../data/smaller_file_ten.csv","../data/smaller_file_hundred.csv", "../data/smaller_file_thousand.csv", "../data/smaller_file_tenthousand.csv", "../data/smaller_file_hundredthousand.csv","../data/smaller_file_million.csv","../data/smaller_file_tenmillion.csv")
files_singlecols = ("../data/single_column_ten.csv","../data/single_column_hundred.csv", "../data/single_column_thousand.csv", "../data/single_column_tenthousand.csv", "../data/single_column_hundredthousand.csv","../data/single_column_million.csv","../data/single_column_tenmillion.csv")
benchmark(files_multicols,"../data/multicols.csv")
benchmark(files_singlecols,"../data/singlecols.csv")
       
