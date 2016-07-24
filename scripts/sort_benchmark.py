import pandas as pd
import numpy as np 
from timeit import timeit
import csv

def benchmark(files,csv_filename):
   with open(csv_filename,"w") as f:
       csv_writer = csv.writer(f)
       setup_pd='import pandas as pd; files = '+str(files)
       setup_np='import numpy as np; from itertools import groupby;from operator import itemgetter; files = '+str(files)
       csv_writer.writerow(["PD_string","PD_int","NP"])
       for i in xrange(7):
           print files[i]
           setup_pd_copy = setup_pd+';i='+str(i)
           setup_np_copy = setup_np+';i='+str(i)
          
           setup_pd_copy = setup_pd_copy+"; p1 = pd.read_csv(files[i], names=('A','B','C','D','E'));"
           setup_np_copy = setup_np_copy+"; n=np.load('"+files[i].split(".csv")[0]+"_only_num.npz')['arr_0']; "
           pd_time_elapsed_string = timeit("p1.sort('D')",setup=setup_pd_copy,number=3)
           pd_time_elapsed_int = timeit("p1.sort('E')",setup=setup_pd_copy,number=3)
	   np_time_elapsed = timeit("np.sort(n.view('f8,f8,f8,i8,i8'),order=['f4'])",setup=setup_np_copy,number=3)

           csv_writer.writerow([round(pd_time_elapsed_string,ndigits=3), round(pd_time_elapsed_int,ndigits=3),round(np_time_elapsed,ndigits=3)])



files_list = ("../data/smaller_file_ten.csv","../data/smaller_file_hundred.csv", "../data/smaller_file_thousand.csv", "../data/smaller_file_tenthousand.csv", "../data/smaller_file_hundredthousand.csv","../data/smaller_file_million.csv","../data/smaller_file_tenmillion.csv")
benchmark(files_list,"../data/sort.csv")
       
