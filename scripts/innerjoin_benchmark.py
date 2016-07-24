import pandas as pd
import numpy as np 
from timeit import timeit
import csv


def benchmark(files,csv_filename):
   with open(csv_filename,"w") as f:
       csv_writer = csv.writer(f)
       setup_pd='import pandas as pd; files = '+str(files)
       setup_np='import numpy as np; from itertools import groupby;from operator import itemgetter; files = '+str(files)
       csv_writer.writerow(["PD_string","PD_int"])#,"NP"])
       for i in xrange(8):
           print files[i]
           setup_pd_copy = setup_pd+';i='+str(i)
           setup_np_copy = setup_np+';i='+str(i)
          
           setup_pd_copy1 = setup_pd_copy+"; p1 = pd.read_csv(files[i], names=('A1','B1','C1','D','E1')); p2 = pd.read_csv('../data/smaller_file_million_original.csv', names=('A2','B2','C2','D','E2'))"
           setup_pd_copy2 = setup_pd_copy+"; p1 = pd.read_csv(files[i], names=('A','B1','C1','D1','E')); p2 = pd.read_csv('../data/smaller_file_million_original.csv', names=('A','B2','C2','D2','E'))"
           setup_np_copy = setup_np_copy+"; n=np.load('"+files[i].split(".csv")[0]+"_only_num.npz')['arr_0']; "
           pd_time_elapsed_string = timeit("pd.merge(p1,p2,on=['D'],how='inner')",setup=setup_pd_copy1,number=3)
           pd_time_elapsed_int = timeit("pd.merge(p1,p2,on=['E'],how='inner')",setup=setup_pd_copy2,number=3)

           csv_writer.writerow([round(pd_time_elapsed_string,ndigits=3),round(pd_time_elapsed_int,ndigits=3)])



files_list = ("../data/smaller_file_ten.csv","../data/smaller_file_hundred.csv", "../data/smaller_file_thousand.csv", "../data/smaller_file_tenthousand.csv", "../data/smaller_file_hundredthousand.csv","../data/smaller_file_million.csv","../data/smaller_file_tenmillion.csv","../data/smaller_file_hundredmillion.csv")
benchmark(files_list,"../data/merge.csv")
       
