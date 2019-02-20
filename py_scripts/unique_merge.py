import pandas as pd
import glob

filelist = glob.glob('proc_*.tsv')

first_df = pd.read_csv(filelist[0], sep=',', low_memory=False)

for i in range(1,len(filelist) - 1):
    print(f'Now we process file {filelist[i]}...')
    tmp = pd.read_csv(filelist[i], sep=',', low_memory=False)
    first_df.append(tmp)
    first_df.drop_duplicates()
    print(f'File {filelist[i]} successfully processed...')
