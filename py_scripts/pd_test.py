import os

import pandas as pd

print(os.getcwd())

target_file = '/home/polter/data_20190218.zip'

result = pd.DataFrame()
filenames = ['mock']
for name in filenames:
    gen = pd.read_csv(name, sep=';', compression='zip', chunksize=1000000, low_memory=False)
    counter = 0
    for chunk in gen:
        df = chunk[(chunk.show == 't')]
        df = df[['subsite_id', 'uin']]
        df.drop_duplicates()
        result = result.append(df)
        print(f'Processed chunk number {counter}')
        counter += 1
    result.drop_duplicates()
    result.subsite_id = result.subsite_id.astype(int)
    result.to_csv('/home/polter/result', index=False, sep=',')
print('stop')
