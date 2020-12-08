import pandas as pd

df = pd.DataFrame()

chk = pd.read_csv(filepath_or_buffer='/home/polter/sample.tsv.gz', chunksize=1000000, sep='\t', compression='gzip',
                  low_memory=False)

count = 0

for i in chk:
    df = pd.concat([df, i['content_id']])
    print(f'chunk {count} read')
    count += 1

unique_amount = df.nunique()

print(f'Amount of uniques is {unique_amount}')
