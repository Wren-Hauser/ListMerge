import numpy as np
import pandas
import pandas as pd

df1 = pd.read_csv('dataset1.csv')
df2 = pd.read_csv('dataset2.csv')

cols = ['legal_entity', 'counter_party', 'tier']

df3 = df1.merge(df2, on='counter_party')

df4 = df3.groupby('counter_party')['rating'].agg(max)

df3 = df3.merge(df4, on='counter_party')
df3 = df3.rename(columns={'rating_y': 'max(rating by counterparty)'})

df3['sum(value where status=ARAP)'] = np.where(df3['status'] == "ARAP", df3['value'], 0)
df3['sum(value where status=ACCR)'] = np.where(df3['status'] == "ACCR", df3['value'], 0)

ldf = df3.groupby('legal_entity').agg({'legal_entity': 'first',
                                       'counter_party': 'nunique',
                                       'tier': 'nunique',
                                       'max(rating by counterparty)': 'max',
                                       'sum(value where status=ARAP)': 'sum',
                                       'sum(value where status=ACCR)': 'sum'})

cdf = df3.groupby(['counter_party']).agg({'legal_entity': 'nunique',
                                          'counter_party': 'first',
                                          'tier': 'nunique',
                                          'max(rating by counterparty)': 'max',
                                          'sum(value where status=ARAP)': 'sum',
                                          'sum(value where status=ACCR)': 'sum'})

tdf = df3.groupby(['tier']).agg({'legal_entity': 'nunique',
                                 'counter_party': 'nunique',
                                 'tier': 'first',
                                 'max(rating by counterparty)': 'max',
                                 'sum(value where status=ARAP)': 'sum',
                                 'sum(value where status=ACCR)': 'sum'})

lcdf = df3.groupby(['legal_entity', 'counter_party']).agg({'legal_entity': 'first',
                                                           'counter_party': 'first',
                                                           'tier': 'nunique',
                                                           'sum(value where status=ARAP)': 'sum',
                                                           'sum(value where status=ACCR)': 'sum',
                                                           'max(rating by counterparty)': 'max'})

fdf = pandas.concat([ldf, cdf, tdf, lcdf])
fdf = fdf.reset_index(drop=True)
fdf.to_csv('output/pandasOut.csv', index=False)
