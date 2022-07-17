import pandas as pd
zero_c = pd.read_excel(r'ML\comb.xlsx', sheet_name='0c')
zero_c.to_csv(r'ML\zero.csv', index=None, header=True)