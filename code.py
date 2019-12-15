import pandas as pd
import numpy as np
import pyodbc


# pull new sector structure

dataFile = 'S:/......;'
new_sectors = pd.read_excel(dataFile, sheet_name='companies',header=0)

# pull current historical table
data_base = pyodbc.connect('....')
sqlQ = "SELECT * FROM company_table"
company_hist = pd.read_sql(sqlQ,data_base)

 
# sets the indices
company_hist = company_hist.set_index('id_company')
new_sectors = new_sectors.set_index('id_company')

 
# joins historical company with new sector table
com_hist_new_sec = company_hist.join(new_sectors,on='id_company',how='left')
 
# NULL is loeaded sometimes as non(correctly) and None(incorrectly), the below fix the problem
com_hist_new_sec = com_hist_new_sec.fillna(value=np.nan)


# list of datapoints that need to be averaged
final_list_of_columns = ["column1","column2",...]


# the function that does average
def average_calc (column, weighting, df):
    ave = df.groupby(['date','sector_new']).apply(lambda x: np.average(x[column], weights=x[weighting]))
    return ave

# list of columns
columns = list(com_hist_new_sec)

# length of list of columns
columns_length = len(columns)

# list of weighting methods
weighting_method = ['equity', 'assets', 'simple_average']

# extra column named simple average and filled with ones
com_hist_new_sec['simple_average'] = 1

# the two loops that are doing whole work by creating the dataframes inside of the list which is eventually concated
final_result = []

for j in weighting_method:
    list_of_results = []
    for i in range(columns_length):
        if columns[i] in final_list_of_columns:
            average_calc_result = average_calc(columns[i], j, com_hist_new_sec)
            average_calc_result = average_calc_result.rename(columns[i])
            list_of_results.append(average_calc_result)

    df_final = pd.concat(list_of_results, axis=1)
    df_final['weighting_method'] = j
    df_final = df_final.reset_index()
    df_final = df_final.set_index(['date', 'sector_new', 'weighting_method'])
    final_result.append(df_final)

final_result = pd.concat(final_result, axis=0)
final_result = final_result.reset_index()