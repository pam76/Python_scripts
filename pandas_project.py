import pandas as pd
import sqlite3


conn = sqlite3.connect('C:/Users/paramesh/Downloads/Data Engineer_ETL Assignment.db')
c = conn.cursor()

sales = pd.read_sql_query('select * from sales', conn)
customers = pd.read_sql_query('select * from customers', conn)
orders = pd.read_sql_query('select * from orders', conn)
items = pd.read_sql_query('select * from items', conn)

# print(sales)

conn.close()

def data_processing():
    merged_df = pd.merge(sales, customers, on='customer_id', how='left')
    merged_df = pd.merge(merged_df, orders, on='sales_id', how='left')
    merged_df = pd.merge(merged_df, items, on='item_id', how='left')

    filtered_df = merged_df[(merged_df['age'] >= 18) & (merged_df['age'] <= 35)]

    grouped = filtered_df.groupby(['customer_id','age','item_name']).agg(total_quantity=('quantity', 'sum'))
    grouped = grouped.reset_index()
    grouped = grouped[grouped['total_quantity'] > 0]
    grouped['total_quantity'] = grouped['total_quantity'].astype(int)
    final_result = grouped.sort_values(by='customer_id',ascending=True)
    final_result.to_csv('C:/Users/paramesh/Documents/Target/final_result_pandas.csv',index=False, sep=';')
    print(final_result)

data_processing()
