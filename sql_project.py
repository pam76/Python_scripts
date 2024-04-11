import pandas as pd
import sqlite3


conn = sqlite3.connect('C:/Users/paramesh/Downloads/Data Engineer_ETL Assignment.db')
c = conn.cursor()

final_result = pd.read_sql_query('select s.customer_id,c.age,i.item_name,cast(sum(o.quantity) as int) as total_quantity from sales s left join customers c on s.customer_id = c.customer_id left join orders o on s.sales_id = o.sales_id left join items i on o.item_id = i.item_id where c.age BETWEEN 18 and 35 GROUP by s.customer_id, c.age, o.item_id having sum(o.quantity) > 0 order by s.customer_id, c.age', conn)

final_result.to_csv('C:/Users/paramesh/Documents/Target/final_result_sql.csv',index=False, sep=';')

c.close()
conn.close()

print(final_result)
