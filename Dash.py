import psycopg2
import pandas as pd

connection = psycopg2.connect(
    host="localhost",
    database="ladm_ttsp",
    user="postgres",
    password="1234"
)

cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM information_schema.schemata where schema_name like 'rev%'")
schema = cursor.fetchone()[0]

No_poly_SVC = [] 
No_poly_MAC = []
No_poly_Uribe = []
for _ in range(1,schema+1):
    query = "select count(etiqueta) from rev_0" + str(_) + ".lc_terreno where etiqueta like '18%';"
    cursor.execute(query)
    No_poly_SVC.append(cursor.fetchone()[0])

    query = "select count(etiqueta) from rev_0" + str(_) + ".lc_terreno where etiqueta like '50%';"
    cursor.execute(query)
    No_poly_MAC.append(cursor.fetchone()[0])

    # query1 = "select count(etiqueta) from rev_0" + str(i) + ".lc_terreno where etiqueta like '50370%';"
    # cursor.execute(query1)
    # No_poly_MAC.append(cursor.fetchone()[0])



    

print(No_poly_SVC)
print(No_poly_MAC)






