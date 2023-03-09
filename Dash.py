import psycopg2
import pandas as pd

connection = psycopg2.connect(
    host="localhost",
    database="ladm_ttsp",
    user="postgres",
    password="1234"
)

cursor = connection.cursor()

# Ejecutar la consulta SQL para obtener los nombres de los esquemas

for i in range(1,9):
    txt = "select count(etiqueta) from rev_0" + str(i) + ".lc_terreno where etiqueta like '50%';"
    print(txt)
    cursor.execute(txt)


    esquemas = cursor.fetchall()
    print(esquemas)
# # Imprimir los nombres de los esquemas

# for esquema in esquemas:
#     print(esquema)





