import psycopg2
import pandas as pd
from shapely.wkb import loads
import os
os.environ['USE_PYGEOS'] = '0'

import geopandas as gpd
import matplotlib.pyplot as plt

import binascii

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
    query = f"""select count(etiqueta) from rev_0{str(_)}.lc_terreno where etiqueta like '18%';"""
    cursor.execute(query)
    No_poly_SVC.append(cursor.fetchone()[0])

    query = f"""select count(etiqueta) from rev_0{str(_)}.lc_terreno where etiqueta like '50%';"""
    cursor.execute(query)
    No_poly_MAC.append(cursor.fetchone()[0])

# print(No_poly_MAC)
# print(No_poly_SVC)

##Predio en ocupación con FMI NOT NULL
result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
    from rev_0{str(_)}.lc_predio as P
    inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
    inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
    inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
    left join rev_0{str(_)}.lc_condicionprediotipo as CP on P.condicion_predio = CP.t_id 
    left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
    where DT.ilicode = 'Ocupación' and P.matricula_inmobiliaria is not null"""  
    cursor.execute(query)
    result.append(cursor.fetchall())

# print(result)

##Predios con único interesado y fracción de derecho < 1
result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
from rev_0{str(_)}.lc_predio as P
inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
left join rev_0{str(_)}.lc_interesado as I on D.interesado_lc_interesado = I.t_id
left join rev_0{str(_)}.lc_agrupacioninteresados as AGI on D.interesado_lc_agrupacioninteresados = AGI.t_id 
left join rev_0{str(_)}.col_grupointeresadotipo as GIT on AGI.tipo = GIT.t_id 
left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
where GIT.dispname is null and D.fraccion_derecho < 1"""
cursor.execute(query)
result.append(cursor.fetchall())
# print(result)

##Predios con Grupo Civil y fracción derecho 1 
result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
from rev_0{str(_)}.lc_predio as P
inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
left join rev_0{str(_)}.lc_interesado as I on D.interesado_lc_interesado = I.t_id
left join rev_0{str(_)}.lc_agrupacioninteresados as AGI on D.interesado_lc_agrupacioninteresados = AGI.t_id 
left join rev_0{str(_)}.col_grupointeresadotipo as GIT on AGI.tipo = GIT.t_id 
left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
where GIT.dispname = 'Grupo civil' and D.fraccion_derecho = 1"""
cursor.execute(query)
result.append(cursor.fetchall())
# print(result)

##

result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
from rev_0{str(_)}.lc_predio as P
inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
left join rev_0{str(_)}.lc_interesado as I on D.interesado_lc_interesado = I.t_id
left join rev_0{str(_)}.lc_agrupacioninteresados as AGI on D.interesado_lc_agrupacioninteresados = AGI.t_id 
left join rev_0{str(_)}.col_grupointeresadotipo as GIT on AGI.tipo = GIT.t_id 
left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
where GIT.dispname = 'Grupo civil' and D.fraccion_derecho = 1"""
cursor.execute(query)
result.append(cursor.fetchall())

##Texto de prueba

##Predios con (Público) Baldío y FMI NOT NULL

result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
    from rev_0{str(_)}.lc_predio as P
    inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
    inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
    inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
    left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
    where PT.dispname = '(Público) Baldío' and P.matricula_inmobiliaria is not null"""  
    cursor.execute(query)
    result.append(cursor.fetchall())

# print(result)

##Predio en posesión con FMI NULL
result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
    from rev_0{str(_)}.lc_predio as P
    inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
    inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
    inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
    left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
    where DT.ilicode = 'Posesion' and P.matricula_inmobiliaria is null """  
    
    cursor.execute(query)
    result.append(cursor.fetchone())

# print(result)

##Predio en posesión con FMI NULL
result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
    from rev_0{str(_)}.lc_predio as P
    inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
    inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
    inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
    left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
    where DT.ilicode = 'Posesion' and P.matricula_inmobiliaria is null """  
    
    cursor.execute(query)
    result.append(cursor.fetchall())

# print(result)

result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
    from rev_0{str(_)}.lc_predio as P
    inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
    inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
    inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id 
    left join rev_0{str(_)}.lc_prediotipo as PT on P.tipo = PT.t_id
    where Dt.ilicode = 'Dominio' and P.matricula_inmobiliaria is null"""  
    
    cursor.execute(query)
    result.append(cursor.fetchall())

# print(result)

result = []
for _ in range(1, schema+1):
    query = f"""select P.id_operacion as QR
    from rev_0{str(_)}.lc_predio as P
    inner join rev_0{str(_)}.lc_terreno as T on P.id_operacion = T.etiqueta 
    inner join rev_0{str(_)}.lc_derecho as D on P.t_id = D.unidad
    inner join rev_0{str(_)}.lc_derechotipo as DT on D.tipo = DT.t_id     
    left join rev_0{str(_)}.lc_datosadicionaleslevantamientocatastral as DA on P.t_id = DA.lc_predio     
    left join rev_0{str(_)}.lc_destinacioneconomicatipo as DE on DA.destinacion_economica = DE.t_id
    left join rev_0{str(_)}.lc_procedimientocatastralregistraltipo as PC on DA.procedimiento_catastral_registral = PC.t_id     
    where PC.dispname = 'No requiere procedimiento' """  

cursor.execute(query)
result.append(cursor.fetchall())

# print(result)

##Representar polígonos
hex_wkb = '01060000A0A1240000010000000103000080010000006C0000006891EDA472485241A245B6F342443C410000000000000000643BDF877F4852410000006022443C410000000000000000643BDF078C4852411F85EBD12F443C4100000000000000006F1283089A485241AC1C5A446B443C4100000000000000004C378991A24852416F1283A084443C4100000000000000003BDF4F5DAC485241B6F3FD9494443C410000000000000000A8C64B4FB3485241022B87F696443C4100000000000000001058398CB9485241DBF97EAA86443C41000000000000000014AE4791BA48524117D9CE7775443C410000000000000000C3F52884BF48524117D9CED745443C41000000000000000017D9CE27C24852410000008032443C4100000000000000000E2DB26DBE4852418716D9AE29443C410000000000000000C520B06AB7485241931804B6FF433C410000000000000000CFF75313B4485241CFF75303E2433C410000000000000000CFF7530BB3485241BC7493D8D8433C41000000000000000091ED7CAFB3485241F6285C0FC1433C4100000000000000005A643B07B44852414E621058B4433C4100000000000000006666665EB44852417F6ABCD4B3433C410000000000000000A4703DB2B94852414260E5D0AB433C4100000000000000002506813DBE4852418FC2F588A3433C4100000000000000008B6CE7C3C44852413F355E5A8E433C41000000000000000014AE47D1C74852419CC4209073433C410000000000000000105839A4C74852413D0AD7A360433C4100000000000000001F85EB71C74852416DE7FB894B433C4100000000000000005839B478C34852416891EDFC2E433C410000000000000000448B6C7FBF4852411B2FDDA411433C4100000000000000003F355ED2BA4852412DB29D2FF8423C4100000000000000001B2FDD8CB5485241E3A59BE4F9423C41000000000000000085EB51A8B0485241560E2DB2F6423C410000000000000000C74B37F1AC485241E9263168F7423C41000000000000000039B4C8469948524139B4C876EF423C4100000000000000004E6210A080485241BE9F1AEFFB423C4100000000000000009CC420307A485241E3A59B6409433C410000000000000000C74B37C9724852418FC2F5E821433C410000000000000000B6F3FD4C694852412FDD24863C433C41000000000000000060E5D00262485241F2D24D4243433C410000000000000000E92631385A485241B6F3FD943D433C410000000000000000BA490C6A524852411F85EBD138433C410000000000000000D9CEF79B4A4852414260E5F031433C410000000000000000DD24069943485241EC51B81E2C433C410000000000000000C74B37993F4852416666668623433C410000000000000000819543733B485241759318241A433C4100000000000000000E2DB225334852416ABC74B30C433C4100000000000000004A0C02DB2D485241819543CB00433C410000000000000000AC1C5A9426485241C520B092F2423C410000000000000000621058291C485241E3A59BC4D9423C4100000000000000008FC2F51013485241E17A148ECA423C410000000000000000CFF7534B0B485241931804B6B6423C410000000000000000D9CEF7AB054852415839B428A8423C41000000000000000085EB51E000485241931804F696423C4100000000000000003D0AD72BFB47524148E17A9472423C4100000000000000003F355EEAF6475241F4FDD49872423C410000000000000000190456EEF44752419EEFA7A67C423C410000000000000000A245B693F747524117D9CE579B423C4100000000000000003BDF4FEDFD4752415C8FC215CD423C4100000000000000004C37899905485241A01A2F5DED423C4100000000000000001D5A64F30C4852412DB29D4F14433C410000000000000000E7FBA9A1134852415839B4082F433C410000000000000000B4C876CE1648524160E5D0C240433C410000000000000000190456EE17485241F0A7C68B53433C41000000000000000008AC1C3218485241D122DB995D433C41000000000000000014AE4701174852411283C06A6C433C410000000000000000AAF1D27D144852412FDD242675433C4100000000000000004C3789A9114852417F6ABC9477433C4100000000000000000E2DB2B5094852413789410077433C410000000000000000B6F3FDFCF1475241CDCCCC6C79433C410000000000000000560E2D02EC475241295C8FE276433C410000000000000000B29DEF7FE5475241B072681168433C410000000000000000E5D02293DF475241BE9F1A4F58433C410000000000000000AC1C5A64DD4752412FDD246655433C410000000000000000E3A59B2CD7475241B6F3FD7456433C41000000000000000062105839D3475241DBF97EEA5F433C410000000000000000F853E3EDCB475241931804366D433C4100000000000000005839B430C44752417B14AE8777433C41000000000000000083C0CAD1BA475241BC7493D885433C410000000000000000819543E3B1475241F0A7C64B84433C410000000000000000986E12A3AB47524121B072C881433C4100000000000000002506819DA7475241F2D24DA294433C410000000000000000E3A59BD4A34752415A643BFF9D433C410000000000000000355EBA19A547524196438BECA3433C410000000000000000C1CAA1BDA54752410C022B47AD433C41000000000000000054E3A573AA475241736891CDBA433C410000000000000000CFF753E3AC4752415EBA498CC8433C4100000000000000002DB29DDFB3475241D578E946D6433C4100000000000000008D976EC2BB475241736891EDE9433C4100000000000000007368910DC24752414260E510F7433C410000000000000000508D972EC6475241986E128300443C41000000000000000085EB5188C947524177BE9FFA00443C41000000000000000062105809CF475241A69BC480E5433C410000000000000000AAF1D2D5D6475241E5D0229BD5433C4100000000000000001B2FDD4CDF47524196438BECDB433C410000000000000000560E2D32E4475241FCA9F112E8433C410000000000000000B6F3FD7CE94752416DE7FB29F3433C4100000000000000007593183CF0475241BA490C22FC433C410000000000000000FED47899F44752411F85EBD1FF433C41000000000000000000000028FA475241D34D62700B443C41000000000000000091ED7CA7FE4752411D5A645B13443C4100000000000000003BDF4F3504485241F4FDD43811443C410000000000000000931804160B485241AC1C5A0417443C410000000000000000B07268C10D4852416891ED5C28443C41000000000000000017D9CE6F03485241448B6CE72F443C410000000000000000D7A3705509485241508D972E3A443C410000000000000000378941C81D4852414A0C028B51443C410000000000000000295C8F3A394852413333335370443C4100000000000000008B6CE7CB624852410AD7A37098443C4100000000000000009EEFA7C663485241508D970E6D443C4100000000000000003D0AD7BB6848524179E926B166443C4100000000000000006891EDA472485241A245B6F342443C410000000000000000'
wkb = binascii.unhexlify(hex_wkb)
geometry = loads(wkb)

# gdf = gpd.GeoDataFrame(geometry=[geometry])
# gdf.plot()
# plt.show()