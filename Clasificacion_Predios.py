
##Clasificación de predios UAF 

area = 25

if area >= float(13.3) and area <= float(14.5578):
    print(f'Asignación: UAF 1')
elif area < float(13.3):
    print(f'Asignación: UAF 1')
elif area >= float(28.850) and area <= float(31.5708):
    print(f'Asignación: UAF 2')
elif area > float(13.3) and area < float(28.850):
    print(f'Asignación: UAF 2')
elif area >= float(55.1) and area <= float(58.5432):
    print(f'Asignación: UAF 3')
elif area >= float(64.3) and area <= float(81.8533):
    print(f'Asignación: UAF 4')
elif area >= float(124) and area <= float(129.6417):
    print(f'Asignación: UAF 5')

