import pandas as pd
import timestring
import sqlalchemy
from geopy.geocoders import Nominatim

#-------------------------------------------------------------------------------------------------------#IMPORTANDO INFORME Y VUELOS_EFECTIVOS

informe = pd.read_csv("Dataset\AccidentesAviones.csv")
vuelos_efectivos = pd.read_csv("Dataset\\vuelos efectivos.csv")


#------------------------------------------------------------------------------------------------------- #NORMALIZANDO COLUMNAS Y DROPEANDO ALGUNAS EN INFORME


informe.rename(columns={"fecha":"Fecha",
                        "HORA declarada":"Hora declarada", 
                        "Ruta": "Lugar del accidente",            
                        "OperadOR":"Operador",
                        "flight_no": "Numero de vuelo",          
                        "route": "Ruta",             
                        "ac_type": "Tipo de aeronave",               
                        "registration": "Registro",             
                        "cn_ln": "Codigo de vuelo",
                        "all_aboard": "Total a bordo",
                        "PASAJEROS A BORDO":"Pasajeros a bordo",
                        "crew_aboard":"Personal a bordo",
                        "cantidad de fallecidos": "Cantidad de fallecidos",
                        "passenger_fatalities": "Pasajeros fallecidos",
                        "crew_fatalities": "Personal fallecidos",
                        "ground": "Accidente terrestre",
                        "summary": "Resumen"
                        },inplace=True)

informe.drop(columns=["Unnamed: 0","Operador","Registro","Hora declarada","Codigo de vuelo",
'Numero de vuelo',"Total a bordo","Cantidad de fallecidos","Accidente terrestre","Resumen","Ruta"],inplace=True)


#------------------------------------------------------------------------------------------------------- #NORMALIZANDO VALORES FALTANTES EN INFORME

for columna in informe.columns:
    informe[columna]= informe[columna].apply(lambda x: "sin registro" if x == "?" else x)

#------------------------------------------------------------------------------------------------------- #NORMALIZANDO INFORME [FECHA]

for i in range(len(informe)):
    informe["Fecha"][i] = timestring.Date(informe["Fecha"][i]) 

for i,e in enumerate(informe['Fecha']):
    informe['Fecha'][i] = str(e)[:11]   

informe["Fecha"] = informe["Fecha"].apply(lambda x: x[:4]) 

#-------------------------------------------------------------------------------------------------------#LIMPIEZA FILAS CON VALORES FALTANTES DE INFORME 


indices = informe[(informe["Lugar del accidente"] == "sin registro") |  
(informe["Tipo de aeronave"] == "sin registro") | 
(informe["Pasajeros a bordo"] == "sin registro") | 
(informe["Personal a bordo"] == "sin registro") |  
(informe["Personal fallecidos"] == "sin registro") |
(informe["Pasajeros fallecidos"] == "sin registro")].index

for i in indices:
    informe.drop(labels=i,inplace=True)

informe.reset_index(inplace=True,drop=True)

#-------------------------------------------------------------------------------------------------------#IMPORTANDO DATASET PAIS

pais = pd.read_csv("Dataset\pais.csv")


#-------------------------------------------------------------------------------------------------------#AGREGANDO COLUMNA PAIS

informe = pd.concat([informe,pais],axis=1)


#------------------------------------------------------------------------------------------------------- #LIMPIEZA VUELOS EFECTIVOS


lst1= []
lst2= []
for año in vuelos_efectivos.columns[14:-2]:
    vuelos_efectivos[año] = vuelos_efectivos[año].fillna(0)
    valor = int(str(vuelos_efectivos[año].sum()//10)[:-2])
    lst1.append(año)
    lst2.append(valor)

vuelos_efectivos = pd.DataFrame({"Año":lst1,"cantidad_vuelos":lst2})


#--------------------------------------------------------------------------------------------------------#CREACION TABLA EXPRESS
lst1 = []
lst2= []
for i in informe["Fecha"]:
    if i not in lst1 and int(i) >= 1970 and int(i) < 2021:
        lst1.append(i)
        lst2.append(len(informe[informe["Fecha"] == i]))
    else:
        continue

vuelos = pd.DataFrame({"año":lst1 ,"accidentes":lst2})

vuelos = pd.concat([vuelos,vuelos_efectivos],axis=1)

vuelos.drop(columns=["año"],inplace=True)



#------------------------------------------------------------------------------------------------------- #EXPORTACION DE TABLAS A MYSQL

conexion= "mysql://root:drosblock@127.0.0.1:3306/PI3"
engine = sqlalchemy.create_engine(conexion)


informe.to_sql (name="accidentes",con=conexion, if_exists="replace")
vuelos_efectivos.to_sql (name="vuelos_anuales",con=conexion, if_exists="replace")
vuelos.to_sql (name="vuelos",con=conexion, if_exists="replace")
print("se cargaron los informes")