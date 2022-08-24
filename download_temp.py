import pandas as pd
import requests
from os import remove

def get_table(iso_code):
    print('Descargando "'+iso_code+'"')
    url = 'https://climateknowledgeportal.worldbank.org/cru/cru/timeseries/tas/annual/climatology/historical/country/'
    page = requests.get(url+iso_code)
    if page.status_code == 200:
        txt = 'year' + str(page.content)[2:].replace('\\n', '\n').split('\n', 1)[1][:-1]
        archivo = open('temporal.csv', 'w')
        archivo.write(txt)
        archivo.close()
        tabla = pd.read_csv('temporal.csv', index_col=0)
        return tabla[tabla.columns[0]].rename(iso_code)
    else:
        print('No se encontró', iso_code)

Series = []
codes = ['ATG', 'ARG', 'BHS', 'BRB', 'BLZ', 'BOL', 'BRA', 'CHL', 'COL', 'CRI', 'CUB', 'DMA', 'ECU', 'SLV', 'GTM', 'GUY', 'HTI', 'HND', 'JAM', 'MEX', 'NIC', 'PAN', 'PRY', 'PER', 'DOM', 'KNA', 'VCT', 'LCA', 'SUR', 'TTO', 'URY', 'VEN']
for code in codes:
    Series.append(get_table(code))
remove('temporal.csv')
print("Se ha terminado de cargar los códigos de los países")

for i in range(len(Series)):
    serie = Series[i].copy()
    pais = serie.name
    serie = serie.reset_index()
    serie.rename(columns={pais: 'temperatura'}, inplace=True)
    serie['codigo'] = pais
    Series[i] = serie

tabla = pd.concat(Series, axis = 0)
tabla.to_csv('Temperatures.csv', index=False)
print('Se han guardado los resultados en "Temperatures.csv"')
input()