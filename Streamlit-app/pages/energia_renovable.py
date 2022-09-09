import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import lib.graficos as graficos 

st.set_page_config(
     page_title="KPI Energias Renovables",
     page_icon="🌍",
     layout="wide",
     initial_sidebar_state= "collapsed",
)


def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)

local_css("./styles/css_energia_renovable.css")


#@st.cache
def crear_dataframe(nombre_archivo):
    df = pd.read_csv('./'+nombre_archivo)
    return df

# Colores Graficos
color_fuente_graf = '#FFFFFF'
color_fuente_titulo_graf = '#FFFFFF'
color_fondo_graf = 'silver'
color_marco_graf = 'rgba(0,0,0,0)'
color_dibujo_graf = '#FDC30C'
color_dibujo_graf_secundario = '#A37C01'
color_escala_mapa = 'solar_r'

transp = 'rgba(0,0,0,0)' 


def main():

   # Carga dataset 
   df = crear_dataframe('./dataset/datos_ONU.csv')
       
    
     #lista_paises
   lista_paises_latinoamerica = sorted(df.Pais.unique())


     # Seleccion paises
   region = st.sidebar.radio("Seleccione Region", ('Latinoamerica', 'Personalizado'))

   if region == 'Latinoamerica':
        seleccion_paises =  lista_paises_latinoamerica
   #elif region == 'Toda America':
        #seleccion_paises = lista_paises
   elif region == 'Personalizado':
        seleccion_paises = st.sidebar.multiselect('Seleccion Paises', options= lista_paises_latinoamerica)
     
   
   
   # Titulo
   col_logo, col_titulo = st.columns([1,6])
     
   with col_logo:
         st.image('./images/icon_proporcion.png') 
   with col_titulo:
         st.image('./images/titulo_proporcion.png')

   df = df.drop(df[df['Pais']=='Canadá'].index)
   df['ISO']= df['Pais'].map({'Antigua y Barbuda':'ATG','Argentina':'ARG','Bahamas':'BHS','Barbados':'BRB','Belice':'BLZ',
                        'Bolivia':'BOL','Brasil':'BRA','Chile':'CHL','Colombia':'COL','Costa Rica':'CRI','Cuba':'CUB','Dominica':'DMA',
                        'Ecuador':'ECU','El Salvador':'SLV','Guatemala':'GTM','Guyana':'GUY','Haití':'HTI','Honduras':'HND',
                        'Jamaica':'JAM','México':'MEX','Nicaragua':'NIC','Panamá':'PAN','Paraguay':'PRY','Perú':'PER','República Dominicana':'DOM',
                        'San Cristóbal y Nieves':'KNA','San Vicente y las Granadinas':'VCT','Santa Lucía':'LCA','Surinam':'SUR','Trinidad y Tobago':'TTO',
                        'Uruguay':'URY','Venezuela':'VEN'})

   # Calculos
   df30 = df.filter(items=['Pais','ISO','Anio','proporcion_de_energias_renovables_del_total_consumido'])
   
   # Tabla grafico de linea generarl
   tabla_g30 = df30.groupby('Anio').mean()
   tabla_g30.reset_index(inplace=True)

    # Tabla grafico de barras top paises mayor proporcion
   df31 = df30[((df30['Anio'])==2019)]
   tabla_g31 = df31.sort_values(by= 'proporcion_de_energias_renovables_del_total_consumido', ascending= False).head(5)
   tabla_g31.reset_index(inplace=True)

   #Tabla Grafico Linea Comparativo Mayores
   lista_pais_mayor_proporcion_renovable = tabla_g31.Pais.unique()
   df32 = df30[df30.Pais.isin(lista_pais_mayor_proporcion_renovable[:3])]
   tabla_g32 = df32
   tabla_g32.reset_index(inplace=True)

    # Tabla grafico de barras top paises menor proporcion
   df33 = df30[((df30['Anio'])==2019)]
   tabla_g33 = df33.sort_values(by= 'proporcion_de_energias_renovables_del_total_consumido', ascending= True).head(5)
   tabla_g33.reset_index(inplace=True)

   #Tabla Grafico Linea Comparativo Menores
   lista_pais_menor_proporcion_renovable = tabla_g33.Pais.unique()
   df34 = df30[df30.Pais.isin(lista_pais_menor_proporcion_renovable[:3])]
   tabla_g34 = df34
   tabla_g34.reset_index(inplace=True)

   # KPI y metricas
   anio_ultimo_registro = tabla_g30.Anio.max()
   promedio_ultimo_registro = tabla_g30[tabla_g30['Anio'] == anio_ultimo_registro].proporcion_de_energias_renovables_del_total_consumido.values[0]

     
     # TARJETAS
   col1, col2, col3 = st.columns(3)

   with col1:
            st.plotly_chart(graficos.indicador_kpi_acceso(0, 100, promedio_ultimo_registro, 'Porcentaje Energia renovable'), use_container_width= True)
            #st.header("Progreso KPI's")
            #st.title("Calcular KPI")
            #st.progress(round(promedio_ultimo_registro))
            

   with col2:
            st.header("Porcentaje Actual")

            st.title( str(round(promedio_ultimo_registro)) +" %")
            

   with col3:
            st.header("Porcentaje Objetivo")
            st.title("100 %")
            

     # GRAFICOS CENTRALES   
   col_graf_1, col_graf_2 = st.columns(2)

   with col_graf_1:
                st.subheader('Mapa Cromatico Proporcion Energias Renovables')
                figura_mapa = graficos.grafico_mapa_renovables(df31, 'proporcion_de_energias_renovables_del_total_consumido', 'ISO', "Energia Limpias", "Pais")
                st.plotly_chart(figura_mapa, use_container_width= True)


   with col_graf_2:
                st.subheader('Consumo Energia Renovable ')
                try:
                    
                    figura2 = graficos.grafico_linea_latinoamerica_renovables(tabla_g30, 'Anio', 'proporcion_de_energias_renovables_del_total_consumido','Años', 'Prop Energia Renovable')
                    st.plotly_chart(figura2, use_container_width= True)
                except ValueError:
                    st.error("Seleccionar por lo menos 1 (uno) Pais")
            
        #st.sidebar.title('Configuracion Graficos de Barras')          


     # GRAFICOS INFERIORES   
   col_graf_3, col_graf_4 = st.columns(2)

   with col_graf_3:
            
            try:
               st.subheader('Países con países con mayor proporción de de energía renovable en el consumo final total de energía')
               figura_top = graficos.grafico_linea_comparativo_renovables(tabla_g32, 'Anio', 'proporcion_de_energias_renovables_del_total_consumido', 'Pais', 'Anio', 'Prop. Energias Renovables')
               st.plotly_chart(figura_top, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")
        
   with col_graf_4:
           
            try:
               st.subheader('Países con países con mayor proporción de de energía renovable en el consumo final total de energía')           
               figura_top = graficos.grafico_linea_comparativo_renovables(tabla_g34, 'Anio', 'proporcion_de_energias_renovables_del_total_consumido', 'Pais', 'Anio', 'Prop. Energias Renovables')
               st.plotly_chart(figura_top, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")


if __name__ == '__main__':
    main()