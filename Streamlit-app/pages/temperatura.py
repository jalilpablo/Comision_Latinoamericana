import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import lib.graficos as graficos 
import plotly.graph_objects as go

st.set_page_config(
     page_title="KPI Temperaturas",
     page_icon="🌍",
     layout="wide",
     initial_sidebar_state= "collapsed",
)

def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)

local_css("./styles/css_temperatura.css")


#@st.cache
def crear_dataframe(nombre_archivo):
    df = pd.read_csv('./dataset/'+nombre_archivo)
    return df


def main():


     df = crear_dataframe('Temperatures.csv')
     df['Pais']= df['codigo'].map({'ATG':'Antigua y Barbuda','ARG':'Argentina','BHS':'Bahamas','BRB':'Barbados','BLZ':'Belice',
                              'BOL':'Bolivia','BRA':'Brasil','CAN':'Canadá','CHL':'Chile','COL':'Colombia','CRI':'Costa Rica','CUB':'Cuba','DMA':'Dominica',
                              'ECU':'Ecuador','USA':'Estados Unidos','SLV':'El Salvador','GTM':'Guatemala','GUY':'Guyana','HTI':'Haití','HND':'Honduras',
                              'JAM':'Jamaica','MEX':'México','NIC':'Nicaragua','PAN':'Panamá','PRY':'Paraguay','PER':'Perú','DOM':'República Dominicana',
                              'KNA':'San Cristóbal y Nieves','VCT':'San Vicente y las Granadinas','LCA':'Santa Lucía','SUR':'Surinam','TTO':'Trinidad y Tobago',
                              'URY':'Uruguay','VEN':'Venezuela'})
     df = df.rename(columns={'year':'Anio', 'codigo': 'ISO'})
     tabla_g = df.groupby('Anio', as_index= False).mean()
     tabla_g.reset_index(inplace=True)

     mean_siglo_XX = tabla_g[tabla_g['Anio']<2001]['temperatura'].mean()
     
    # KPI y Metricas

     media_siglo_XX = mean_siglo_XX
     temperatura_limite = 1.5+mean_siglo_XX
     
     ultimo_anio = tabla_g.Anio.max()
     media_actual = tabla_g[tabla_g['Anio'] == ultimo_anio ]['temperatura'].mean()
        
     kpi_estado = ((media_actual - media_siglo_XX)/ (temperatura_limite - media_siglo_XX))*100

     df2 = df[((df['Anio'])==1901)]
     df3 = df[((df['Anio'])==2021)]
     df4= pd.merge(df2,df3, on= ['Pais', 'ISO'])

     df4['diferencia'] = df4['temperatura_y']-df4['temperatura_x']
     tabla_g2 = df4.sort_values(by= 'diferencia', ascending= False).head(5)
     tabla_g2.reset_index(inplace=True, drop=True)

     tabla_g4 = df4.sort_values(by= 'diferencia', ascending= True).head(5)
     tabla_g4.reset_index(inplace=True)

    # Tablas para grafico de linea comparativo
     lista_pais_mayor_aumento = tabla_g2.Pais.unique()
     df5 = df[df.Pais.isin(lista_pais_mayor_aumento[:3])]
     t_3_1 = df5[df5['Pais'] == lista_pais_mayor_aumento[0]]
     t_3_2 = df5[df5['Pais'] == lista_pais_mayor_aumento[1]]
     t_3_3 = df5[df5['Pais'] == lista_pais_mayor_aumento[2]]

     #tabla mapa cromatico
     df_mapa = df4.sort_values(by= 'diferencia', ascending= False)


     with st.sidebar:
        st.button('Introduccion')
        #st.button('Hoja 1')
        #st.button('hoja 2')

     #lista_paises = sorted(df.Pais.unique())
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
         st.image('./images/icon_temperatura.png') 
     with col_titulo:
         st.image('./images/titulo_temperatura.png')


     #Contenedor
     #with st.container():
        #Columnas
     col1, col2, col3 = st.columns(3)

     with col1:
            st.plotly_chart(graficos.indicador_kpi_acceso(mean_siglo_XX, temperatura_limite, media_actual, 'Temperatura'), use_container_width= True)
            
            #st.header("KPI's")
            #st.title("+ " + str(round((media_actual - media_siglo_XX), 2))+"°C")
            #st.progress(round(kpi_estado))
            

     with col2:
            st.header("Temperatura Promedio Actual")

            st.title(str(round(media_actual,2)) + "°C")
            

     with col3:
            st.header("Temperatura Promedio Limite")
            st.title(str(round(temperatura_limite,2)) + "°C")
            
        
     col_mapa, col_grafico = st.columns(2)

     with col_mapa:
                st.subheader("Mapa Cromatico - Variacion Temperatura Pais")
                figura_mapa = graficos.grafico_mapa_temperaturas(df_mapa, 'diferencia', 'ISO', "Diferencia temperatura", "Pais")
                st.plotly_chart(figura_mapa,  use_container_width=True)


     with col_grafico:
                st.subheader("Promedio de temperatura en Latinoamerica")
                try:
                    figura2 = graficos.grafico_linea_temperatura(tabla_g, mean_siglo_XX, 'Anio', 'temperatura', 'Año', 'Temperatura Promedio (°C)')
                    st.plotly_chart(figura2,  use_container_width=True)
                except ValueError:
                    st.error("Seleccionar por lo menos 1 (uno) Pais")
            
        #st.sidebar.title('Configuracion Graficos de Barras')          
        
        
     col_top, col_down = st.columns(2)

     with col_top:
            
            try:
                st.subheader("Paises con mayor aumento de Temperatura")      
                figura_top = graficos.grafico_temp_linea_comparativo(t_3_1, t_3_2, t_3_3)
                st.plotly_chart(figura_top,  use_container_width=True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")
        
     with col_down:
           
            try:
                st.subheader("Paises con menor aumento de Temperatura")          
                figura_barra = graficos.grafico_temp_barra(tabla_g4, 'Pais', 'diferencia')
                st.plotly_chart(figura_barra,  use_container_width=True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")


if __name__ == '__main__':
    main()
