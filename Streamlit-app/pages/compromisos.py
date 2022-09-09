import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import lib.graficos as graficos 

st.set_page_config(
     page_title="KPI Compromisos",
     page_icon="🌍",
     layout="wide",
     initial_sidebar_state= "collapsed",
)

def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)

local_css("./styles/css_compromisos.css")


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

     '''
     df = crear_dataframe('')
    
    
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
     '''
     #st.image('./images/ComisionLat1.png')
     #st.image('./images/kpi4b.png')
       # Titulo
     col_logo, col_titulo = st.columns([1,6])
     
     with col_logo:
         st.image('./images/icon_cumplimiento.png') 
     with col_titulo:
         #st.title("El 80% de los países Latinoamericanos cumplan con sus compromisos de reducción de emisiones de CO2 para el año 2030")
         st.image('./images/titulo_cumplimiento.png')
     
     
     # TARJETAS
     col1, col2, col3 = st.columns(3)

     with col1:
            st.header("Cumplimiento KPI's")
            st.title("Calcular KPI")
            #st.progress(kpi_pct)
            

     with col2:
            st.header("Definir Metrica")

            st.title("Temperatura Actual Promedio")
            

     with col3:
            st.header("Definir Metrica")
            st.title("Temperatura Promedio Limite")
            

     # GRAFICOS CENTRALES   
     col_graf_1, col_graf_2 = st.columns(2)

     with col_graf_1:
               st.subheader("GRAFICO 1")
               #figura_mapa = graficos.grafico_mapa(df_mapa, 'diferencia', 'ISO', "world", "Mapa Cromatico - Variacion Temperatura Pais", "Diferencia temperatura", "Pais", color_escala_mapa, color_fuente_graf, color_marco_graf, color_fondo_graf, color_fuente_titulo_graf)
               #st.plotly_chart(figura_mapa, use_container_width= True)


     with col_graf_2:
                #st.subheader('Emisiones CO2 - Agrupacion Anual')
                try:
                    st.subheader("GRAFICO 2")
                    #figura2 = graficos.grafico_linea_temperatura(tabla_g, mean_siglo_XX, 'Anio', 'temperatura', "Promedio de temperatura en Latinoamerica", 'Año', 'Temperatura Promedio (°C)', color_fuente_graf, color_marco_graf, color_fondo_graf, color_fuente_titulo_graf, color_dibujo_graf, color_dibujo_graf_secundario)
                    #st.plotly_chart(figura2, use_container_width= True)
                except ValueError:
                    st.error("Seleccionar por lo menos 1 (uno) Pais")
            
        #st.sidebar.title('Configuracion Graficos de Barras')          


     # GRAFICOS INFERIORES   
     col_graf_3, col_graf_4 = st.columns(2)

     with col_graf_3:
            
            try:
               st.subheader("GRAFICO 3")
               #figura_top = graficos.grafico_temp_linea_comparativo(t_3_1, t_3_2, t_3_3)
               #st.plotly_chart(figura_top, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")
        
     with col_graf_4:
           
            try:
               st.subheader("GRAFICO 4")           
               #figura_barra = graficos.grafico_temp_barra(tabla_g4, 'Pais', 'diferencia')
               #st.plotly_chart(figura_barra, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")


if __name__ == '__main__':
    main()