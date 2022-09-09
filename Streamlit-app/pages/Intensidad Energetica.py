
from re import U
import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import lib.graficos as graficos 
import plotly.graph_objects as go

st.set_page_config(
     page_title="KPI Eficiencia Energetica",
     page_icon="🌍",
     layout="wide",
     initial_sidebar_state= "collapsed",
)

def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)

local_css("./styles/css_intensidad_energetica.css")


#@st.cache
def crear_dataframe(nombre_archivo):
    df = pd.read_csv('./'+nombre_archivo)
    return df




def main():

     
    tabla = crear_dataframe('./dataset/datos_ONU.csv')
    df_paises = crear_dataframe('./dataset/Paises.csv')
    df = tabla[['Pais','Anio','intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI']].copy()
    df.rename(columns={'intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI':'Value'}, inplace=True)
    df = df.groupby('Anio').mean().reset_index()
    
    
     #lista_paises
    lista_paises_latinoamerica = sorted(tabla.Pais.unique())


     # Seleccion paises
    region = st.sidebar.radio("Seleccione Region", ('Latinoamerica', 'Personalizado'))

    if region == 'Latinoamerica':
        seleccion_paises =  lista_paises_latinoamerica
     #elif region == 'Toda America':
        #seleccion_paises = lista_paises
    elif region == 'Personalizado':
        seleccion_paises = st.sidebar.multiselect('Seleccion Paises', options= lista_paises_latinoamerica)
   
    #st.image('./images/ComisionLat1.png')


    # Calculos
    

    val_2015 = df[df['Anio'] == 2015]['Value'].values[0]
    val_objective = val_2015/2
    val_actual = df[df['Anio'] == 2019]['Value'].values[0]
    # arr_anios = np.arange(2015,2020)
    arr_anios = df.Anio.values

    # Reducción objetivo y reducción actual
    ob_red = val_2015 - val_objective
    ac_red = val_2015 - val_actual
    #print('Reducción objetivo:', ob_red)
    #print('Reducción actual:', ac_red)
    objective_percent = ac_red / ob_red
    #print('Avance del KPI:', objective_percent*100, '%')

    values2015 = tabla[tabla['Anio'] == 2015][['Pais','intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI']].set_index('Pais').rename(columns={'intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI':'Value_2015'})
    values2019 = tabla[tabla['Anio'] == 2019][['Pais','intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI']].set_index('Pais').rename(columns={'intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI':'Value_2019'})
    df_por_pais = values2015.join(values2019)

    df_por_pais['Reducción']  = df_por_pais['Value_2015'] - df_por_pais['Value_2019']
    df_por_pais['Red_esperada'] = df_por_pais['Value_2015']/2
    df_por_pais['Porcentaje_red'] = df_por_pais['Reducción'] / df_por_pais['Value_2015'] * 100
    df_por_pais['Porcentaje_cumplido'] = df_por_pais['Reducción'] / df_por_pais['Red_esperada'] * 100

    df_por_pais = pd.merge(df_por_pais, df_paises[['Pais', 'ISO', 'Region']], on= 'Pais', how='left')
    df_por_pais = df_por_pais[df_por_pais['Region'] != 'América del Norte']

    top_lejos = df_por_pais.sort_values('Porcentaje_cumplido').head().reset_index()
    top_cerca = df_por_pais.sort_values('Porcentaje_cumplido', ascending=False).head().reset_index()




     # Titulo
    col_logo, col_titulo = st.columns([1,6])
     
    with col_logo:
         st.image('./images/icon_intensidad.png') 
    with col_titulo:
         st.image('./images/titulo_intensidad.png')
     # TARJETAS
    col1, col2, col3 = st.columns(3)

    with col1:
            st.plotly_chart(graficos.indicador_kpi_acceso(0, 100, round(ac_red,2), 'Porcentaje Intensidad' ), use_container_width= True)
            
            #st.header("KPI's")
            #st.title(str(round(objective_percent*100,2)) + " %")

            #st.progress(objective_percent)
            

    with col2:
            st.header("Reduccion ultimo Registro")

            st.title(str(round(ac_red,2)))
            

    with col3:
            st.header("Objetivo Reduccion")
            st.title(str(round(ob_red,2)))
            

     # GRAFICOS CENTRALES   
    col_graf_1, col_graf_2 = st.columns(2)

    with col_graf_1:
               st.subheader("Mapa Cromatico - Valor Intensidad Energetica Pais")
               
               figura_mapa = graficos.grafico_mapa_intensidad(df_por_pais, 'Value_2019', 'ISO', "Intensidad Energetica", "Pais")
               st.plotly_chart(figura_mapa, use_container_width= True)


    with col_graf_2:
                st.subheader('Cambios Intensidad Energetica')
                try:
                    figura2 = graficos.grafico_linea_intensidad(df, 'Anio', 'Value', 'Años', 'Valores', arr_anios, val_2015, val_actual)
                    st.plotly_chart(figura2, use_container_width= True)
                except ValueError:
                    st.error("Seleccionar por lo menos 1 (uno) Pais")
            
        #st.sidebar.title('Configuracion Graficos de Barras')          


     # GRAFICOS INFERIORES   
    col_graf_3, col_graf_4 = st.columns(2)

    with col_graf_3:
            st.subheader("Paises con mayor porcentaje cumplido")
            try:
               
               figura_top = graficos.grafico_barras_intensidad(top_cerca, 'Pais', 'Porcentaje_cumplido', 'Pais', 'Porcentaje Cumplido')
               st.plotly_chart(figura_top, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")
        
    with col_graf_4:
            st.subheader("Paises con menor Porcentaje Cumplido") 
            try:
                         
               figura_barra = graficos.grafico_barras_intensidad(top_lejos, 'Pais', 'Porcentaje_cumplido', 'Pais', 'Porcentaje Cumplido')
               st.plotly_chart(figura_barra, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")


if __name__ == '__main__':
    main()