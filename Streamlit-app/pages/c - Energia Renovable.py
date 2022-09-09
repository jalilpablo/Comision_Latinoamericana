import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import lib.graficos as graficos 
import lib.filtros as filtro

st.set_page_config(
     page_title="KPI Energías Renovables",
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


def main():

   # Carga dataset 
   df = crear_dataframe('./dataset/datos_ONU.csv')
       
    
     #lista_paises
   lista_paises_latinoamerica = sorted(df.Pais.unique())
   lista_paises_latinoamerica.remove('Canadá')
   df = df.drop(df[df['Pais']=='Canadá'].index)

     # KPI y metricas
   # Calculos
   df30 = df.filter(items=['Pais','ISO','Anio','proporcion_de_energias_renovables_del_total_consumido'])
   
   # Tabla grafico de linea generarl
   tabla_g30 = df30.groupby('Anio').mean()
   tabla_g30.reset_index(inplace=True)
   
   
   anio_ultimo_registro = tabla_g30.Anio.max()
   promedio_ultimo_registro = tabla_g30[tabla_g30['Anio'] == anio_ultimo_registro].proporcion_de_energias_renovables_del_total_consumido.values[0]

   anio_maximo = df.Anio.max()
   anio_minimo = df.Anio.min()
   anio_inicio_kpi = 2015
   sel_fecha_inicio = anio_inicio_kpi -15
   sel_fecha_fin = anio_maximo
    
   st.sidebar.write('Para una correcta visualización, utilizar modo "Light". (Menu derecho-superior/ Settings/ Theme Choose: Light)')
    
   lista_periodos = filtro.lista_anios(df, 'Anio')
   periodo = st.sidebar.radio("Seleccione Periodo", ('Predeterminado', 'Personalizado'))
   if periodo == 'Predeterminado':
        st.sidebar.write('Periodo predeterminado: ', sel_fecha_inicio, '-', lista_periodos[-1])
    
   elif periodo == 'Personalizado':
        sel_fecha_fin = lista_periodos[-1]
        lista_periodo_min = [x for x in range(lista_periodos[0], sel_fecha_fin+1)]
        sel_fecha_inicio = st.sidebar.selectbox("Seleccionar Fecha Inicio", lista_periodo_min)
        lista_periodo_max = [x for x in range(sel_fecha_inicio, lista_periodos[-1]+1)]
        sel_fecha_fin = st.sidebar.selectbox("Seleccionar Fecha Fin", reversed(lista_periodo_max))

   
   df = df[(df['Anio'] >= (sel_fecha_inicio)) & (df['Anio'] <= sel_fecha_fin)]


     # Seleccion paises
   region = st.sidebar.radio("Seleccione Región", ('Latinoamerica', 'Personalizado'))

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
         st.markdown(
          """
         <h1 style="color:white;background-color:rgb(192,142, 46);padding: 2% 2% 2% 2%;border: solid #DCDCDC;border-radius: 10px;">100% de Proporción de Energía Renovable en el Consumo Final Total de Energía para el Año 2030</h1>
         """,unsafe_allow_html=True)
         #st.image('./images/titulo_proporcion.png')

   
   df['ISO']= df['Pais'].map({'Antigua y Barbuda':'ATG','Argentina':'ARG','Bahamas':'BHS','Barbados':'BRB','Belice':'BLZ',
                        'Bolivia':'BOL','Brasil':'BRA','Chile':'CHL','Colombia':'COL','Costa Rica':'CRI','Cuba':'CUB','Dominica':'DMA',
                        'Ecuador':'ECU','El Salvador':'SLV','Guatemala':'GTM','Guyana':'GUY','Haití':'HTI','Honduras':'HND',
                        'Jamaica':'JAM','México':'MEX','Nicaragua':'NIC','Panamá':'PAN','Paraguay':'PRY','Perú':'PER','República Dominicana':'DOM',
                        'San Cristóbal y Nieves':'KNA','San Vicente y las Granadinas':'VCT','Santa Lucía':'LCA','Surinam':'SUR','Trinidad y Tobago':'TTO',
                        'Uruguay':'URY','Venezuela':'VEN'})

  
   
        
        #filtro pais
   if region == 'Personalizado' and len(seleccion_paises)>0:
        df = df[df['Pais'].isin(seleccion_paises)]

   # Calculos
   df30 = df.filter(items=['Pais','ISO','Anio','proporcion_de_energias_renovables_del_total_consumido'])
   
   # Tabla grafico de linea generarl
   tabla_g30 = df30.groupby('Anio').mean()
   tabla_g30.reset_index(inplace=True)

    # Tabla grafico de barras top paises mayor proporcion
   df31 = df30[((df30['Anio'])== sel_fecha_fin)]
   tabla_g31 = df31.sort_values(by= 'proporcion_de_energias_renovables_del_total_consumido', ascending= False).head(5)
   tabla_g31.reset_index(inplace=True)

   #Tabla Grafico Linea Comparativo Mayores
   lista_pais_mayor_proporcion_renovable = tabla_g31.Pais.unique()
   
   

    # Tabla grafico de barras top paises menor proporcion
   df33 = df30[((df30['Anio'])== sel_fecha_fin)]
   tabla_g33 = df33.sort_values(by= 'proporcion_de_energias_renovables_del_total_consumido', ascending= True).head(5)
   tabla_g33.reset_index(inplace=True)

   #Tabla Grafico Linea Comparativo Menores
   lista_pais_menor_proporcion_renovable = tabla_g33.Pais.unique()
   
   

   

     
     # TARJETAS
   col1, col2 = st.columns(2)

   with col1:
            #st.plotly_chart(graficos.indicador_kpi_acceso(0, 100, promedio_ultimo_registro, 'Porcentaje Energía renovable'), use_container_width= True)
            st.plotly_chart(graficos.indicador_vel_positivo(min_valor= 0,
                                                            max_valor= 100,
                                                            valor_actual= round(promedio_ultimo_registro), 
                                                            valor_objetivo= 100,
                                                            unidad_medida= '%',
                                                            titulo= "Energía Renovable (2019)",
                                                            color= "rgb(192,142, 46)"), use_container_width= True)
            #st.header("Progreso KPI's")
            #st.title("Calcular KPI")
            #st.progress(round(promedio_ultimo_registro))
            

   with col2:
            st.header("Objetivo 2030 (%)")
            st.title("100")
            st.header("")
            
            st.header("Predicción 2030 (%)")
            st.title( "21.8 ± 0.4")

   #with col3:
    #        st.header("Porcentaje Objetivo")
     #       st.title("100 %")
            

     # GRAFICOS CENTRALES   
   col_graf_1, col_graf_2 = st.columns(2)

   with col_graf_1:
                st.subheader('Proporción Energías Renovables')
                figura_mapa = graficos.grafico_mapa_renovables(df31, 'proporcion_de_energias_renovables_del_total_consumido', 'ISO', "", "Pais")
                st.plotly_chart(figura_mapa, use_container_width= True)


   with col_graf_2:
                st.subheader('Consumo Energía Renovable ')
                try:
                    
                    figura2 = graficos.grafico_linea_latinoamerica_renovables(tabla_g30, 'Anio', 'proporcion_de_energias_renovables_del_total_consumido','Años', 'Prop Energía Renovable (%)')
                    st.plotly_chart(figura2, use_container_width= True)
                except ValueError:
                    st.error("Seleccionar por lo menos 1 (uno) Pais")
            
        #st.sidebar.title('Configuracion Graficos de Barras')          


     # GRAFICOS INFERIORES   
   col_graf_3, col_graf_4 = st.columns(2)

   with col_graf_3:
            
            try:
               st.subheader('Países con Mayor Proporción')
               opciones_mayor = st.slider('Seleccionar Cantidad de Paises.', 3, 5, 3)
               #opciones_mayor = opciones_mayor-1
               df32 = df30[df30.Pais.isin(lista_pais_mayor_proporcion_renovable[:opciones_mayor])]
               figura_top = graficos.grafico_linea_comparativo_renovables(df32, 'Anio', 'proporcion_de_energias_renovables_del_total_consumido', 'Pais', 'Anio', 'Prop. Energias Renovables (%)')
               st.plotly_chart(figura_top, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")
        
   with col_graf_4:
           
            try:
               st.subheader('Países con Menor Proporción')
               opciones_menor = st.slider('Seleccionar Cantidad de Paises', 3, 5, 3)
               df34 = df30[df30.Pais.isin(lista_pais_menor_proporcion_renovable[:opciones_menor])]
                                  
               figura_top = graficos.grafico_linea_comparativo_renovables(df34, 'Anio', 'proporcion_de_energias_renovables_del_total_consumido', 'Pais', 'Anio', 'Prop. Energías Renovables (%)')
               st.plotly_chart(figura_top, use_container_width= True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (uno) Pais")


if __name__ == '__main__':
    main()