import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import lib.graficos as graficos 
import plotly.graph_objects as go
import lib.filtros as filtro

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

     anio_maximo = df.Anio.max()
     anio_minimo = df.Anio.min()
     anio_inicio_kpi = 1901
     sel_fecha_inicio = anio_inicio_kpi
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
     anio_maximo = df.Anio.max()
     anio_minimo = df.Anio.min()

     tabla_g = df.groupby('Anio', as_index= False).mean()
     tabla_g.reset_index(inplace=True)

     df2 = df[((df['Anio'])== anio_minimo)]
     df3 = df[((df['Anio'])== anio_maximo)]
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

     if region == 'Personalizado' and len(seleccion_paises)>0:  #todo el proceso de vuelta
                
                df = df[df['Pais'].isin(seleccion_paises)]
                
                
                
                tabla_g = df.groupby('Anio', as_index= False).mean()
                tabla_g.reset_index(inplace=True)

                #mean_siglo_XX = tabla_g[tabla_g['Anio']<2001]['temperatura'].mean()
                
                # KPI y Metricas

                #media_siglo_XX = mean_siglo_XX
                #temperatura_limite = 1.5+mean_siglo_XX
                
                ultimo_anio = tabla_g.Anio.max()
                #media_actual = tabla_g[tabla_g['Anio'] == ultimo_anio ]['temperatura'].mean()
                    
                #kpi_estado = ((media_actual - media_siglo_XX)/ (temperatura_limite - media_siglo_XX))*100
                anio_maximo = df.Anio.max()
                anio_minimo = df.Anio.min()

                df2 = df[((df['Anio'])== anio_minimo)]
                df3 = df[((df['Anio'])== anio_maximo)]

                
                df4= pd.merge(df2,df3, on= ['Pais', 'ISO'])

                df4['diferencia'] = df4['temperatura_y']-df4['temperatura_x']
                tabla_g2 = df4.sort_values(by= 'diferencia', ascending= False).head(5)
                tabla_g2.reset_index(inplace=True, drop=True)

                tabla_g4 = df4.sort_values(by= 'diferencia', ascending= True).head(5)
                tabla_g4.reset_index(inplace=True)

                # Tablas para grafico de linea comparativo
                lista_pais_mayor_aumento = tabla_g2.Pais.unique()
                if len(seleccion_paises)==1:
                    df5 = df[df.Pais.isin(lista_pais_mayor_aumento[:1])]
                    t_3_1 = df5[df5['Pais'] == lista_pais_mayor_aumento[0]]
                    #grafico
                    def grafico_temp_linea_comparativo_1(t_3_1):
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(x = t_3_1['Anio'], y = t_3_1['temperatura'], mode='lines', name = t_3_1['Pais'].values[0]))
                            fig.update_layout(
                        #title = 'Países con más aumento de temperatura',
                        paper_bgcolor= 'rgba(0,0,0,0)',
                        plot_bgcolor= '#EAEAEA',
                        title_font_color= 'rgb(252, 183, 20)',
                        font_color= 'rgb(252, 183, 20)',
                        #width=1000,
                        #height=500
                        font=dict(
                                #family="Courier New, monospace",
                                size= 18,
                                #color="#ffffff"
                                ),
                        
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
                        )
                            fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)')
                            fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)')
                            return fig

                elif len(seleccion_paises)==2:
                     df5 = df[df.Pais.isin(lista_pais_mayor_aumento[:2])]
                     t_3_1 = df5[df5['Pais'] == lista_pais_mayor_aumento[0]]
                     t_3_2 = df5[df5['Pais'] == lista_pais_mayor_aumento[1]]


                     def grafico_temp_linea_comparativo_1(t_3_1,t_3_2):
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(x = t_3_1['Anio'], y = t_3_1['temperatura'], mode='lines', name = t_3_1['Pais'].values[0]))
                            fig.add_trace(go.Scatter(x = t_3_2['Anio'], y = t_3_2['temperatura'],  mode='lines',                      name = t_3_2['Pais'].values[0]))
                            fig.update_layout(
                        #title = 'Países con más aumento de temperatura',
                        paper_bgcolor= 'rgba(0,0,0,0)',
                        plot_bgcolor= '#EAEAEA',
                        title_font_color= 'rgb(252, 183, 20)',
                        font_color= 'rgb(252, 183, 20)',
                        #width=1000,
                        #height=500
                        font=dict(
                                #family="Courier New, monospace",
                                size= 18,
                                #color="#ffffff"
                                ),
                        
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
                        )
                            fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)')
                            fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)')
                            return fig
                else:
                    df5 =df[df.Pais.isin(lista_pais_mayor_aumento[:3])]
                    t_3_1 = df5[df5['Pais'] == lista_pais_mayor_aumento[0]]
                    t_3_2 = df5[df5['Pais'] == lista_pais_mayor_aumento[1]]
                    t_3_3 = df5[df5['Pais'] == lista_pais_mayor_aumento[2]]

                #tabla mapa cromatico
                df_mapa = df4.sort_values(by= 'diferencia', ascending= False)





    # Titulo
     col_logo, col_titulo = st.columns([1,6])
     
     with col_logo:
         st.image('./images/icon_temperatura.png') 
     with col_titulo:
         st.markdown(
          """
         <h1 style="color:white;background-color:rgb(252, 183, 20);padding: 2% 2% 2% 2%;border: solid #DCDCDC;border-radius: 10px;">El Aumento de la Temperatura Promedio no Deberá Superar los 1.5°C a las Mediciones del Inicio del Siglo XX para el Año 2030</h1>
         """,unsafe_allow_html=True)


     #Contenedor
     #with st.container():
        #Columnas
     col1, col2 = st.columns(2)

     with col1:
            #st.plotly_chart(graficos.indicador_kpi_acceso(mean_siglo_XX, temperatura_limite, media_actual, 'Temperatura'), use_container_width= True)
            st.plotly_chart(graficos.indicador_vel_positivo(min_valor= mean_siglo_XX,
                                                            max_valor= temperatura_limite,
                                                            valor_actual= round(media_actual, 2), 
                                                            valor_objetivo= temperatura_limite,
                                                            unidad_medida= '°C',
                                                            titulo= "Temperatura 2021",
                                                            color= "rgb(252, 183, 20)"), use_container_width= True)
            #st.header("KPI's")
            #st.title("+ " + str(round((media_actual - media_siglo_XX), 2))+"°C")
            #st.progress(round(kpi_estado))
            

     with col2:
            st.header("Límite 2030 (°C)")
            st.title(str(round(temperatura_limite,2)))
            st.header("")
            st.header("Predicción 2030 (°C)")
            st.title( "24.2 ± 0.3")
            

     #with col3:
      #      st.header("Temperatura Promedio Límite")
       #     st.title(str(round(temperatura_limite,2)) + "°C")
            
        
     col_mapa, col_grafico = st.columns(2)

     with col_mapa:
                st.subheader("Mapa Cromático - Variación Temperatura País")
                figura_mapa = graficos.grafico_mapa_temperaturas(df_mapa, 'diferencia', 'ISO', "", "Pais")
                st.plotly_chart(figura_mapa,  use_container_width=True)


     with col_grafico:
                st.subheader("Promedio de Temperatura en Latinoamérica")
                try:
                    figura2 = graficos.grafico_linea_temperatura(tabla_g, mean_siglo_XX, 'Anio', 'temperatura', 'Año', 'Temperatura Promedio (°C)')
                    st.plotly_chart(figura2,  use_container_width=True)
                except ValueError:
                    st.error("Seleccionar por lo menos 1 (uno) Pais")
            
        #st.sidebar.title('Configuracion Graficos de Barras')          
        
        
     col_top, col_down = st.columns(2)

     with col_top:
            
            try:
                st.subheader("Países con Mayor Aumento de Temperatura")   

                if len(seleccion_paises)==1:
                    st.plotly_chart(grafico_temp_linea_comparativo_1(t_3_1),use_container_width=True)
                
                elif len(seleccion_paises)==2:
                    st.plotly_chart(grafico_temp_linea_comparativo_1(t_3_1,t_3_2),use_container_width=True)

                else:
                        figura_top = graficos.grafico_temp_linea_comparativo(t_3_1, t_3_2, t_3_3)
                        st.plotly_chart(figura_top,  use_container_width=True)
            
            except ValueError:
                st.error("Seleccionar por lo menos 1 (un) Pais")
        
     with col_down:
           
            try:
                st.subheader("Países con Menor Aumento de Temperatura")          
                figura_barra = graficos.grafico_temp_barra(tabla_g4, 'Pais', 'diferencia')
                st.plotly_chart(figura_barra,  use_container_width=True)
            except ValueError:
                st.error("Seleccionar por lo menos 1 (un) Pais")


if __name__ == '__main__':
    main()
