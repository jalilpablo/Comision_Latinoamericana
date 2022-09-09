import pandas as pd
import datetime as dt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


#Configuraciones generales
                 
tamaño_fuente_graficos = 18

# Indicadores KPIs

def indicador_vel_positivo(min_valor, max_valor, valor_actual, valor_objetivo, unidad_medida, titulo, color ):
        #grafico tentativo
   fig = go.Figure(go.Indicator(
                          mode = "gauge+number",
                          value = valor_actual,
                          
                          delta = {'reference': valor_objetivo},
                          title = {'text': f"{titulo} ({unidad_medida})",'font_size':24},
                          gauge = {'axis': {'range': [min_valor, max_valor]},
                              'bar': {'color': color,'thickness': 1}
                              #'steps' : [{'range': [0, 20], 'color': "#f25829"},
                              #          {'range': [20, 40], 'color': "#f2a529"},
                              #          {'range': [40, 60], 'color': "#eff229"},
                              #          {'range': [60, 80], 'color': "#85e043"},
                              #          {'range': [80, 1000], 'color': "#2bad4e"}]
                              ,
                            'threshold' : {'line': {'color': "grey", 'width': 4}, 'thickness': 0.75, 'value': valor_objetivo}}
                          ))
   fig.update_layout(height=250)
   fig.update_layout(
                        margin={"r":0,"t":90,"l":0,"b":10},
                        font=dict(
                                #family="Courier New, monospace",
                                size=24,
                                #color="#ffffff"
                                ),
                        )
   return fig

def indicador_kpi_emisiones(min_valor, max_valor, valor_actual, titulo):
        #grafico
    plot_bgcolor = "#FFFFFF" 
    quadrant_colors = [plot_bgcolor, "#2bad4e", "#85e043", "#eff229", "#f2a529", "#f25829"]
    quadrant_text = ["", "<b>Very high</b>", "<b>High</b>", "<b>Medium</b>", "<b>Low</b>", "<b>Very low</b>"]
    n_quadrants = len(quadrant_colors) - 1

    current_value = valor_actual * -1
    min_value = max_valor * -1
    max_value = min_valor * -1
    hand_length = np.sqrt(2) / 4
    hand_angle = np.pi * (1 - (max(min_value, min(max_value, current_value)) - min_value) / (max_value - min_value))

    fig = go.Figure(
        data=[
            go.Pie(
                values=[0.5] + (np.ones(n_quadrants) / 2 / n_quadrants).tolist(),
                rotation=90,
                hole=0.5,
                marker_colors=quadrant_colors,
    #            text = quadrant_text, aniade texto a los colores
                textinfo="text",
                hoverinfo="skip",
            ),
        ],
        layout=go.Layout(
            showlegend=False,
            margin=dict(b=0,t=10,l=10,r=10),
            width=450,
            height=450,
            paper_bgcolor=plot_bgcolor,
            annotations=[
                go.layout.Annotation(
                    text=f"<b>{titulo}:</b><br>{round(current_value * -1)}",
                    x=0.5, xanchor="center", xref="paper",
                    y=0.25, yanchor="bottom", yref="paper",
                    showarrow=False,
                )
            ],
            shapes=[
                go.layout.Shape(
                    type="circle",
                    x0=0.48, x1=0.52,
                    y0=0.48, y1=0.52,
                    fillcolor="#333",
                    line_color="#333",
                ),
                go.layout.Shape(
                    type="line",
                    x0=0.5, x1=0.5 + hand_length * np.cos(hand_angle),
                    y0=0.5, y1=0.5 + hand_length * np.sin(hand_angle),
                    line=dict(color="#333", width=4)
                )
            ]
        )
    )
    fig.update_layout(
                        height=300, 
                        
                        margin={"r":0,"t":0,"l":0,"b":0},
                        font=dict(
                                #family="Courier New, monospace",
                                size=30,
                                #color="#ffffff"
                                ),
                        )
    return fig

def indicador_kpi_acceso(min_valor, max_valor, valor_actual, titulo):
        #grafico
    plot_bgcolor = "#FFFFFF" 
    quadrant_colors = [plot_bgcolor, "#2bad4e", "#85e043", "#eff229", "#f2a529", "#f25829"]
    quadrant_text = ["", "<b>Very high</b>", "<b>High</b>", "<b>Medium</b>", "<b>Low</b>", "<b>Very low</b>"]
    n_quadrants = len(quadrant_colors) - 1

    current_value = valor_actual
    min_value = min_valor
    max_value = max_valor
    hand_length = np.sqrt(2) / 4
    hand_angle = np.pi * (1 - (max(min_value, min(max_value, current_value)) - min_value) / (max_value - min_value))

    fig = go.Figure(
        data=[
            go.Pie(
                values=[0.5] + (np.ones(n_quadrants) / 2 / n_quadrants).tolist(),
                rotation=90,
                hole=0.5,
                marker_colors=quadrant_colors,
    #            text = quadrant_text, aniade texto a los colores
                textinfo="text",
                hoverinfo="skip",
            ),
        ],
        layout=go.Layout(
            showlegend=False,
            margin=dict(b=0,t=10,l=10,r=10),
            width=450,
            height=450,
            paper_bgcolor=plot_bgcolor,
            annotations=[
                go.layout.Annotation(
                    text=f"<b>{titulo}:</b><br>{round(current_value)}",
                    x=0.5, xanchor="center", xref="paper",
                    y=0.25, yanchor="bottom", yref="paper",
                    showarrow=False,
                )
            ],
            shapes=[
                go.layout.Shape(
                    type="circle",
                    x0=0.48, x1=0.52,
                    y0=0.48, y1=0.52,
                    fillcolor="#333",
                    line_color="#333",
                ),
                go.layout.Shape(
                    type="line",
                    x0=0.5, x1=0.5 + hand_length * np.cos(hand_angle),
                    y0=0.5, y1=0.5 + hand_length * np.sin(hand_angle),
                    line=dict(color="#333", width=4)
                )
            ]
        )
    )
    fig.update_layout(
                        height=300, 
                        margin={"r":0,"t":0,"l":0,"b":0},
                        font=dict(
                                #family="Courier New, monospace",
                                size=30,
                                #color="#ffffff"
                                ),
                        )
    return fig

# GRaficos Compromisos

def grafico_mapa_compromiso(dfb):
        fig = px.choropleth(
                    locations= dfb['ISO_x'], 
                    locationmode="ISO-3", 
                    scope="world",
                    color= dfb['Compromiso'],
                    color_continuous_scale= 'viridis',#"balance", #LE CAMBIÉ LA ESCALA DE COLORES DE BALANCE A VIRIDIS
                    #PARA QUE SE VEA MEJOR LA DIFERENCIA ENTRE ESTADOS UNIDOS Y LOS OTROS PAÍSES, SI QUERÉS SIGO BUSCANDO OTRAS
                    # ESCALAS DE COLORES    
                    hover_name= dfb['Pais'],        
                    labels={ "color": "","locations": "País"})#PUSE CO2 PORQUE ES CORTO, ESTO TRAE LAS ETIQUETAS CUANDO TE POSAS
                    #PERO TAMBIÉN ES EL TÍTULO DE LA BARRA DE COLORES. LO CORRECTO SERÍA PONER EMISIONES CO2 PERO ERA MUY LARGO

        fig.update_layout(
            #title_text = 'Países que cumplieron o no el compromiso',
            showlegend = True,
            geo = dict(
                scope='world',
                resolution=50,
                projection_type='miller',
                showcoastlines=True,
                showocean=True,
                showcountries=True,
                oceancolor='#eaeaea',
                lakecolor='#eaeaea',
                coastlinecolor='#dadada',
            fitbounds="locations"))


        fig.update_layout(
                        #height=500, 
                        margin={"r":0,"t":0,"l":0,"b":0})#ACÁ EN HEIGHT CAMBIAMOS EL TAMAÑO DEL MAPA.
        return fig        


#Graficos Emisiones

#Colores Graficos
#Colores graficos
color_fuente_graf_emisiones = '#000000'
color_fuente_titulo_graf_emisiones = '#407F46'
color_fondo_graf_emisiones = '#EAEAEA'
color_marco_graf_emisiones = '#FFFFFF'
color_dibujo_graf_emisiones = 'rgba(92,175,138, 1)'
color_escala_mapa_emisiones = 'temps'
transp = 'rgba(0,0,0,0)' 



def grafico_mapa_emisiones(df, nombre_columna_color, columna_locacion, nombre_escala, nombre_locacion):
            fig = px.choropleth(
                    locations= df[columna_locacion], 
                    locationmode="ISO-3", 
                    scope= 'world',
                    color= df[nombre_columna_color],
                    color_continuous_scale= color_escala_mapa_emisiones,#"balance", #LE CAMBIÉ LA ESCALA DE COLORES DE BALANCE A VIRIDIS
                    #PARA QUE SE VEA MEJOR LA DIFERENCIA ENTRE ESTADOS UNIDOS Y LOS OTROS PAÍSES, SI QUERÉS SIGO BUSCANDO OTRAS
                    # ESCALAS DE COLORES    
                    hover_name= df['Pais'],        
                    labels={ "color": nombre_escala,"locations": nombre_locacion})#PUSE CO2 PORQUE ES CORTO, ESTO TRAE LAS ETIQUETAS CUANDO TE POSAS
                    #PERO TAMBIÉN ES EL TÍTULO DE LA BARRA DE COLORES. LO CORRECTO SERÍA PONER EMISIONES CO2 PERO ERA MUY LARGO

            fig.update_layout(
                    
                    showlegend = False,
                    geo = dict(
                        scope= 'world',
                        resolution=50,
                        projection_type='miller',
                        showcoastlines=True,
                        showocean=True,
                        showcountries=True,
                        oceancolor='#eaeaea',
                        lakecolor='#eaeaea',
                        coastlinecolor='#dadada',
                        fitbounds="locations",
                        ),
                    font=dict(
                                #family="Courier New, monospace",
                                size=tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                    #width=600,
                    )

            fig.update_layout(
                    #height=500, 
                    margin={"r":0,"t":50,"l":0,"b":0}, #ACÁ EN HEIGHT CAMBIAMOS EL TAMAÑO DEL MAPA.
                    paper_bgcolor=color_marco_graf_emisiones,
                    plot_bgcolor=color_fondo_graf_emisiones,
                    title_font_color=color_fuente_titulo_graf_emisiones,
                    font_color=color_fuente_graf_emisiones,
                    title_x = 0.5,
                    
            )
            return fig


# Grafico de lineas
def grafico_linea_emisiones(df, nombre_columna_x, nombre_columna_y, nombre_eje_x, nombre_eje_y):
        fig = go.Figure()
        fig.add_trace(go.Scatter(
                    x= df[nombre_columna_x],
                    y= df[nombre_columna_y],
                    line={'color':color_dibujo_graf_emisiones}
                    ))

        fig.update_layout(
                        paper_bgcolor=color_marco_graf_emisiones,
                        plot_bgcolor=color_fondo_graf_emisiones,
                        title_font_color=color_fuente_titulo_graf_emisiones,
                        font_color=color_fuente_graf_emisiones,
                        #width=600, 
                        #height=500,
                        #title= titulo,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff",
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
                           )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
                    
        return fig
    
    
# Grafico Barras
def grafico_barras_emisiones(df, nombre_columna_x, nombre_columna_y, nombre_eje_x, nombre_eje_y):
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df[nombre_columna_x], y = df[nombre_columna_y], marker={'color':color_dibujo_graf_emisiones}))
        fig.update_layout(
                        #title = titulo,
                    paper_bgcolor=color_marco_graf_emisiones,
                    plot_bgcolor= color_fondo_graf_emisiones,
                    title_font_color=color_fuente_titulo_graf_emisiones,
                    font_color=color_fuente_graf_emisiones,
                    #width=600,
                    #height=500,
                    font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                    title_x = 0.5,
                    margin={"r":0,"t":0,"l":0,"b":0},
                    )
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)', title_text = nombre_eje_x)
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)', title_text = nombre_eje_y)
        return fig



# Graficos  especificos KPI Temperatura

# Colores Graficos
color_fuente_graf_tem = '#000000'
color_fuente_titulo_graf_tem = 'rgb(252, 183, 20)'
color_fondo_graf_tem = '#EAEAEA'
color_marco_graf_tem = 'rgba(0,0,0,0)'
color_dibujo_graf_tem = '#FDC30C'
color_dibujo_graf_secundario_tem = '#A37C01'
color_dibujo_linea_comparativo_tem =  '#FF5733'
color_escala_mapa_tem = 'temps'

def indicador_kpi_temp(df, columna_dato, valor_referencia):
        fig = go.Figure(go.Indicator(
                                mode = "gauge+number",
                                value = df[columna_dato].values[-1],
                                title = {'text': "Proporcion"},
                                delta = {'reference': valor_referencia},
                                domain = {'x': [0, 0.5], 'y': [0, 0.5]},
                                 
                                #width=1000,
                                #height=500,
                                ))

        return fig




def grafico_mapa_temperaturas(df, nombre_columna_color, columna_locacion, nombre_escala, nombre_locacion):
            fig = px.choropleth(
                    locations= df[columna_locacion], 
                    locationmode="ISO-3", 
                    scope= 'world',
                    color= df[nombre_columna_color],
                    color_continuous_scale= color_escala_mapa_tem,#"balance", #LE CAMBIÉ LA ESCALA DE COLORES DE BALANCE A VIRIDIS
                    #PARA QUE SE VEA MEJOR LA DIFERENCIA ENTRE ESTADOS UNIDOS Y LOS OTROS PAÍSES, SI QUERÉS SIGO BUSCANDO OTRAS
                    # ESCALAS DE COLORES    
                    hover_name= df['Pais'],        
                    labels={ "color": nombre_escala,"locations": nombre_locacion})#PUSE CO2 PORQUE ES CORTO, ESTO TRAE LAS ETIQUETAS CUANDO TE POSAS
                    #PERO TAMBIÉN ES EL TÍTULO DE LA BARRA DE COLORES. LO CORRECTO SERÍA PONER EMISIONES CO2 PERO ERA MUY LARGO

            fig.update_layout(
                    
                    showlegend = False,
                    geo = dict(
                        scope= 'world',
                        resolution=50,
                        projection_type='miller',
                        showcoastlines=True,
                        showocean=True,
                        showcountries=True,
                        oceancolor='#eaeaea',
                        lakecolor='#eaeaea',
                        coastlinecolor='#dadada',
                        fitbounds="locations",
                        ),
                    font=dict(
                                #family="Courier New, monospace",
                                size=tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                    #width=600,
                    )

            fig.update_layout(
                    #height=500, 
                    margin={"r":0,"t":50,"l":0,"b":0}, #ACÁ EN HEIGHT CAMBIAMOS EL TAMAÑO DEL MAPA.
                    paper_bgcolor=color_marco_graf_tem,
                    plot_bgcolor=color_fondo_graf_tem,
                    title_font_color=color_fuente_titulo_graf_tem,
                    font_color=color_fuente_graf_tem,
                    title_x = 0.5,
                    
            )
            return fig



def grafico_linea_temperatura(df_tabla, df_promedios,nombre_columna_x, nombre_columna_y, nombre_eje_x, nombre_eje_y):

    fig = go.Figure()
    fig.add_trace(go.Scatter(x = df_tabla[nombre_columna_x], y = df_promedios*np.ones(len(df_tabla[nombre_columna_x])), mode='lines', line={'color':color_dibujo_graf_secundario_tem, 'dash':'dash', 'width':4}, name = 'Media siglo XX'))
    fig.add_trace(go.Scatter(x = df_tabla[nombre_columna_x], y = 1.5+df_promedios*np.ones(len(df_tabla[nombre_columna_x])), mode='lines', line={'color':color_dibujo_graf_secundario_tem, 'dash':'dot', 'width':4}, name = 'Límite: + 1.5ºC'))
    fig.add_trace(go.Scatter(x = df_tabla[nombre_columna_x], y = df_tabla[nombre_columna_y], mode='lines+markers', line={'width':4}, name='Temp. ºC'))
    
    fig.update_layout(
        #title = titulo,
        paper_bgcolor = color_marco_graf_tem,
        plot_bgcolor = color_fondo_graf_tem,
        title_font_color =  color_fuente_titulo_graf_tem,
        font_color = color_fuente_graf_tem,
        #width=1000,
        #height=500,
        font=dict(
                  #family="Courier New, monospace",
                  size= tamaño_fuente_graficos,
                  #color="#ffffff"
                ),
        title_x = 0.5,
        margin={"r":0,"t":0,"l":0,"b":0},
        )
    
    fig.update_xaxes(showline=True, linewidth=0, linecolor='#FFFFFF',gridcolor= None, title_text = nombre_eje_x)
    fig.update_yaxes(showline=True, linewidth=3, linecolor='#FFFFFF',gridcolor='rgba(255,255,255,0.5)', title_text = nombre_eje_y)
    
    return fig


def grafico_temp_linea_comparativo(df1, df2, df3):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x = df1['Anio'], y = df1['temperatura'], mode='lines', name = df1['Pais'].values[0]))
        fig.add_trace(go.Scatter(x = df2['Anio'], y = df2['temperatura'], mode='lines', name = df2['Pais'].values[0]))
        fig.add_trace(go.Scatter(x = df3['Anio'], y = df3['temperatura'], mode='lines', name = df3['Pais'].values[0]))
        fig.update_layout(
                        #title = 'Países con más aumento de temperatura',
                        paper_bgcolor= color_marco_graf_tem,
                        plot_bgcolor= color_fondo_graf_tem,
                        title_font_color= color_fuente_titulo_graf_tem,
                        font_color= color_fuente_graf_tem,
                        #width=1000,
                        #height=500
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
                        )
        fig.update_xaxes(showline=False, linewidth=3, linecolor='#FFFFFF', title_text = 'Año')
        fig.update_yaxes(showline=True, linewidth=3, linecolor='#FFFFFF',gridcolor='rgba(255,255,255,0.5)', title_text = 'Temp (°C)')
        return fig


def grafico_temp_barra(df, columna_x, columna_y):
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df[columna_x], y = df[columna_y], 
                                marker={'color':'rgb(255, 187, 51)'}
                                )
                        )
        fig.update_layout(
                        #title = 'Top Países con menos aumento de temperatura entre 1901 y 2021',
                        paper_bgcolor= color_marco_graf_tem,
                        plot_bgcolor= color_fondo_graf_tem,
                        title_font_color= color_fuente_titulo_graf_tem,
                        font_color= color_fuente_graf_tem,
                        #width=700,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
                        
                        )
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)', title_text = 'Pais')
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)', title_text = 'Temp (°C)')
        return fig



#Graficos Acceso

# Colores Grafico
color_marco_graf_acceso = "#FFFFFF"
color_fondo_graf_acceso = '#EAEAEA'
color_fuente_titulo_graf_acceso = "#E5233D"
color_fuente_graf_acceso = "#000000"
color_escala_mapa_acceso = "temps_r"

def grafico_linea_latinoamerica_acceso(df_grafico, columna_x, columna_y, nombre_eje_x, nombre_eje_y):
        fig = px.line(df_grafico, 
                       x = columna_x, 
                       y= columna_y, 
                       #title= titulo,
                )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_acceso,
                        plot_bgcolor= color_fondo_graf_acceso,
                        title_font_color= color_fuente_titulo_graf_acceso,
                        font_color= color_fuente_graf_acceso,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig


def grafico_barra_top_acceso(df, columna_x, columna_y, nombre_eje_x, nombre_eje_y):
        fig = px.bar(data_frame= df,
                        x= columna_x,
                        y = columna_y,
                        #title= titulo,
                        )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_acceso,
                        plot_bgcolor= color_fondo_graf_acceso,
                        title_font_color= color_fuente_titulo_graf_acceso,
                        font_color= color_fuente_graf_acceso,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig

def grafico_barras_colores_acceso(df_inicio, df_ultimo, anio_inicio, anio_ultimo, nombre_eje_x, nombre_eje_y):
        fig = go.Figure()
        fig.add_trace(go.Bar(
                x=df_inicio['Pais'],
                y=df_inicio['promedio'],
                name= anio_inicio,
                marker_color='indianred',
                ))
        fig.add_trace(go.Bar(
                x=df_ultimo['Pais'],
                y=df_ultimo['promedio'],
                 name=anio_ultimo,
                marker_color='lightsalmon',
        ))
        fig.update_layout(
                font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig


def grafico_linea_comparativo_acceso(df_grafico, columna_x, columna_y, color_categ, nombre_eje_x, nombre_eje_y):
        fig = px.line(df_grafico,
                        x= columna_x, 
                        y= columna_y,
                        color= color_categ,
                        #title= titulo,
                        )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_acceso,
                        plot_bgcolor= color_fondo_graf_acceso,
                        title_font_color= color_fuente_titulo_graf_acceso,
                        font_color= color_fuente_graf_acceso,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        legend=dict(
                                orientation = 'h',
                                yanchor="top",
                                y=1.12,
                                xanchor="left",
                                x=0.01
                        ),
                        margin={"r":0,"t":0,"l":0,"b":0},
                )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig




def grafico_mapa_acceso(df, nombre_columna_color, columna_locacion, nombre_escala, nombre_locacion ):
        fig = px.choropleth(
                    locations= df[columna_locacion], 
                    locationmode="ISO-3", 
                    scope="world",
                    color= df[nombre_columna_color],
                    color_continuous_scale= color_escala_mapa_acceso,#"balance", #LE CAMBIÉ LA ESCALA DE COLORES DE BALANCE A VIRIDIS
                    #PARA QUE SE VEA MEJOR LA DIFERENCIA ENTRE ESTADOS UNIDOS Y LOS OTROS PAÍSES, SI QUERÉS SIGO BUSCANDO OTRAS
                    # ESCALAS DE COLORES    
                    hover_name= df['Pais'],        
                    labels={ "color": nombre_escala,"locations": nombre_locacion},
                    range_color= [70, 100])

        fig.update_layout(
                #title_text = titulo,
                showlegend = False,
        geo = dict(
                scope='world',
                resolution=110,
                projection_type='miller',
                showcoastlines=True,
                showocean=True,
                showcountries=True,
                oceancolor='#eaeaea',
                lakecolor='#eaeaea',
                coastlinecolor='#dadada',
                fitbounds="locations"),
                )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_acceso,
                        plot_bgcolor= color_fondo_graf_acceso,
                        title_font_color= color_fuente_titulo_graf_acceso,
                        font_color= color_fuente_graf_acceso,
                        #width=600,
                        #height=500, #ACÁ EN HEIGHT CAMBIAMOS EL TAMAÑO DEL MAPA.
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        
                        )

        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        return fig



#Graficos Renovables

# Colores Grafico
color_marco_graf_renovables = "#FFFFFF"
color_fondo_graf_renovables = '#EAEAEA'
color_fuente_titulo_graf_renovables = "rgb(192,142, 46)"
color_fuente_graf_renovables = "#000000"
color_escala_mapa_renovables = "temps"

def grafico_linea_latinoamerica_renovables(df_grafico, columna_x, columna_y, nombre_eje_x, nombre_eje_y):
        fig = px.line(df_grafico, 
                       x = columna_x, 
                       y= columna_y, 
                       #title= titulo,
                )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_renovables,
                        plot_bgcolor= color_fondo_graf_renovables,
                        title_font_color= color_fuente_titulo_graf_renovables,
                        font_color= color_fuente_graf_renovables,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig


def grafico_barra_top_renovables(df, columna_x, columna_y, nombre_eje_x, nombre_eje_y):
        fig = px.bar(data_frame= df,
                        x= columna_x,
                        y = columna_y,
                        #title= titulo,
                        )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_renovables,
                        plot_bgcolor= color_fondo_graf_renovables,
                        title_font_color= color_fuente_titulo_graf_renovables,
                        font_color= color_fuente_graf_renovables,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig


def grafico_linea_comparativo_renovables(df_grafico, columna_x, columna_y, color_categ, nombre_eje_x, nombre_eje_y):
        fig = px.line(df_grafico,
                        x= columna_x, 
                        y= columna_y,
                        color= color_categ,
                        #title= titulo,
                        )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_renovables,
                        plot_bgcolor= color_fondo_graf_renovables,
                        title_font_color= color_fuente_titulo_graf_renovables,
                        font_color= color_fuente_graf_renovables,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        legend=dict(
                        orientation = 'h',
                        yanchor="top",
                        y=1.12,
                        xanchor="left",
                        x=0.01
                        ),
                        margin={"r":0,"t":0,"l":0,"b":0},
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig




def grafico_mapa_renovables(df, nombre_columna_color, columna_locacion, nombre_escala, nombre_locacion ):
        fig = px.choropleth(
                    locations= df[columna_locacion], 
                    locationmode="ISO-3", 
                    scope="world",
                    color= df[nombre_columna_color],
                    color_continuous_scale= color_escala_mapa_renovables,#"balance", #LE CAMBIÉ LA ESCALA DE COLORES DE BALANCE A VIRIDIS
                    #PARA QUE SE VEA MEJOR LA DIFERENCIA ENTRE ESTADOS UNIDOS Y LOS OTROS PAÍSES, SI QUERÉS SIGO BUSCANDO OTRAS
                    # ESCALAS DE COLORES    
                    hover_name= df['Pais'],        
                    labels={ "color": nombre_escala,"locations": nombre_locacion})#PUSE CO2 PORQUE ES CORTO, ESTO TRAE LAS ETIQUETAS CUANDO TE POSAS
                    #PERO TAMBIÉN ES EL TÍTULO DE LA BARRA DE COLORES. LO CORRECTO SERÍA PONER EMISIONES CO2 PERO ERA MUY LARGO

        fig.update_layout(
                        #title_text = titulo,
                        showlegend = False,
                geo = dict(
                        scope='world',
                        resolution=110,
                        projection_type='miller',
                        showcoastlines=True,
                        showocean=True,
                        showcountries=True,
                        oceancolor='#eaeaea',
                        lakecolor='#eaeaea',
                        coastlinecolor='#dadada',
                        fitbounds="locations"),
                )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_renovables,
                        plot_bgcolor= color_fondo_graf_renovables,
                        title_font_color= color_fuente_titulo_graf_renovables,
                        font_color= color_fuente_graf_renovables,
                        #width=600,
                        #height=500, #ACÁ EN HEIGHT CAMBIAMOS EL TAMAÑO DEL MAPA.
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        
                        )

        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        return fig


# Intensidad Energetica

# Colores Graficos

color_marco_graf_intensidad = "#FFFFFF"
color_fondo_graf_intensidad = '#EAEAEA'
color_fuente_titulo_graf_intensidad = "rgb(243, 108, 37)"
color_fuente_graf_intensidad = "#000000"
color_escala_mapa_intensidad = "temps" #ver color 

def grafico_mapa_intensidad(df, nombre_columna_color, columna_locacion, nombre_escala, nombre_locacion ):
        fig = px.choropleth(
                    locations= df[columna_locacion], 
                    locationmode="ISO-3", 
                    scope="world",
                    color= df[nombre_columna_color],
                    color_continuous_scale= color_escala_mapa_intensidad,#"balance", #LE CAMBIÉ LA ESCALA DE COLORES DE BALANCE A VIRIDIS
                    #PARA QUE SE VEA MEJOR LA DIFERENCIA ENTRE ESTADOS UNIDOS Y LOS OTROS PAÍSES, SI QUERÉS SIGO BUSCANDO OTRAS
                    # ESCALAS DE COLORES    
                    hover_name= df['Pais'],        
                    labels={ "color": nombre_escala,"locations": nombre_locacion},
                    range_color= [0,5])#PUSE CO2 PORQUE ES CORTO, ESTO TRAE LAS ETIQUETAS CUANDO TE POSAS
                    #PERO TAMBIÉN ES EL TÍTULO DE LA BARRA DE COLORES. LO CORRECTO SERÍA PONER EMISIONES CO2 PERO ERA MUY LARGO

        fig.update_layout(
                #title_text = titulo,
                showlegend = False,
        geo = dict(
                scope='world',
                resolution=110,
                projection_type='miller',
                showcoastlines=True,
                showocean=True,
                showcountries=True,
                oceancolor='#eaeaea',
                lakecolor='#eaeaea',
                coastlinecolor='#dadada',
                fitbounds="locations"),
                )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_intensidad,
                        plot_bgcolor= color_fondo_graf_intensidad,
                        title_font_color= color_fuente_titulo_graf_intensidad,
                        font_color= color_fuente_graf_intensidad,
                        #width=600,
                        #height=500, #ACÁ EN HEIGHT CAMBIAMOS EL TAMAÑO DEL MAPA.
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        
                        )

        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        return fig

def grafico_linea_intensidad(df, columna_x, columna_y, nombre_eje_x, nombre_eje_y, arr_anios, val_2015, val_actual):
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x = df[columna_x], y = df[columna_y], name='Intensidad'))
        fig.add_trace(go.Scatter(x = arr_anios, y = val_2015*np.ones_like(arr_anios), mode = 'lines', line={'dash':'dash'}, name='Int. 2015'))
        # fig.add_trace(go.Scatter(x = arr_anios, y = val_objective*np.ones_like(arr_anios), mode = 'lines', line={'dash':'dash'}, name='Objetivo'))
        fig.add_trace(go.Scatter(x = [2019], y = [val_actual], mode='markers'))
        fig.add_layout_image
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_intensidad,
                        plot_bgcolor= color_fondo_graf_intensidad,
                        title_font_color= color_fuente_titulo_graf_intensidad,
                        font_color= color_fuente_graf_intensidad,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)


        return fig

def grafico_barras_intensidad(df, columna_x, columna_y, nombre_eje_x, nombre_eje_y):
        fig = px.bar(data_frame= df,
                        x= columna_x,
                        y = columna_y,
                        #title= titulo,)
                )
        fig.update_traces(marker_color='rgb(230, 77, 0)'        
                        
                        )
        fig.update_layout(
                        paper_bgcolor= color_marco_graf_intensidad,
                        plot_bgcolor= color_fondo_graf_intensidad,
                        title_font_color= color_fuente_titulo_graf_intensidad,
                        font_color= color_fuente_graf_intensidad,
                        #width=600,
                        #height=500,
                        font=dict(
                                #family="Courier New, monospace",
                                size= tamaño_fuente_graficos,
                                #color="#ffffff"
                                ),
                        title_x = 0.5,
                        margin={"r":0,"t":0,"l":0,"b":0},
        )
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_y)
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.5)',
                            title_text= nombre_eje_x)
        return fig
