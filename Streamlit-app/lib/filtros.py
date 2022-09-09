# Filtro para usar en streamlit

def lista_paises(df, nombre_columna):
    lista_paises = sorted(df['Pais'].unique())
    return lista_paises

def lista_anios(df, nombre_columna):
    anio_inicio = df[nombre_columna].min()
    anio_fin = df[nombre_columna].max()
    lista = list(range(anio_inicio ,(anio_fin + 1)))
    return lista
