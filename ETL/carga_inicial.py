from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
pd.options.mode.chained_assignment = None #ignora warnings
import Levenshtein as lev



#             DEFINO FUNCIONES  


def extraer_tablas():
  credentials = service_account.Credentials.from_service_account_file('tidy-hold-359719-964ba4f0d1d8.json') #credenciales
  project_id = 'tidy-hold-359719' #nombre del proyecto
  client = bigquery.Client(credentials= credentials,project=project_id) #creo cliente
  dataset_ref = client.dataset("proyecto", project=project_id) #referencia al dataset
  dataset = client.get_dataset(dataset_ref) #dataset
  #QUERYS
  q_NDC="""SELECT * FROM `tidy-hold-359719.proyecto.NDC`"""   #query para tabla NDC
  
  q_datos_ONU = """                              
    SELECT a.GeoAreaName as Pais,
    a.TimePeriod as Anio,
    a.Value as intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI,
    b.Value as proporcion_de_la_poblacion_con_acceso_a_elecricidad,
    c.Value as proporcion_de_la_poblacion_con_dependencia_primaria_a_energias_limpias,
    d.Value as proporcion_de_energias_renovables_del_total_consumido,
    FROM `tidy-hold-359719.proyecto.energy_intensity_measured_in_terms_of_primary_energy_and_GDP` a
    LEFT JOIN `tidy-hold-359719.proyecto.proportion_of_population_with_access_to_electricity` b ON (a.GeoAreaName=b.GeoAreaName and a.TimePeriod = b.TimePeriod) AND b.Location='ALLAREA'
    LEFT JOIN `tidy-hold-359719.proyecto.proportion_of_population_with_primary_reliance_on_clean_fuels_and_technology` c ON (a.GeoAreaName=c.GeoAreaName and a.TimePeriod = c.TimePeriod)
    LEFT JOIN `tidy-hold-359719.proyecto.renewable_energy_share_in_the_total_final_energy_consumption` d ON (a.GeoAreaName=d.GeoAreaName and a.TimePeriod = d.TimePeriod)
    """
  q_energyco2 = """
    SELECT Country AS Pais, Year AS Anio, CO2_emission AS Emisiones_de_CO2
    FROM `tidy-hold-359719.proyecto.energyco2`
    WHERE Energy_type = 'all_energy_types'
    """
  
  q_Temperaturas = """
    SELECT * FROM `tidy-hold-359719.proyecto.temperaturas`
    """

  q_porcentaje_compromiso = """
    SELECT * FROM `tidy-hold-359719.proyecto.porcentraje_compromiso`
    """
  NDC = client.query(q_NDC).to_dataframe() #dataframe de NDC
  datos_ONU = client.query(q_datos_ONU).to_dataframe() #dataframe de datos onu
  energyco2 = client.query(q_energyco2).to_dataframe() 
  temperaturas = client.query(q_Temperaturas).to_dataframe()
  porcentaje_compromiso = client.query(q_porcentaje_compromiso).to_dataframe()  
  return NDC,datos_ONU,energyco2,temperaturas,porcentaje_compromiso

def traductor(df):
  for i in range(len(df)):
    for j in range(len(NDC)):
        if lev.ratio(df.Pais[i], NDC.Pais[j]) > 0.8:
            df.Pais[i]= NDC.Pais[j]
  df.loc[df.Pais=='World', 'Pais']='Mundo'
  df.loc[df.Pais=='United States', 'Pais']='Estados Unidos'
  df.loc[df.Pais=='Dominican Republic', 'Pais']='República Dominicana'
  df.loc[df.Pais=='Saint Kitts and Nevis', 'Pais']='San Cristóbal y Nieves'
  df.loc[df.Pais=='Peru', 'Pais']='Perú'
  df.loc[df.Pais=='Haiti', 'Pais']='Haití'
  return df

#defino otro traductor pq ONU tiene otros nombres
def traductor_ONU(df):
  for i in range(len(df)):
    for j in range(len(NDC)):
        if lev.ratio(df.Pais[i], NDC.Pais[j]) > 0.8:
            df.Pais[i]= NDC.Pais[j]
  df.loc[df.Pais=='World', 'Pais']='Mundo'
  df.loc[df.Pais=='United States', 'Pais']='Estados Unidos'
  df.loc[df.Pais=='Dominican Republic', 'Pais']='República Dominicana'
  df.loc[df.Pais=='Saint Kitts and Nevis', 'Pais']='San Cristóbal y Nieves'
  df.loc[df.Pais=='Peru', 'Pais']='Perú'
  df.loc[df.Pais=='Haiti', 'Pais']='Haití'
  df.loc[df.Pais=='Bolivia (Plurinational State of)', 'Pais']='Bolivia'
  df.loc[df.Pais=='Venezuela (Bolivarian Republic of)', 'Pais']='Venezuela'
  return df

def filtro_latam(df):
  df = df[df.Pais.isin(NDC.Pais.unique().tolist())]
  df.reset_index(drop=True, inplace=True)
  return df

def carga(df):
  credentials = service_account.Credentials.from_service_account_file('tidy-hold-359719-964ba4f0d1d8.json') #credenciales
  project_id = 'tidy-hold-359719' #nombre del proyecto
  client = bigquery.Client(credentials= credentials,project=project_id) #creo cliente
  table_id = "tidy-hold-359719.comision_latinoamericana."+[ k for k,v in globals().items() if v is df][0]
  #configuracion de la carga
  job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  
  # Make an API request.
  job.result()  # Wait for the job to complete.
  table = client.get_table(table_id)  # Make an API request.
  print("Cargadas {} filas y {} columnas a {}".format(table.num_rows, len(table.schema), table_id))

def nans(df):
  df.fillna(-1,inplace=True) #nan con -1 para despues filtrarlos
def erroneos(df):
  df.replace({'>95':96,'NaN':-1,'<5':4},inplace=True)






NDC, datos_ONU, energyco2,temperaturas,porcentaje_compromiso = extraer_tablas()


NDC.rename(columns={'string_field_0':'Pais'},inplace=True)


temperaturas.columns=['Anio', 'Temperatura', 'Cod_Pais']




#carga de temperatura
credentials = service_account.Credentials.from_service_account_file('tidy-hold-359719-964ba4f0d1d8.json') #credenciales
project_id = 'tidy-hold-359719' #nombre del proyecto
client = bigquery.Client(credentials= credentials,project=project_id) #creo cliente
table_id = "tidy-hold-359719.comision_latinoamericana.temperatura"
job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    #schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        #bigquery.SchemaField("Anio", bigquery.enums.SqlTypeNames.DATE)
        # Indexes are written if included in the schema by name.
    #    bigquery.SchemaField("Pais", bigquery.enums.SqlTypeNames.DATE)
    #],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(
    temperaturas, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)






# FILTRO Y CARGA DATOS ONU

datos_ONU = traductor_ONU(datos_ONU)
datos_ONU = filtro_latam(datos_ONU)
nans(datos_ONU)
erroneos(datos_ONU)
datos_ONU['proporcion_de_la_poblacion_con_dependencia_primaria_a_energias_limpias'] = pd.to_numeric(datos_ONU['proporcion_de_la_poblacion_con_dependencia_primaria_a_energias_limpias'])
carga(datos_ONU)


# FILTRO Y CARGA DE ENERGY CO2
energyco2 = traductor(energyco2)
energyco2 = filtro_latam(energyco2)
nans(energyco2)
erroneos(energyco2)
carga(energyco2)



#PORCENTAJE COMPROMISO


porcentaje_compromiso.columns=['Pais', 'Cod_Pais', 'Compromiso']
#carga de porcentaje compromiso

credentials = service_account.Credentials.from_service_account_file('tidy-hold-359719-964ba4f0d1d8.json') #credenciales
project_id = 'tidy-hold-359719' #nombre del proyecto
client = bigquery.Client(credentials= credentials,project=project_id) #creo cliente
table_id = "tidy-hold-359719.comision_latinoamericana.compromiso"
job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    #schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        #bigquery.SchemaField("Anio", bigquery.enums.SqlTypeNames.DATE)
        # Indexes are written if included in the schema by name.
    #    bigquery.SchemaField("Pais", bigquery.enums.SqlTypeNames.DATE)
    #],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(
    porcentaje_compromiso, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)



print('** CARGA DE DATOS COMPLETA**')