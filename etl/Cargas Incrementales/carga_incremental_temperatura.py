from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

def carga_incremental_temperatura(temperatura_nuevo):
  #credenciales y cliente
  credentials = service_account.Credentials.from_service_account_file('tidy-hold-359719-964ba4f0d1d8.json') #credenciales
  project_id = 'tidy-hold-359719' #nombre del proyecto
  client = bigquery.Client(credentials= credentials,project=project_id)
  # imprimo valores actuales de la tabla a actualizar
  table_id = "tidy-hold-359719.comision_latinoamericana.temperatura"
  table = client.get_table(table_id)  # Make an API request.
  print("La tabla tenia originalmente {} filas y {} columnas".format(table.num_rows, len(table.schema)))

  #cargo tabla a actualizar
  q_temperatura_viejo="""SELECT * FROM `tidy-hold-359719.comision_latinoamericana.temperatura`"""   #query para tabla NDC
  temperatura_viejo = client.query(q_temperatura_viejo).to_dataframe() #dataframe de NDC

  #tabla vacia con la estructura de la tabla a actualizar, aca se cargan las filas nuevas
  filas_a_cargar = temperatura_viejo.truncate(after=-1)
  
  nuevo = temperatura_nuevo[['Pais','Cod_Pais']].values.tolist() #Cod_Pais y Pais de la tabla con valores nuevos
  viejo = temperatura_viejo[['Pais','Cod_Pais']].values.tolist() #Cod_Pais y Pais de la tabla a actualizar

  #si el conjunto Cod_Pais pais de la tabla nueva no esta en la tabla vieja, anexa la fila a la tabla a cargar
  for i in nuevo:
    if i not in viejo:
      filas_a_cargar=filas_a_cargar.append(temperatura_nuevo[(temperatura_nuevo.Pais==i[0]) & (temperatura_nuevo.Cod_Pais==i[1])])
  

  #configuracion de la carga
  job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
  job = client.load_table_from_dataframe(filas_a_cargar, table_id, job_config=job_config)  
  # Make an API request.
  job.result()  # Wait for the job to complete.
  table = client.get_table(table_id)  # Make an API request.
  print("Ahora tiene {} filas".format(table.num_rows))