from google.cloud import bigquery
from google.oauth2 import service_account

# Construct a BigQuery client object.
credentials = service_account.Credentials.from_service_account_file('tidy-hold-359719-f6808bf80ee8.json')
project_id = 'tidy-hold-359719'
client = bigquery.Client(credentials= credentials,project=project_id)



# tabla temperatura
table_id = "tidy-hold-359719.comision_latinoamericana.temperatura" #Controlar nombre de tabla
schema = [
    bigquery.SchemaField("anio", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("temperatura", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cod_pais", "FLOAT64", mode="NULLABLE"),
]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


# tabla datos_ONU
table_id = "tidy-hold-359719.comision_latinoamericana.datos_ONU"
schema = [
    bigquery.SchemaField("anio", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("pais", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("intensidad_energetica_medida_en_terminos_de_energia_primaria_y_PBI", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("proporcion_de_la_poblacion_con_acceso_a_elecricidad", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("proporcion_de_la_poblacion_con_dependencia_primaria_a_energias_limpias", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("proporcion_de_energias_renovables_del_total_consumido", "FLOAT", mode="NULLABLE"),
]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


# tabla energyco2
table_id = "tidy-hold-359719.comision_latinoamericana.energyco2"

schema = [
    bigquery.SchemaField("Anio", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Pais", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Emisiones_de_CO2", "FLOAT", mode="NULLABLE"),
]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


# tabla porcentaje_compromiso
table_id = "tidy-hold-359719.comision_latinoamericana.compromiso" #Controlar nombre de tabla
schema = [
    bigquery.SchemaField("pais", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cod_pais", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("compromiso", "FLOAT64", mode="NULLABLE"),
]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


print('** CARGA DE ESTRUCTURA COMPLETA**')