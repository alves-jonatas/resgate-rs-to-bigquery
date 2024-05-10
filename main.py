import gspread
import numpy as np
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account


spreadsheet_id = '1oDK1D8JLNyNGbQ7o_jwR6PjN3ZDikYA5p7BQK5Owvb0'
project_id = 'resgaters'
dataset_id = 'resgaters_dataset'
table_id = 'resgaters_table'

#def load_sheet_to_bigquery(request):
# Verifica se a solicitação contém os parâmetros necessários
#request_json = request.get_json(silent=True)
#if not request_json:
#    return 'Erro: JSON inválido na solicitação.', 400
# spreadsheet_id = request_json.get('spreadsheet_id')
# project_id = request_json.get('project_id')
# dataset_id = request_json.get('dataset_id')
# table_id = request_json.get('table_id')

# Autenticação do Google Sheets
# scope_sheets = ['https://spreadsheets.google.com/feeds',
#                 'https://www.googleapis.com/auth/drive']
# credentials_sheets = service_account.Credentials.from_service_account_info(
#     request_json['credentials_sheets'], scopes=scope_sheets)
# client_sheets = gspread.authorize(credentials_sheets)
client_sheets = gspread.service_account('credentials.json')

# Abrindo a planilha
spreadsheet = client_sheets.open_by_key(spreadsheet_id)
sheet = spreadsheet.get_worksheet(0)

# Lendo os dados da planilha e criando um DataFrame
data = sheet.get_all_values()
columns = [
    "carimbo_data_hora",
    "call_to_action",
    "endereco_completo",
    "bairro",
    "cidade",
    "link_google_maps",
    "qtd_adultos",
    "qtd_criancas",
    "qtd_bebes",
    "qtd_idosos",
    "qtd_animais",
    "total_pessoas",
    "horario_verificado",
    "ultima_observacao",
    "verificado_por",
    "situacao",
    "telefone_1",
    "telefone_2",
    "telefone_3",
    "telefone_4",
    "telefone_5",
    "telefone_6",
    "observacoes_gerais",
    "latitude",
    "longitude"
]
# count = 0
# for column in data[1]:
#     if column and isinstance(column, str):
#         new_column = column.replace('/', '-')
#         columns.append(column)
#     else:
#         columns.append(f'col {count}')
#         count += 1
content = np.array(data[2:])
df = pd.DataFrame(content[:, :len(columns)], columns=columns)

# Convertendo para inteiro
quantidades = [
    "qtd_adultos",
    "qtd_criancas",
    "qtd_bebes",
    "qtd_idosos",
    "qtd_animais",
    "total_pessoas"]
df[quantidades] = df[quantidades].replace('', '0')
df[quantidades] = df[quantidades].astype(int)


# Carregando o DataFrame para o BigQuery
# credentials_bq = service_account.Credentials.from_service_account_info(
#     request_json['credentials_bq'])
# project_id = request_json['project_id']
credentials = service_account.Credentials.from_service_account_file('credentials.json')

pandas_gbq.to_gbq(
    df,
    destination_table=f"{dataset_id}.{table_id}",
    project_id=project_id,
    if_exists='replace',
    credentials=credentials
)  # Substitui a tabela se já existir

#return 'Dados carregados com sucesso para o BigQuery!'
