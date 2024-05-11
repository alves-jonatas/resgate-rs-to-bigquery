import functions_framework

import gspread
import numpy as np
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account


@functions_framework.http
def load_sheet_to_bigquery():
    # Verifica se a solicitação contém os parâmetros necessários

    spreadsheet_id = '10NEisQe8VihPFdZK6RokxtIt0GhIbzdKetrR8U0Gr1Y'
    project_id = 'resgaters'
    dataset_id = 'resgaters_dataset'
    table_id = 'resgaters_table_dev'

    # Autenticação do Google Sheets
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
        "apagar",
        "latitude",
        "longitude"
    ]
    content = np.array(data[2:])
    df = pd.DataFrame(content[:, :len(columns)], columns=columns)
    df = df.drop('apagar', axis=1)

    # Convertendo para inteiro
    quantidades = [
        "qtd_adultos",
        "qtd_criancas",
        "qtd_bebes",
        "qtd_idosos",
        "qtd_animais",
        "total_pessoas"]
    df[quantidades] = df[quantidades].apply(lambda col: pd.to_numeric(col, errors='coerce').astype('Int64'))

    # Carregando o DataFrame para o BigQuery
    credentials = service_account.Credentials.from_service_account_file('credentials.json')

    pandas_gbq.to_gbq(
        df,
        destination_table=f"{dataset_id}.{table_id}",
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )  # Substitui a tabela se já existir

    return 'Dados carregados com sucesso para o BigQuery!'


if __name__ == '__main__':
    load_sheet_to_bigquery()