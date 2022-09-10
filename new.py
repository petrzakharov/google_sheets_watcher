import os

import httplib2
import apiclient.discovery
import pandas as pd
from apiclient import errors
from oauth2client.service_account import ServiceAccountCredentials
import logging
from dotenv import load_dotenv
import time
import requests
from sqlalchemy import create_engine, text

load_dotenv()

CREDENTIALS_FILE = 'credentials.json'
SPREADSHEET_ID = '1vdV_-_-Oz_Mwr0FnNZJ7A9VAU2CPtD0VxMY9CmIrhvc'
EXCHANGE_API_URL = 'https://api.exchangerate.host/latest'

ERROR_MESSAGES = {
    'exchange': 'Ошибка при обновлении курса валюты',
    'sheets': 'Ошибка при получении файла Google Sheets',
    'drive': 'Ошибка при получении modified_data из Google Drive',
    'file': 'Файл c указанным id "{id}" найден',
    'file_without_data': 'Некорректный ответ от Google Drive'
}

SUCCESS_MESSAGES = {
    'db': 'Файл успешно записан в БД',
}

INFO_MESSAGES = {
    'new_data': 'Таблица в GS была обновлена, приступаем к синхронизации',
}

CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])

http_auth = CREDENTIALS.authorize(httplib2.Http())


def get_time_modified():
    drive_service = apiclient.discovery.build('drive', 'v3', http=http_auth)
    try:
        results = drive_service.files().list(
            pageSize=1,
            fields="nextPageToken, files(id, name, modifiedTime)").execute()
        items = results.get('files', [])
        new_time_modified = [file['modifiedTime'] for file in items if file['id'] == SPREADSHEET_ID][0]
        return new_time_modified
    except IndexError:
        raise IndexError(
            ERROR_MESSAGES['file'].format(
                id=SPREADSHEET_ID
            ),
        )
    except errors.HttpError:
        raise ConnectionError(
            ERROR_MESSAGES['drive']
        )


def get_usd_rate():
    response = requests.get(EXCHANGE_API_URL, {'symbols': 'RUB', 'base': 'USD'})
    if response.status_code != 200:
        raise ConnectionError(
            ERROR_MESSAGES['exchange']
        )
    return response.json()['rates']['RUB']


def get_and_prepare_file():
    sheets_service = apiclient.discovery.build('sheets', 'v4', http=http_auth)
    try:
        values = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range='A1:D1000000',
            majorDimension='COLUMNS'
        ).execute()
    except errors.HttpError:
        raise ConnectionError(
            ERROR_MESSAGES['sheets']
        )
    if 'values' not in values:
        raise RuntimeError(
            ERROR_MESSAGES['file_without_data']
        )
    table = [i[1:] for i in values['values']]
    headers = [i[0] for i in values['values']]
    df = pd.DataFrame(table).T
    df.columns = headers
    df[['num', 'order_number', 'price_usd']] = df[['num', 'order_number', 'price_usd']].apply(pd.to_numeric)
    df['delivery_date'] = pd.to_datetime(df['delivery_date'], format='%d.%m.%Y')
    df['price_rub'] = round(get_usd_rate() * df['price_usd'], 2)
    return df


def upload_file(file):
    connection = 'postgresql://{username}:{password}@{host}:{port}/{database}'.format(
            username=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            host=os.getenv('DB_HOST')
        )
    engine = create_engine(connection)
    file.to_sql('test_table', engine, if_exists='replace')
    logging.info(SUCCESS_MESSAGES['db'])


def main():
    last_time_modified = None
    while True:
        try:
            new_time_modified = get_time_modified()
            if not last_time_modified or last_time_modified < new_time_modified:
                logging.info(INFO_MESSAGES['new_data'])
                file = get_and_prepare_file()
                upload_file(file)
                last_time_modified = new_time_modified
            time.sleep(15)
        except Exception as exception:
            logging.error(exception)
            time.sleep(30)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        filename=__file__.split('py')[0] + 'log',
        format='%(asctime)s:%(levelname)s:%(funcName)s %(message)s',
        filemode='w'
    )
    main()
