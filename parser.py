import requests
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import datetime
import time

def get_token(): #Получаем токен для запросов
    url = 'https://api.remonline.ru/token/new?api_key='
    change_token = requests.get(url)
    change_token = change_token.json
    return change_token()['token']

def get_order(num_page): #Получаем данные по заказам
    token = get_token()
    url = 'https://api.remonline.ru/order/'+'?token='+ token + '&created_at[]=1609448400000' + '&page=' + num_page
    order = requests.get(url)
    order = order.json
    return order()

data_order = {     #Шаблон заголовков и данных
        "id": 1,
        "brand": "",
        "model": "Sony Ericsson K800i",
        "price": 1700,
        "payed": 1200,
        "resume": "",
        "urgent": 'false',
        "serial": "356128022598709",
        "client": {
            "id": 142,
            "phone": [
                "+7 (947) 294-82-93"
            ],
            "address": "г. Город, ул. Улица д.12, кв.34",
            "name": "Jack London",
            "email": "",
            "modified_at": 1454278600000,
            "notes": "Платит вовремя.",
            "supplier": 'false',
            "juridical": 'false',
            "conflicted": 'true',
            "discount_code": "2900000000018",
            "discount_goods": 0,
            "discount_services": 5,
            "discount_materials": 25,
            "custom_fields": {
                "1": "Some custom value"
            },
            "ad_campaign": {
                "id": 1,
                "name": "Internet"
            }
        },
        "ad_campaign": {
            "id": 1,
            "name": "Internet"
        },
        "status": {
            "id": 828,
            "name": "New",
            "group": 1,
            "color": "#999999"
        },
        "done_at": 1456137000000,
        "overdue": 'false',
        "engineer_id": 1,
        "manager_id": 1,
        "branch_id": 218,
        "appearance": "Scratches, abrasions",
        "created_by_id": 1,
        "order_type": {
            "id": 1,
            "name": "VIP"
        },
        "parts": [
            {
                "id": 1,
                "engineer_id": 9130,
                "title": "Display",
                "cost": 100,
                "price": 150,
                "discount_value": 25,
                "amount": 1,
                "warranty": 6,
                "warranty_period": 1
            }
        ],
        "operations": [
            {
                "id": 1,
                "engineer_id": 9130,
                "title": "Diagnostics",
                "cost": 20,
                "price": 25,
                "discount_value": 5,
                "amount": 1,
                "warranty": 1,
                "warranty_period": 1
            },
            {
                "id": 2,
                "engineer_id": 9130,
                "title": "Work",
                "cost": 200,
                "price": 222,
                "discount_value": 3,
                "amount": 2,
                "warranty": 7,
                "warranty_period": 0
            }
        ],
        "attachments": [
            {
                "created_by_id": 11,
                "created_at": 1521040338000,
                "url": "/documents/download/6729cff9b6c8401aae544c2c1006f296",
                "filename": "file.pdf"
            },
            {
                "created_by_id": 13,
                "created_at": 1521031974000,
                "url": "/documents/download/fe61c761fca74786936306cdb017187d",
                "filename": "order-1237701.kud"
            }
        ],
        "created_at": 1456132000000,
        "scheduled_for": 'null',
        "closed_at": 1456137000000,
        "modified_at": 1456137000000,
        "packagelist": "",
        "kindof_good": "Smartphone",
        "malfunction": "Broken display",
        "id_label": "W1",
        "closed_by_id": 1,
        "custom_fields": {
            "1": "Some custom value"
        },
        "warranty_date": 1459137000000,
        "manager_notes": "",
        "estimated_cost": 1700,
        "engineer_notes": "",
        "warranty_granted": 'true',
        "estimated_done_at": 1456136000000

}
# Создаем пустые списки, для заголовков
main_head = []
client = []
ad_campaign = []
status = []
order_type = []
parts = []
operations = []
attachments = []
custom_fields = []

for key, value in data_order.items(): #Записываем эталонные заголовки столбцов
    if key == 'client':
        client.append('global_id')
        for key, value in data_order['client'].items():
            client.append(key)
    elif key == 'ad_campaign':
        ad_campaign.append('global_id')
        for key, value in data_order['ad_campaign'].items():
            ad_campaign.append(key)
    elif key == 'status':
        status.append('global_id')
        for key, value in data_order['status'].items():
            status.append(key)
    elif key == 'order_type':
        order_type.append('global_id')
        for key, value in data_order['order_type'].items():
            order_type.append(key)
    elif key == 'parts':
        parts.append('global_id')
        for key, value in data_order['parts'][0].items():
            parts.append(key)
    elif key == 'operations':
        operations.append('operations')
        for key, value in data_order['operations'][0].items():
            operations.append(key)
    elif key == 'attachments':
        attachments.append('global_id')
        for key, value in data_order['attachments'][0].items():
            attachments.append(key)
    elif key == 'custom_fields':
        custom_fields.append('global_id')
        for key, value in data_order['custom_fields'].items():
            custom_fields.append(key)
    else:
        main_head.append(key)



CREDENTIALS_FILE = 'test-1988-314310-2b0d5f660dbd.json'  # Имя файла с закрытым ключом

# Читаем ключи из файла
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])

httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API
def create_doc():
    spreadsheet = service.spreadsheets().create(body = {
        'properties': {'title': 'remonline.ru', 'locale': 'ru_RU'},
        'sheets': [{'properties': {'sheetType': 'GRID',
                                   'sheetId': 0,
                                   'title': 'main_head',
                                   'gridProperties': {'rowCount': 1000, 'columnCount': 1000}}}]
    }).execute()
    spreadsheetId = spreadsheet['spreadsheetId'] # сохраняем идентификатор файла
    return spreadsheetId

spreadsheetId = '1O3uQMWHErMatTlOx_M4Tlm7aWiNONd8YqDHsgG4dKik'

def creat_list(name): # Создаем вкладки исходя из структуры данных по заказам
    results = service.spreadsheets().batchUpdate(
        spreadsheetId = spreadsheetId,
        body =
    {
      "requests": [
        {
          "addSheet": {
            "properties": {
              "title": name,
              "gridProperties": {
                "rowCount": 100,
                "columnCount": 100
              }
            }
          }
        }
      ]
    }).execute()
    return

def root(): # Добавление прав на просмотр/редактирование созданного файла
    driveService = apiclient.discovery.build('drive', 'v3', http = httpAuth) # Выбираем работу с Google Drive и 3 версию API
    access = driveService.permissions().create(
        fileId = spreadsheetId,
        body = {'type': 'user', 'role': 'writer', 'emailAddress': 'capply@gmail.com'},  # Открываем доступ на редактирование
        fields = 'id'
    ).execute()
    return

def in_table(name, data): #Функция добавления строк в ГУГЛ ДОК
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open_by_key(spreadsheetId)
    worksheet = sh.worksheet(name)
    worksheet.append_row(data)

def change_date(dtime):
    timestamp  = int(dtime)
    value = datetime.datetime.fromtimestamp(timestamp/1000)
    new_date = value.strftime('%d.%m.%Y')
    return new_date



in_table('main_head', main_head) #Записываем заголовки на главную таблицу

creat_list('clients') #Вызываем функции по созданию вкладок
in_table('clients', client) #Записываем заголовки в таблицу на созданную вкладку

creat_list('ad_campaign') #Вызываем функции по созданию вкладок
in_table('ad_campaign', ad_campaign)#Записываем заголовки в таблицу на созданную вкладку

creat_list('status') #Вызываем функции по созданию вкладок
in_table('status', status)#Записываем заголовки в таблицу на созданную вкладку

creat_list('order_type') #Вызываем функции по созданию вкладок
in_table('order_type', order_type)#Записываем заголовки в таблицу на созданную вкладку

creat_list('parts')  #Вызываем функции по созданию вкладок
in_table('parts', parts)#Записываем заголовки в таблицу на созданную вкладку

creat_list('operations') #Вызываем функции по созданию вкладок
in_table('operations', operations)#Записываем заголовки в таблицу на созданную вкладку

creat_list('attachments') #Вызываем функции по созданию вкладок
in_table('attachments', attachments)#Записываем заголовки в таблицу на созданную вкладку

creat_list('custom_fields') #Вызываем функции по созданию вкладок
in_table('custom_fields', custom_fields)#Записываем заголовки в таблицу на созданную вкладку


dat_str = ['modified_at', 'done_at', 'created_at', 'closed_at', 'warranty_date', 'estimated_done_at']
for page in range(1,1000): #Перебираем страницы с пагинацией

    if (get_order(str(page))['data']): #Проверяем конец пагинации
        for int_n in range(len(get_order(str(page))['data'])): #Добавляем данные в таблицу
            dict_n = get_order(str(page))['data'][int_n]
            main_head_n = []
            client_n = [dict_n['id']]
            ad_campaign_n = [dict_n['id']]
            status_n = [dict_n['id']]
            order_type_n = [dict_n['id']]
            parts_n = [dict_n['id']]
            operations_n = [dict_n['id']]
            attachments_n = [dict_n['id']]
            custom_fields_n = [dict_n['id']]

            for i in main_head:
                if i in dat_str:
                    if i in dict_n:
                        main_head_n.append(change_date(dict_n[i]))
                    else:
                        main_head_n.append('')
                else:
                    if i in dict_n:
                        main_head_n.append(dict_n[i])
                    else:
                        main_head_n.append('')
            in_table('main_head', main_head_n)

            for i in client[1:]:#Перебираем заголовки таблицы
                if i in dat_str: #Проверияем, заголовок на тип даты
                    if 'client' in dict_n:
                        if i in dict_n['client']:
                            client_n.append(change_date(str(dict_n['client'][i])))
                        else:
                            client_n.append('')
                else:
                    if 'client' in dict_n:
                        if i in dict_n['client']:
                            client_n.append(str(dict_n['client'][i]))
                        else:
                            client_n.append('')
            in_table('clients', client_n)
        
            for i in ad_campaign[1:]:#Перебираем заголовки таблицы
                if i in dat_str:
                    if 'ad_campaign' in dict_n:
                        if i in dict_n['ad_campaign']:
                            ad_campaign_n.append(change_date(str(dict_n['ad_campaign'][i])))
                        else:
                            ad_campaign_n.append('')
                else:
                    if 'ad_campaign' in dict_n:
                        if i in dict_n['ad_campaign']:
                            ad_campaign_n.append(str(dict_n['ad_campaign'][i]))
                        else:
                            ad_campaign_n.append('')
            in_table('ad_campaign', ad_campaign_n)

            for i in status[1:]:
                if i in dat_str:
                    if 'status' in dict_n:
                        if i in dict_n['status']:
                            status_n.append(change_date(str(dict_n['status'][i])))
                        else:
                            status_n.append('')
                else:
                    if 'status' in dict_n:
                        if i in dict_n['status']:
                            status_n.append(str(dict_n['status'][i]))
                        else:
                            status_n.append('')
            in_table('status', status_n)
        
            for i in order_type[1:]:
                if i in dat_str:
                    if 'order_type' in dict_n:
                        if i in dict_n['order_type']:
                            order_type_n.append(change_date(str(dict_n['order_type'][i])))
                        else:
                            order_type.append('')
                else:
                    if 'order_type' in dict_n:
                        if i in dict_n['order_type']:
                            order_type_n.append(str(dict_n['order_type'][i]))
                        else:
                            order_type.append('')
            in_table('order_type', order_type_n)
        
            if len(dict_n['parts']) > 0:
                for m in range(len(dict_n['parts'])):
                    parts_n = []
                    parts_n = [dict_n['id']]
                    for i in parts[1:]:
                        if i in dat_str:
                            if 'parts' in dict_n:
                                if i in dict_n['parts'][m]:
                                    parts_n.append(change_date(str(dict_n['parts'][m][i])))
                                else:
                                    parts_n.append('')
                        else:
                            if 'parts' in dict_n:
                                if i in dict_n['parts'][m]:
                                    parts_n.append(str(dict_n['parts'][m][i]))
                                else:
                                    parts_n.append('') 
                                    
                    in_table('parts', parts_n)
            else:
                in_table('parts', parts_n)
        
            if len(dict_n['operations']) > 0:
                for m in range(len(dict_n['operations'])):
                    operations_n = []
                    operations_n = [dict_n['id']]
                    for i in operations[1:]:
                        if i in dat_str:      
                            if 'operations' in dict_n:
                                if i in dict_n['operations'][m]:
                                    operations_n.append(change_date(str(dict_n['operations'][m][i])))
                                else:
                                    operations_n.append('')
                        else:
                            if 'operations' in dict_n:
                                if i in dict_n['operations'][m]:
                                    operations_n.append(str(dict_n['operations'][m][i]))
                                else:
                                    operations_n.append('')
                    in_table('operations', operations_n)
            else:
                in_table('operations', operations_n)
        
            if len(dict_n['attachments']) > 0:
                for m in range(len(dict_n['attachments'])):
                    attachments_n = []
                    attachments_n = [dict_n['id']]
                    for i in attachments[1:]:
                        if i in dat_str:
                            if 'attachments' in dict_n:
                                if i in dict_n['attachments'][m]:
                                    attachments_n.append(change_date(str(dict_n['attachments'][m][i])))
                                else:
                                    attachments_n.append('')
                        else:
                            if 'attachments' in dict_n:
                                if i in dict_n['attachments'][m]:
                                    attachments_n.append(str(dict_n['attachments'][m][i]))
                                else:
                                    attachments_n.append('')
                    in_table('attachments', attachments_n)
            else:
                in_table('attachments', attachments_n)
        
            if 'custom_fields' in dict_n:
                for key, value in dict_n['custom_fields'].items():
                    if value != '':
                        custom_fields_n.append(key)
                        custom_fields_n.append(value)
                in_table('custom_fields', custom_fields_n)





