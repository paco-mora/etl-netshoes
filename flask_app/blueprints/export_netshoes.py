from flask import Blueprint, request, jsonify, send_from_directory
import pandas as pd
import json

from conf import NETSHOES_EXPORT_FOLDER, DB_NAME, DB_ADDRESS, DB_PASSWORD, DB_USER


export_netshoes_blueprint = Blueprint('export', __name__)

@export_netshoes_blueprint.route('/price-sheet', methods = ['GET', 'POST'])
def get_json():
    try:
        data = request.json
        user_id = data['user_id']
        data_parsed = get_data(data)
    except:
        return 'JSON Inválido!'
    df = pd.DataFrame(data=data_parsed)
    file = 'price_sheet_' + str(user_id) + '.xlsx'
    df.to_excel(NETSHOES_EXPORT_FOLDER + file, columns=['SkuSeller', 'PrecoDe', 'PrecoPor'], index = False)
    return send_from_directory(NETSHOES_EXPORT_FOLDER, file, as_attachment=True)

@export_netshoes_blueprint.route('/default/file-name/<file_name>/format/<format>/user/<user_id>', methods = ['GET', 'POST'])
def get_export_default(file_name, format, user_id):
    data = request.json
    df = pd.json_normalize(data)
    file = file_name + str(user_id) + '.' + format
    if(format == 'csv'):
        df.to_csv(NETSHOES_EXPORT_FOLDER + file, index = False)
    elif(format == 'xlsx' or format == 'xls'):
        df.to_excel(NETSHOES_EXPORT_FOLDER + file, index = False)
    return send_from_directory(NETSHOES_EXPORT_FOLDER, file, as_attachment=True)


def get_data(data):
    sku_seller = []
    preco_de = []
    preco_por = []
    for d in data['products']:
        sku_seller.append(d['skuNetshoes'])
        preco_de.append(d['newPrice'])
        preco_por.append(d['newPrice'])
    return {'SkuSeller': sku_seller,
           'PrecoDe': preco_de,
           'PrecoPor': preco_por}
