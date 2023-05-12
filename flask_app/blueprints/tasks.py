from flask import Blueprint
from sqlalchemy import create_engine
import requests
from conf import DB_NAME, DB_ADDRESS, DB_PASSWORD, DB_USER

tasks_blueprint = Blueprint('tasks', __name__)


@tasks_blueprint.route('/netshoes/save-historic', methods=['GET'])
def save_historic():
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:25060/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    with engine.begin() as cnx:
        update_sql = 'insert into hooklab_crawler.historic_customer_products (date_historic, user_id, sku_netshoes, marketplace, cost_price, seller_price, sale_price, min_markup, stock, seller, last_update_stock, last_update_scrap, insert_date, is_available, brand, category) select now() as date_historic, cp.fk_user as user_id, p.pk_sku_netshoes as sku_netshoes, p.marketplace as marketplace, cp.cost_price as cost_price, cp.seller_price as seller_price, p.sale_price as sale_price, cp.min_markup as min_markup, cp.stock as stock, p.seller as seller, cp.last_update_stock as last_update_stock, p.last_update_scrap as last_update_scrap, now() as insert_date, p.is_available as is_available, brand, category from hooklab_crawler.products as p inner join hooklab_crawler.customer_products as cp on cp.fk_sku_netshoes = p.pk_sku_netshoes and cp.marketplace = p.marketplace on conflict (user_id, sku_netshoes, marketplace, insert_date) do nothing'
        cnx.execute(update_sql)
    engine.dispose()
    return '200'


@tasks_blueprint.route('/calculator/<skus>/<reajuste>/<desconto>', methods = ['GET'])
def get_price_simulator(skus, reajuste, desconto):
    skus_final = int(skus)
    price = 0
    p = [0.17, 0.12, 0.05, 0.02]
    if(int(skus_final) > 1000):
        for i in p:
            if(skus_final > 0 and skus_final >= 1000):
                price += 1000 * i
                skus_final -= 1000
            elif(skus_final > 0 and skus_final < 1000):
                price += skus_final * i
                skus_final -= skus_final
            else:
                pass
        if(skus_final > 0):
            price += skus_final * 0.01
    else:
        price = skus_final * 0.17
    if reajuste == 'true ':
        price = price * 1.0452
    price = price * (1 - float(desconto))
    return str(price)

@tasks_blueprint.route('/get-price/<skus>/<plan>/<desconto>', methods = ['GET'])
def get_price_simulator_new(skus, plan, desconto):
    skus = int(skus)
    desconto = float(desconto)
    if(skus >= 0 and skus <= 400):
        price = 89
    elif(skus >= 401 and skus <= 700):
        price = 109
    elif(skus >= 701 and skus <= 1000):
        price = 129
    elif(skus >= 1001 and skus <= 1300):
        price = 149
    elif(skus >= 1301 and skus <= 1600):
        price = 169
    elif(skus >= 1601 and skus <= 1900):
        price = 189
    elif(skus >= 1901 and skus <= 2200):
        price = 209
    elif(skus >= 2201 and skus <= 2500):
        price = 229
    elif(skus >= 2501 and skus <= 2800):
        price = 249
    elif(skus >= 2801 and skus <= 3100):
        price = 269
    elif(skus >= 3101 and skus <= 3400):
        price = 289
    elif(skus >= 3401 and skus <= 3700):
        price = 309
    elif(skus >= 3701 and skus <= 4000):
        price = 329
    elif(skus > 4000):
        price = 349
    if(plan == '2'):
        price += 70
    if(desconto is not None):
        price = price * (1 - (desconto))
    return {'price' : float(price)}

@tasks_blueprint.route('/get-price-old/<skus>/<plan>/<desconto>', methods = ['GET'])
def get_price_simulator_new(skus, plan, desconto):
    skus = int(skus)
    desconto = float(desconto)
    if(skus >= 0 and skus <= 400):
        price = 89
    elif(skus >= 401 and skus <= 700):
        price = 109
    elif(skus >= 701 and skus <= 1000):
        price = 129
    elif(skus >= 1001 and skus <= 1300):
        price = 149
    elif(skus >= 1301 and skus <= 1600):
        price = 169
    elif(skus >= 1601 and skus <= 1900):
        price = 189
    elif(skus >= 1901 and skus <= 2200):
        price = 209
    elif(skus >= 2201 and skus <= 2500):
        price = 229
    elif(skus >= 2501 and skus <= 2800):
        price = 249
    elif(skus >= 2801 and skus <= 3100):
        price = 269
    elif(skus >= 3101 and skus <= 3400):
        price = 289
    elif(skus >= 3401 and skus <= 3700):
        price = 309
    elif(skus >= 3701 and skus <= 4000):
        price = 329
    elif(skus > 4000):
        price = 349
    if(plan == '2'):
        price += 70
    if(desconto is not None):
        price = price * (1 - (desconto))
    return {'price' : float(price)}

@tasks_blueprint.route('/netshoes/update/title-indisponivel', methods=['GET'])
def update_title_indisponivel():
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:25060/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    with engine.begin() as cnx:
        update_sql = "update hooklab_crawler.products set is_available = false where title = 'Indisponível'"
        cnx.execute(update_sql)
    engine.dispose()
    return '200'

@tasks_blueprint.route('/netshoes/update/stock-null', methods=['GET'])
def update_stock_null():
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:25060/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    with engine.begin() as cnx:
        update_sql = "update hooklab_crawler.customer_products set stock = 1, last_update_stock = null where stock is null"
        cnx.execute(update_sql)
    engine.dispose()
    return '200'

@tasks_blueprint.route('/netshoes/update/is_available-false', methods=['GET'])
def update_available_false():
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:25060/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    with engine.begin() as cnx:
        update_sql = "update hooklab_crawler.products set is_available = true, last_update_scrap = null where is_available = false and title != 'Indisponível'"
        cnx.execute(update_sql)
    engine.dispose()
    return '200'
