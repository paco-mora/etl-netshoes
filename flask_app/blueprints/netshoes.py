from flask import Blueprint, request, jsonify
from modules import etl_netshoes as etl
from modules import database
import pandas as pd

etl_netshoes_blueprint = Blueprint('etl-netshoes', __name__)

@etl_netshoes_blueprint.route('/user/<user>/platform/moovin/estoque-netshoes/price/<price>')
def etl_moovin_estoque(user, price):
    user = str(user)
    try:
        exp = etl.Netshoes_Exportacao(user)
        preco = etl.Netshoes_Preco(user)
        moovin = etl.Moovin(user, estoque=False)
        estoque = etl.Netshoes_Estoque(user)
    except:
        error_charge(user, 'error on transformation')
        raise
    df = pd.merge(exp.get_dataframe(), preco.get_dataframe(), on=['sku_seller'], how='left')
    df = pd.merge(df, moovin.get_dataframe(), on=['sku_seller'], how='left')
    df = pd.merge(df, estoque.get_dataframe(), on=['sku_seller'], how='left')
    df['user_id'] = user
    df.drop(['sku_seller'], axis=1, inplace=True)
    df.rename({'sku_seller_bruto': 'sku_seller'}, axis= 1, inplace=True)
    database.df_to_db(df, 'hooklab_crawler', 'temp_'+user, if_exists='replace')
    if(price == 'true'):
        if(temp_to_db_price(user, True) == '200'):
            finish_charge_price(user)
        else:
            error_charge(user, 'error on transfer to database')
    else:
        if(temp_to_db(user, True) == '200'):
            finish_charge(user)
        else:
            error_charge(user, 'error on transfer to database')
    return '200'

@etl_netshoes_blueprint.route('/user/<user>/platform/moovin/price/<price>')
def etl_moovin(user, price):
    user = str(user)
    try:
        exp = etl.Netshoes_Exportacao(user)
        preco = etl.Netshoes_Preco(user)
        moovin = etl.Moovin(user, estoque=True)
    except:
        error_charge(user, 'error on transformation')
        raise
    df = pd.merge(exp.get_dataframe(), preco.get_dataframe(), on=['sku_seller'], how='left')
    df = pd.merge(df, moovin.get_dataframe(), on=['sku_seller'], how='left')
    df.drop(['sku_seller'], axis=1, inplace=True)
    df.rename({'sku_seller_bruto': 'sku_seller'}, axis= 1, inplace=True)
    df['user_id'] = user
    database.df_to_db(df, 'hooklab_crawler', 'temp_'+user, if_exists='replace')
    if(price == 'true'):
        if(temp_to_db_price(user, True) == '200'):
            finish_charge_price(user)
        else:
            error_charge(user, 'error on transfer to database')
    else:
        if(temp_to_db(user, True) == '200'):
            finish_charge(user)
        else:
            error_charge(user, 'error on transfer to database')
    return '200'

@etl_netshoes_blueprint.route('/user/<user>/platform/netshoes/price/<price>')
def etl_netshoes(user, price):
    user = str(user)
    try:
        exp = etl.Netshoes_Exportacao(user)
        preco = etl.Netshoes_Preco(user)
        estoque = etl.Netshoes_Estoque(user)
        df = pd.merge(exp.get_dataframe(), estoque.get_dataframe(), on=['sku_seller'], how="left")
        df = pd.merge(df,  preco.get_dataframe(), on=['sku_seller'], how='left')
    except:
        error_charge(user, 'error on transformation')
        raise
    df['user_id'] = user
    df.drop(['sku_seller'], axis=1, inplace=True)
    df.rename({'sku_seller_bruto': 'sku_seller'}, axis= 1, inplace=True)
    database.df_to_db(df, 'hooklab_crawler', 'temp_'+user, if_exists='replace')
    if(price == 'true'):
        if(temp_to_db_price(user, False) == '200'):
            finish_charge_price(user)
        else:
            error_charge(user, 'error on transfer to database')
    else:
        if(temp_to_db(user, False) == '200'):
            finish_charge(user)
        else:
            error_charge(user, 'error on transfer to database')
    return '200'

@etl_netshoes_blueprint.route('/new/user/<user>/platform/netshoes')
def etl_netshoes_new(user):
    print('iniciando')
    user = str(user)
    try:
        df = etl.Netshoes_Exportacao(user).get_dataframe()
    except:
        error_charge(user, 'error on transformation')
        raise
    df['user_id'] = user
    df.drop(['sku_seller'], axis=1, inplace=True)
    df.rename({'sku_seller_bruto': 'sku_seller'}, axis= 1, inplace=True)
    print(df.head())
    database.df_to_db(df, 'hooklab_crawler', 'temp_'+user, if_exists='replace')
    temp_db = temp_to_db_new(user)
    print('temp to db: ', temp_db)
    if(temp_db == '200'):
        finish_charge(user)
    else:
        error_charge(user, 'error on transfer to database')
    return '200'

@etl_netshoes_blueprint.route('/new/user/<user>/platform/netshoes-custo')
def etl_netshoes_custo(user):
    user = str(user)
    try:
        df_exp = etl.Netshoes_Exportacao(user)
        df_custo = etl.Template_Custo(user)
        df = pd.merge(df_exp.get_dataframe(), df_custo.get_dataframe(), on=['sku_seller'], how="left")
    except:
        error_charge(user, 'error on transformation')
        raise
    df['user_id'] = user
    df.drop(['sku_seller'], axis=1, inplace=True)
    df.rename({'sku_seller_bruto': 'sku_seller'}, axis= 1, inplace=True)
    print(df.head())
    database.df_to_db(df, 'hooklab_crawler', 'temp_'+user, if_exists='replace')
    if(temp_to_db_netshoes_custo(user) == '200'):
        finish_charge(user)
    else:
        error_charge(user, 'error on transfer to database')
    return '200'

@etl_netshoes_blueprint.route('/user/<user>/platform/anymarket/price/<price>')
def etl_anymarket(user, price):
    user = str(user)
    try:
        exp = etl.Netshoes_Exportacao(user)
        preco = etl.Netshoes_Preco(user)
        anymarket = etl.Anymarket(user)
    except:
        error_charge(user, 'error on transformation')
        raise
    df = pd.merge(exp.get_dataframe(), preco.get_dataframe(), on=['sku_seller'], how='left')
    df = pd.merge(df, anymarket.get_dataframe(), on=['sku_seller'], how='left')
    df['user_id'] = user
    database.df_to_db(df, 'hooklab_crawler', 'temp_'+user, if_exists='replace')
    if(price == 'true'):
        if(temp_to_db_price(user, True) == '200'):
            finish_charge_price(user)
        else:
            error_charge(user, 'error on transfer to database')
    else:
        if(temp_to_db(user, True) == '200'):
            finish_charge(user)
        else:
            error_charge(user, 'error on transfer to database')
    return '200'

@etl_netshoes_blueprint.route('/user/<user>/platform/shoppub/price/<price>')
def etl_shoppub(user, price):
    user = str(user)
    try:
        exp = etl.Netshoes_Exportacao(user)
        preco = etl.Netshoes_Preco(user)
        shoppub = etl.Shoppub(user)
    except:
        error_charge(user, 'error on transformation')
        raise
    df = pd.merge(exp.get_dataframe(), preco.get_dataframe(), on=['sku_seller'], how='left')
    df = pd.merge(df, shoppub.get_dataframe(), on=['sku_seller'], how='left')
    df['user_id'] = user
    database.df_to_db(df, 'hooklab_crawler', 'temp_'+user, if_exists='replace')
    if(price == 'true'):
        if(temp_to_db_price(user, True) == '200'):
            finish_charge_price(user)
        else:
            error_charge(user, 'error on transfer to database')
    else:
        if(temp_to_db(user, True) == '200'):
            finish_charge(user)
        else:
            error_charge(user, 'error on transfer to database')
    return '200'


def finish_charge(user):
    query = f'update hooklab_crawler.aux_carga_netshoes set ultima_carga = now(), nova_carga = false, erro = false where hooklab_user = {str(user)}'
    return database.db_query(query, (), 'update')

def finish_charge_price(user):
    query = f'update hooklab_crawler.aux_carga_netshoes set ultima_carga_preco = now() where hooklab_user = {str(user)}'
    return database.db_query(query, (), 'update')

def error_charge(user, log):
    query = 'update hooklab_crawler.aux_carga_netshoes set erro = true, log = %s where hooklab_user = %s'
    return database.db_query(query, (str(log), str(user)), 'update')

def temp_to_db(user, preco_custo):
    if(preco_custo == False):
        query_1 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'netshoes' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, preco as seller_price, null as cost_price, estoque as stock, current_timestamp as last_update_stock FROM hooklab_crawler.temp_{}) where sku_netshoes is not null ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET seller_price = excluded.seller_price, stock = excluded.stock, last_update_stock = excluded.last_update_stock, sku_seller = excluded.sku_seller".format(str(user))
        query_2 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'zattini' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, preco as seller_price, null as cost_price, estoque as stock, current_timestamp as last_update_stock FROM hooklab_crawler.temp_{}) where sku_netshoes is not null ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET seller_price = excluded.seller_price, stock = excluded.stock, last_update_stock = excluded.last_update_stock, sku_seller = excluded.sku_seller".format(str(user))
    elif(preco_custo == True):
        query_1 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock) (SELECT user_id::int as fk_user, 'netshoes' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, preco as seller_price, cost_price, estoque as stock,current_timestamp as last_update_stock FROM hooklab_crawler.temp_{}) where sku_netshoes is not null ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET seller_price = excluded.seller_price, stock = excluded.stock, last_update_stock = excluded.last_update_stock, sku_seller = excluded.sku_seller".format(str(user))
        query_2 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock) (SELECT user_id::int as fk_user, 'zattini' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, preco as seller_price, cost_price, estoque as stock,current_timestamp as last_update_stock FROM hooklab_crawler.temp_{}) where sku_netshoes is not null ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET seller_price = excluded.seller_price, stock = excluded.stock, last_update_stock = excluded.last_update_stock, sku_seller = excluded.sku_seller".format(str(user))
    query_3 =  "insert into hooklab_crawler.products (pk_sku_netshoes, marketplace, is_available, brand, category) (SELECT sku_netshoes as pk_sku_netshoes, 'zattini' as marketplace,  true, brand, category FROM hooklab_crawler.temp_{}) where sku_netshoes is not null ON CONFLICT (marketplace, pk_sku_netshoes) DO UPDATE SET brand = excluded.brand, category = excluded.category".format(str(user))
    query_4 =  "insert into hooklab_crawler.products (pk_sku_netshoes, marketplace, is_available, brand, category) (SELECT sku_netshoes as pk_sku_netshoes, 'netshoes' as marketplace,  true, brand, category FROM hooklab_crawler.temp_{}) where sku_netshoes is not null ON CONFLICT (marketplace, pk_sku_netshoes) DO UPDATE SET brand = excluded.brand, category = excluded.category".format(str(user))
    database.db_query(query_1, (), 'insert')
    database.db_query(query_2, (), 'insert')
    database.db_query(query_3, (), 'insert')
    database.db_query(query_4, (), 'insert')
    return '200'

def temp_to_db_new(user):
    query_1 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'netshoes' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, null as seller_price, null as cost_price, null as stock, null as last_update_stock FROM hooklab_crawler.temp_{}) ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET sku_seller = excluded.sku_seller".format(str(user))
    query_2 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'zattini' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, null as seller_price, null as cost_price, null as stock, null as last_update_stock FROM hooklab_crawler.temp_{}) ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET sku_seller = excluded.sku_seller".format(str(user))
    query_3 =  "insert into hooklab_crawler.products (pk_sku_netshoes, marketplace, is_available, brand, category, title) (SELECT sku_netshoes as pk_sku_netshoes, 'zattini' as marketplace,  true, brand, category, title FROM hooklab_crawler.temp_{}) ON CONFLICT (marketplace, pk_sku_netshoes) DO UPDATE SET brand = excluded.brand, category = excluded.category".format(str(user))
    query_4 =  "insert into hooklab_crawler.products (pk_sku_netshoes, marketplace, is_available, brand, category, title) (SELECT sku_netshoes as pk_sku_netshoes, 'netshoes' as marketplace,  true, brand, category, title FROM hooklab_crawler.temp_{}) ON CONFLICT (marketplace, pk_sku_netshoes) DO UPDATE SET brand = excluded.brand, category = excluded.category".format(str(user))
    database.db_query(query_1, (), 'insert')
    database.db_query(query_2, (), 'insert')
    database.db_query(query_3, (), 'insert')
    database.db_query(query_4, (), 'insert')
    return '200'

def temp_to_db_netshoes_custo(user):
    query_1 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'netshoes' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, null as seller_price, preco_custo::decimal as cost_price, null as stock, null as last_update_stock FROM hooklab_crawler.temp_{}) ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET sku_seller = excluded.sku_seller, cost_price = excluded.cost_price".format(str(user))
    query_2 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'zattini' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, null as seller_price, preco_custo::decimal as cost_price, null as stock, null as last_update_stock FROM hooklab_crawler.temp_{}) ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET sku_seller = excluded.sku_seller, cost_price = excluded.cost_price".format(str(user))
    query_3 =  "insert into hooklab_crawler.products (pk_sku_netshoes, marketplace, is_available, brand, category, title) (SELECT sku_netshoes as pk_sku_netshoes, 'zattini' as marketplace,  true, brand, category, title FROM hooklab_crawler.temp_{}) ON CONFLICT (marketplace, pk_sku_netshoes) DO UPDATE SET brand = excluded.brand, category = excluded.category".format(str(user))
    query_4 =  "insert into hooklab_crawler.products (pk_sku_netshoes, marketplace, is_available, brand, category, title) (SELECT sku_netshoes as pk_sku_netshoes, 'netshoes' as marketplace,  true, brand, category, title FROM hooklab_crawler.temp_{}) ON CONFLICT (marketplace, pk_sku_netshoes) DO UPDATE SET brand = excluded.brand, category = excluded.category".format(str(user))
    database.db_query(query_1, (), 'insert')
    database.db_query(query_2, (), 'insert')
    database.db_query(query_3, (), 'insert')
    database.db_query(query_4, (), 'insert')
    return '200'

def temp_to_db_price(user, preco_custo):
    query_1 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'netshoes' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, preco as seller_price, null as cost_price, estoque as stock, current_timestamp as last_update_stock FROM hooklab_crawler.temp_{}) ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET seller_price = excluded.seller_price".format(str(user))
    query_2 = "insert into hooklab_crawler.customer_products (fk_user, marketplace, fk_sku_netshoes, sku_seller, seller_price, cost_price, stock, last_update_stock ) (SELECT user_id::int as fk_user, 'zattini' as marketplace, sku_netshoes as fk_sku_netshoes, sku_seller, preco as seller_price, null as cost_price, estoque as stock, current_timestamp as last_update_stock FROM hooklab_crawler.temp_{}) ON CONFLICT (fk_user, marketplace, fk_sku_netshoes) DO UPDATE SET seller_price = excluded.seller_price".format(str(user))
    database.db_query(query_1, (), 'insert')
    database.db_query(query_2, (), 'insert')
    return '200'
