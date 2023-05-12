from sqlalchemy import create_engine
import requests
import pandas as pd

DB_USER = 'hooklab_user'
DB_PASSWORD = 's9xzjib7so0jiiih'
DB_ADDRESS = 'db-postgresql-nyc1-hooklab-do-user-6943115-0.db.ondigitalocean.com'
DB_NAME = 'hooklab_db'
DB_PORT = '25060'


def refresh_optimized_products(user_id, user_name):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:25060/{db}"
    .format(user= DB_USER,
    pw=DB_PASSWORD,
    db=DB_NAME,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)  
    with engine.begin() as cnx:
        update_sql = f"insert into hooklab_crawler.optimized_products_2 (user_id, sku_netshoes, marketplace, old_seller, suggested_price, old_sale_price, current_sale_price, last_update) (select {user_id} as user_id, cp.fk_sku_netshoes as sku_netshoes, cp.marketplace as marketplace, hp.seller as old_seller, (hp.sale_price - 0.1) as suggested_price, hp.sale_price as old_sale_price, cp.seller_price as current_sale_price, now()::date as last_update from hooklab_crawler.customer_products as cp inner join (select h.sku_netshoes as sku_netshoes, h.marketplace as marketplace, h.sale_price as sale_price, h.seller as seller, h.date_historic as date_historic from (select sku_netshoes, marketplace, max(date_historic) as date_historic from hooklab_crawler.historic_products where lower(seller) != lower('{user_name}') group by sku_netshoes, marketplace) as sub_h inner join hooklab_crawler.historic_products as h on sub_h.sku_netshoes = h.sku_netshoes and h.marketplace = sub_h.marketplace and h.date_historic = sub_h.date_historic) as hp on cp.fk_sku_netshoes = hp.sku_netshoes and cp.marketplace = hp.marketplace inner join hooklab_crawler.products as p on p.pk_sku_netshoes = cp.fk_sku_netshoes and cp.marketplace = p.marketplace where cp.fk_user = {user_id} and cp.seller_price < hp.sale_price and cp.stock > 0 and (hp.sale_price - cp.seller_price) > 1 and lower(p.seller) = lower('{user_name}') and p.is_available = true ) on conflict (user_id, sku_netshoes, marketplace) DO update set old_seller = excluded.old_seller, suggested_price = excluded.suggested_price, old_sale_price = excluded.old_sale_price, last_update = excluded.last_update"
        cnx.execute(update_sql)
    engine.dispose()
    return '200'
    
def get_hooklab_users():
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:25060/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    df = pd.read_sql("select pk_id as user_id, user_name from hooklab_crawler.hooklab_users where active=True", engine)
    engine.dispose()
    return df
    
def check_optimize_products(user_id):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:25060/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    with engine.begin() as cnx:
        update_sql = f'update hooklab_crawler.hooklab_users set last_optimized_products = now() where pk_id = {user_id}'
        cnx.execute(update_sql)
    engine.dispose()
    return '200'
    
df = get_hooklab_users()

for user_id, user_name in df.values:
    refresh_optimized_products(user_id, user_name)
    check_optimize_products(user_id)
    print(user_id, ' - ', user_name)