from sqlalchemy import create_engine
import requests
import pandas as pd

_DB_USER = 'hooklab_user'
_DB_PASSWORD = 's9xzjib7so0jiiih'
_DB_ADDRESS = 'db-postgresql-nyc1-hooklab-do-user-6943115-0.db.ondigitalocean.com'
_DB_NAME = 'hooklab_db'
_DB_PORT = '25060'

DB_USER = "hooklab_admin"
DB_PASSWORD = "43QsRn3ADIyATTb0a0LK"
DB_ADDRESS = "hooklab-db-instance.postgres.database.azure.com"
DB_NAME = "hooklab_db"
DB_PORT = '5432'


def refresh_optimized_products(user_id, user_name):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
    .format(user= DB_USER,
    pw=DB_PASSWORD,
    db=DB_NAME,DB_PORT=DB_PORT,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)  
    with engine.begin() as cnx:
        update_sql = f"insert into hooklab_crawler.optimizable_products (sku_netshoes, suggested_price, marketplace, current_sale_price, user_id) select distinct on (p.pk_sku_netshoes) cp.fk_sku_netshoes as sku_netshoes, cp.suggested_price, cp.marketplace as marketplace, p.sale_price as current_sale_price, {user_id} as user_id from hooklab_crawler.customer_products cp inner join hooklab_crawler.products p on cp.fk_sku_netshoes = p.pk_sku_netshoes and cp.marketplace = p.marketplace where LOWER(p.seller) = '{user_name.lower()}' and cp.suggested_price - p.sale_price > 1 and cp.fk_user = {user_id} and p.is_available is true and cp.stock > 0 on conflict (user_id, sku_netshoes) do update set current_sale_price = excluded.current_sale_price, suggested_price = excluded.suggested_price, last_update = excluded.last_update"
        cnx.execute(update_sql)
    engine.dispose()
    return '200'
    
def get_hooklab_users():
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,DB_PORT=DB_PORT,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    df = pd.read_sql("select pk_id as user_id, user_name from hooklab_crawler.hooklab_users where active=True", engine)
    engine.dispose()
    return df
    
def check_optimize_products(user_id):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,DB_PORT=DB_PORT,
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
