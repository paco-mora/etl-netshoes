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


def refresh_oportunities_products(user_id, user_name):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
    .format(user= DB_USER,
    pw=DB_PASSWORD,
    db=DB_NAME,DB_PORT=DB_PORT,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)  
    with engine.begin() as cnx:
        update_sql = f"insert into hooklab_crawler.opportunity_products (sku_netshoes, user_id, suggested_price, seller, sale_price, marketplace, seller_price) select distinct on (cp.sku_seller) pk_sku_netshoes, cp.fk_user, (p.sale_price-0.01) as suggested_price, p.seller, p.sale_price,p.marketplace, cp.seller_price from hooklab_crawler.products p inner join hooklab_crawler.customer_products cp on p.pk_sku_netshoes = cp.fk_sku_netshoes 	and p.marketplace = cp.marketplace where cp.fk_user = '{user_id}' and LOWER(p.seller) != 'netshoes' and LOWER(p.seller) != 'zattini' and LOWER(p.seller) != LOWER('{user_name}') and (cp.stock > 0) and (p.is_available != false) and (LOWER(p.title) != 'indisponível') and (p.title is not null) and (seller_price - sale_price <= 1) and (( (cp.min_markup * cp.cost_price <= p.sale_price) and (p.sale_price < cp.seller_price) ) or (cp.cost_price is null and (p.sale_price < cp.seller_price) ) or ( ((cp.min_markup * cp.cost_price) > p.sale_price) and (p.sale_price < cp.seller_price) )) on conflict (sku_netshoes, user_id, marketplace) do update set  suggested_price = excluded.suggested_price, seller = excluded.seller, last_update = current_timestamp, sale_price = excluded.sale_price, seller_price = excluded.seller_price"         
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
    
def check_optimize_oportunities(user_id):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
        .format(user= DB_USER,
        pw=DB_PASSWORD,
        db=DB_NAME,DB_PORT=DB_PORT,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    with engine.begin() as cnx:
        update_sql = f'update hooklab_crawler.hooklab_users set last_oportunity_products = now() where pk_id = {user_id}'
        cnx.execute(update_sql)
    engine.dispose()
    return '200'
    
df = get_hooklab_users()

for user_id, user_name in df.values:
    refresh_oportunities_products(user_id, user_name)
    check_optimize_oportunities(user_id)
    print(user_id, ' - ', user_name)
