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

def save_products_historic():
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
    .format(user= DB_USER,
    pw=DB_PASSWORD,
    db=DB_NAME,DB_PORT=DB_PORT,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)  
    with engine.begin() as cnx:
        insert_query = "insert into hooklab_crawler.historic_products (date_historic, sku_netshoes, marketplace, sale_price, seller) (select now()::date, pk_sku_netshoes, marketplace, sale_price, seller from hooklab_crawler.products where is_available = true) on conflict (date_historic, sku_netshoes, marketplace) do nothing"
        cnx.execute(insert_query)
    engine.dispose()
    return '200'
    
save_products_historic()
