import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

_DB_USER = 'hooklab_user'
_DB_PASSWORD = 's9xzjib7so0jiiih'
_DB_ADDRESS = 'db-postgresql-nyc1-hooklab-do-user-6943115-0.db.ondigitalocean.com'
_DB_NAME = 'hooklab_db'

DB_USER = "hooklab_admin"
DB_PASSWORD = "43QsRn3ADIyATTb0a0LK"
DB_ADDRESS = "hooklab-db-instance.postgres.database.azure.com"
DB_NAME = "hooklab_db"
DB_PORT = '5432'

class Database():
    def __init__(self):
        self.engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
            .format(user= DB_USER,
            pw=DB_PASSWORD,
            db=DB_NAME,DB_PORT=DB_PORT,
            db_address = DB_ADDRESS), connect_args={}, echo=False)
        
    def to_db(self):
        conn = self.engine.connect()
        trans = conn.begin()
        insert_sql = f"update hooklab_crawler.customer_products as cp set seller_price = t.price, stock = t.stock, last_update_stock = now() from hooklab_crawler.temp_stock_price as t where cp.fk_sku_netshoes = t.pk_sku_netshoes and cp.marketplace = t.marketplace and cp.fk_user = t.fk_user 	and cp.fk_user = t.fk_user"
        conn.execute(insert_sql)
        trans.commit()
        conn.close()
        return 'products scraped!'


db = Database()

query = "select cp.fk_user, hu.netshoes_id, hu.user_name, p.pk_sku_netshoes, p.marketplace, p.offers from hooklab_crawler.products as p inner join hooklab_crawler.customer_products as cp on p.pk_sku_netshoes = cp.fk_sku_netshoes and p.marketplace = cp.marketplace inner join hooklab_crawler.hooklab_users as hu on hu.pk_id = cp.fk_user where p.is_available = true and hu.active = true and hu.pk_id not in (3, 666)"

df = pd.read_sql(query, db.engine)

def get_seller_info(netshoes_id, offers):
    try:
        info = list(filter(lambda item: str(item['seller_id']) == str(netshoes_id), offers))[0]
    except:
        info = {'price': 0, 'stock': 0}
    return {'price': info['price'],
           'stock': info['stock']}


df['price'] = df.apply(lambda x: get_seller_info(x['netshoes_id'], x['offers'])['price'], axis=1)
df['stock'] = df.apply(lambda x: get_seller_info(x['netshoes_id'], x['offers'])['stock'], axis=1)


df = df[['fk_user', 'pk_sku_netshoes', 'marketplace', 'price', 'stock']]

df.to_sql(f'temp_stock_price', db.engine, schema='hooklab_crawler', if_exists='replace', index=False, method='multi')

db.to_db()


#os.system("suggested_price.py")
