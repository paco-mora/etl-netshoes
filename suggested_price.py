import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import numpy as np
import statistics

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
        
    def price_to_db(self):
        conn = self.engine.connect()
        trans = conn.begin()
        insert_sql = f"update hooklab_crawler.customer_products as cp set suggested_price = t.suggested_price from hooklab_crawler.temp_suggested_price as t where cp.fk_sku_netshoes = t.sku_netshoes and	cp.marketplace = t.marketplace and cp.fk_user = t.fk_user"
        conn.execute(insert_sql)
        trans.commit()
        conn.close()
        return 'products scraped!'
    
    def check_db(self, user_id):
        conn = self.engine.connect()
        trans = conn.begin()
        insert_sql = f"update hooklab_crawler.hooklab_users set last_suggested_price = now() where pk_id = {user_id}"
        conn.execute(insert_sql)
        trans.commit()
        conn.close()
        return 'products scraped!'

def get_optimized_only(offers):
    if(len(offers) > 1):
        price = []
        for i in offers:
            price.append(i['price'])
        std = np.std(price) 
        return float(str(int(statistics.mean(price) + std))[:-1] + '9.9')
    else:
        return offers[0]['price']

def get_opportunity(offers, user_name):
    if(len(offers) > 1):
        #if(offers[0]['seller_name'].lower() == 'netshoes' or offers[0]['seller_name'].lower() == 'zattini'):
        if(offers[0]['seller_name'].lower() in ['netshoes', 'zattini', 'asics', 'adidas']):
            if(offers[1]['seller_name'].lower() == user_name.lower()):
                try:
                    return round(offers[2]['price'] - 0.01, 2)
                except:
                    return offers[1]['price']
            else:
                return round(offers[1]['price'] - 0.01, 2)
        else:
            return round(offers[0]['price'] - 0.01, 2)
    else:
        return offers[0]['price']

def get_opportunity_seller(offers):
    return round(offers[1]['price'] - 0.01, 2)

def is_stock(x):
    if(x['stock'] == 1):
        return True
    else:
        return False
    
def verify_if_only_seller(offers):
    if(sum(1 if is_stock(x) == 1 else 0 for x in offers) > 1):
        return False
    else:
        return True
        
def get_suggested_price(seller, user_name, offers):
    if(seller.lower() != user_name.lower()):
        return get_opportunity(offers, user_name)
    else:
        if(verify_if_only_seller(offers)):
            return get_optimized_only(offers)
        else:
            return get_opportunity_seller(offers)
            
            
db = Database()

query = "select pk_id from hooklab_crawler.hooklab_users where active = true and pk_id != 666"
df_users = pd.read_sql(query, db.engine)

users = df_users['pk_id'].tolist()

for user_id in users:
    try:
        query = f"select p.pk_sku_netshoes as sku_netshoes, p.marketplace, p.seller, cp.seller_price, cp.fk_user, hu.user_name, p.offers from hooklab_crawler.customer_products as cp inner join hooklab_crawler.products as p on p.pk_sku_netshoes = cp.fk_sku_netshoes and cp.marketplace = p.marketplace inner join hooklab_crawler.hooklab_users as hu on hu.pk_id = cp.fk_user where p.offers is not null and p.is_available = True and hu.pk_id = {user_id}"
        df = pd.read_sql(query, db.engine)
        df['suggested_price'] = df.apply(lambda x: get_suggested_price(x['seller'], x['user_name'], x['offers']), axis=1)
        df = df[['sku_netshoes', 'marketplace', 'fk_user', 'suggested_price']]
        df.to_sql(f'temp_suggested_price', db.engine, schema='hooklab_crawler', if_exists='replace', index=False, method='multi')
        db.price_to_db()
        db.check_db(user_id)
        print('ok ', user_id)
    except:
        print('error with ', user_id)
