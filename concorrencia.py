from sqlalchemy import create_engine
#import requests
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


def query_execute(query):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
    .format(user= DB_USER,
    pw=DB_PASSWORD,
    db=DB_NAME,DB_PORT=DB_PORT,
    db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)  
    with engine.begin() as cnx:
        insert_query = query
        cnx.execute(insert_query)
    engine.dispose()
    return '200'
    
short_1 = "update hooklab_crawler.products set competition_short = 1 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '3' day group by sku_netshoes, marketplace having count(distinct(seller)) = 1)"
short_2 = "update hooklab_crawler.products set competition_short = 3 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '3' day group by sku_netshoes, marketplace having count(distinct(seller)) > 1)"

med_1 = "update hooklab_crawler.products set competition_med = 1 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '5' day group by sku_netshoes, marketplace having count(distinct(seller)) = 1)"
med_2 = "update hooklab_crawler.products set competition_med = 2 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '5' day group by sku_netshoes, marketplace having count(distinct(seller)) = 2)"
med_3 = "update hooklab_crawler.products set competition_med = 3 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '5' day group by sku_netshoes, marketplace having count(distinct(seller)) > 2)"

high_1 = "update hooklab_crawler.products set competition_high = 1 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '10' day group by sku_netshoes, marketplace having count(distinct(seller)) = 1)"
high_2 = "update hooklab_crawler.products set competition_high = 2 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '10' day group by sku_netshoes, marketplace having count(distinct(seller)) >= 2)"
high_3 = "update hooklab_crawler.products set competition_high = 3 where pk_sku_netshoes in (select sku_netshoes from hooklab_crawler.historic_products where date_historic > current_date - interval '10' day group by sku_netshoes, marketplace having count(distinct(seller)) > 2)"

refresh_1 = "update hooklab_crawler.products set competition = round((competition_short * 0.50) + (competition_med * 0.35) + (competition_high * 0.15))::int"
refresh_2 = "update hooklab_crawler.products set competition = competition_short + competition_med + competition_high"

print('short 1')
query_execute(short_1)
print('short 2')
query_execute(short_2)

print('medium 1')
query_execute(med_1)
print('medium 2')
query_execute(med_2)
print('medium 3')
query_execute(med_3)

print('high 1')
query_execute(high_1)
print('high 2')
query_execute(high_2)
print('high 3')
query_execute(high_3)

print('refresh 1')
query_execute(refresh_1)
print('refresh 2')
query_execute(refresh_2)
