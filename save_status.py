from sqlalchemy import create_engine

_DB_NAME = 'hooklab_db'
_DB_USER = 'hooklab_user'
_DB_PASSWORD = 's9xzjib7so0jiiih'
_DB_ADDRESS = 'db-postgresql-nyc1-hooklab-do-user-6943115-0.db.ondigitalocean.com'
_DB_PORT = '25060'

DB_USER = "hooklab_admin"
DB_PASSWORD = "43QsRn3ADIyATTb0a0LK"
DB_ADDRESS = "hooklab-db-instance.postgres.database.azure.com"
DB_NAME = "hooklab_db"
DB_PORT = '5432'



def save_historic():
    customers = db.get_customers()
    for user_name, user_id in zip (customers['user_name'], customers['user_id']):
        netshoes = db.get_netshoes(user_id)
        losing = db.get_losing(user_id, user_name)
        alert = db.get_alert(user_id, user_name)
        selling = db.get_selling(user_id, user_name)
        available = selling + netshoes + alert + losing
        db.save_status(user_id, available, netshoes, losing, alert, selling)
    return '200'



class DatabaseHistoric():
    def __init__(self):
        self.engine = create_engine("postgresql+psycopg2://{user}:{pw}@{db_address}:{DB_PORT}/{db}"
            .format(user= DB_USER,
            pw=DB_PASSWORD,
            db=DB_NAME,DB_PORT=DB_PORT,
            db_address = DB_ADDRESS), connect_args={'sslmode':'require'}, echo=False)
    def get_customers(self):
        with self.engine.begin() as cnx:
            select_sql = "select pk_id, user_name from hooklab_crawler.hooklab_users"
            cursor = cnx.execute(select_sql)
        return parser_users(cursor)

    def get_available(self, user_name):
        with self.engine.begin() as cnx:
            val = [user_name]
            select_sql = "select count(*) from hooklab_crawler.products as p inner join hooklab_crawler.customer_products as cp on p.pk_sku_netshoes = cp.fk_sku_netshoes and p.marketplace = cp.marketplace where p.is_available = true and cp.stock > 0 and ((LOWER(seller)=LOWER(%s)))"
            cursor = cnx.execute(select_sql, val)
        return parser_count(cursor)

    def get_netshoes(self, user_id):
        with self.engine.begin() as cnx:
            val = [user_id]
            select_sql = "SELECT count(pk_sku_netshoes) FROM hooklab_crawler.products INNER JOIN hooklab_crawler.customer_products ON pk_sku_netshoes = fk_sku_netshoes AND products.marketplace = customer_products.marketplace WHERE fk_user = %s AND (is_available = 't' AND stock > 0) AND ( ((LOWER(seller) = 'netshoes') OR (LOWER(seller) = 'zattini')))"
            cursor = cnx.execute(select_sql, val)
        return parser_count(cursor)

    def get_losing(self, user_id, user_name):
        with self.engine.begin() as cnx:
            val = [user_id, user_name]
            select_sql = "SELECT count(pk_sku_netshoes) FROM hooklab_crawler.products as p INNER JOIN hooklab_crawler.customer_products as cp ON p.pk_sku_netshoes = cp.fk_sku_netshoes AND p.marketplace = cp.marketplace WHERE fk_user = %s AND (is_available = 't' AND stock > 0) AND ((min_markup * cost_price) > sale_price) AND (LOWER(seller) != LOWER(%s)) AND (LOWER(seller) != 'netshoes') AND (LOWER(seller) != 'zattini') AND (sale_price < seller_price)"
            cursor = cnx.execute(select_sql, val)
        return parser_count(cursor)

    def get_alert(self, user_id, user_name):
        with self.engine.begin() as cnx:
            val = [user_id, user_name, user_name]
            select_sql = "SELECT count(p.pk_sku_netshoes) FROM hooklab_crawler.products p INNER JOIN hooklab_crawler.customer_products cp ON p.pk_sku_netshoes = cp.fk_sku_netshoes AND p.marketplace = cp.marketplace  WHERE fk_user = %s AND (is_available = True AND stock > 0) AND(((cp.min_markup * cp.cost_price <= p.sale_price) AND LOWER(p.seller) != LOWER(%s) AND LOWER(p.seller) != 'netshoes' AND LOWER(p.seller) != 'zattini' AND (p.sale_price < cp.seller_price) ) OR ((cost_price IS NULL OR cost_price = '0') AND (LOWER(seller) != LOWER(%s) AND LOWER(seller) != 'netshoes' AND LOWER(seller) != 'zattini') AND (sale_price < seller_price)AND ((sale_price < seller_price) OR (seller_price IS NULL OR seller_price = '0'))))"
            cursor = cnx.execute(select_sql, val)
        return parser_count(cursor)

    def get_selling(self, user_id, user_name):
        with self.engine.begin() as cnx:
            val = [user_id, user_name]
            select_sql = "SELECT count(*) FROM hooklab_crawler.products as p INNER JOIN hooklab_crawler.customer_products as cp ON p.pk_sku_netshoes = cp.fk_sku_netshoes AND p.marketplace = cp.marketplace WHERE fk_user = %s AND (is_available = 't' AND stock > 0) AND ((LOWER(seller)=LOWER(%s)))"
            cursor = cnx.execute(select_sql, val)
        return parser_count(cursor)
    
    def save_status(self, user_id, available, unavailable, losing, alert, selling):
        with self.engine.begin() as cnx:
            val = [user_id, available, unavailable, losing, alert, selling]
            select_sql = "insert into hooklab_crawler.user_historic values (current_date, %s, %s, %s, %s, %s, %s)"
            cursor = cnx.execute(select_sql, val)
        return '200'
    
def parser_count(cursor):
    count = []
    for c in cursor:
        count.append(list(c))
    return count[0][0]

def parser_users(cursor):
    user_id = []
    user_name = []
    for u_id, u_name in cursor:
        user_id.append(u_id)
        user_name.append(u_name)
    return {'user_name': user_name,
           'user_id': user_id}


db  = DatabaseHistoric()
save_historic()
