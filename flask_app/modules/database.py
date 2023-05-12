import psycopg2
from sqlalchemy import create_engine
from conf import DB_USER, DB_PASSWORD, DB_ADDRESS, DB_NAME, DB_PORT

def get_engine():
    return psycopg2.connect(user=DB_USER,
                            password=DB_PASSWORD,
                            host=DB_ADDRESS,
                            port=DB_PORT,
                            database=DB_NAME)


def db_query(query, arguments, query_type):
    try:
#        connection = psycopg2.connect(user=DB_USER,
#                                      password=DB_PASSWORD,
#                                      host=DB_ADDRESS,
#                                      port=DB_PORT,
#                                      database=DB_NAME)
        connection = get_engine()
        cursor = connection.cursor()
        cursor.execute(query, arguments)
        if(query_type == 'insert' or query_type == 'update'):
            connection.commit()
            count = cursor.rowcount
            return '200'
        elif(query_type == 'select'):
            return cursor.fetchall() 
    except (Exception, psycopg2.Error) as error :
        if(connection):
            return "Error: ", error
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            
def df_to_db(df, db_schema, db_table_name, if_exists='append'):
    try:
        print('writing: ', db_table_name)
        connection = create_engine('postgresql://'+DB_USER+':'+DB_PASSWORD+'@'+DB_ADDRESS+':'+DB_PORT+'/'+DB_NAME)
        print(df.to_sql(db_table_name, con=connection, schema= db_schema, if_exists=if_exists, index= False, chunksize=1000, method='multi'))
        return '200'
    except Exception as e:
        return str(e)
        if(connection):
            return "Error: ", error