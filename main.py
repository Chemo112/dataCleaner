import psycopg2
import os
import pandas as pd
import sqlalchemy

db_host = os.environ.get('PGHOST')
db_port = os.environ.get('PGPORT')
db_name = os.environ.get('PGDATABASE')
db_user = os.environ.get('PGUSER')
db_password = os.environ.get('PGPASSWORD')

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)

engine = sqlalchemy.create_engine('postgresql://' + db_user + ':' + db_password + '@' + db_host + ':' + db_port + '/' + db_name)


#here we must implement the csv input from the scaper
DB = pd.read_csv("edx_courses.csv")
DB.to_sql('bronze', engine, if_exists='replace', index=False)

lista = [row for row in DB['subject']]
argomenti = sorted(list(set(lista)))

df = DB[['title','subject','course_description','Level','course_url']].copy()
df.to_sql('silver', engine, if_exists='replace', index=False)

df1 = DB[['subject','course_description','Level','course_url']].copy()

df1.fillna(' ', inplace=True)  # fa il replace delle descrizioni mancanti
df1 = df1.drop_duplicates()  # rimuove righe duplicate
df1.to_sql('gold', engine, if_exists='replace', index=False)

