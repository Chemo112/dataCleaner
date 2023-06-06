import psycopg2
from flask_sqlalchemy import SQLAlchemy
from flask import Flask
import os
import pandas as pd


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

app = Flask(__name__)

DB = pd.read_csv("edx_courses.csv")
DB.to_sql('bronze', conn, if_exists='replace', index=False)

lista = [row for row in DB['subject']]
argomenti = sorted(list(set(lista)))

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + db_user + ':' + db_password + '@' + db_host + ':' + db_port + '/' + db_name
db = SQLAlchemy(app)

def reccomender(subjects_exp):
    dict_sbj = dict()
    res = dict()
    for sbj in subjects_exp:
        if subjects_exp[sbj] > 0:
            if subjects_exp[sbj] // 3 <= 1:
                diff = "Introductory"
            else:
                if subjects_exp[sbj] // 3 <= 2:
                    diff = "Intermediate"
                else:
                    diff = "Advanced"
            dict_sbj[sbj] = diff
    for sbj in dict_sbj:
        df = DB.loc[DB['subject'] == sbj]
        df.fillna('Description is Missing', inplace=True) #fa il replace delle descrizioni mancanti
        df = df.drop_duplicates() #rimuove righe duplicate
        df.to_sql('silver', conn, if_exists='replace', index=False)
        df1 = df.loc[df['Level'] == dict_sbj[sbj]]
        df1.to_sql('silver', conn, if_exists='replace', index=False)

        res[sbj] = df1
    return res


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=os.getenv("PORT"))
