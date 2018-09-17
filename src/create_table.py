import psycopg2

host = ''
database = ''
user = ''
password = ''

text_cre = ('CREATE TABLE events(' +
                    'agency text,' +
                    'closed_date timestamp,' +
                    'complaint_type text,' +
                    'created_date timestamp,' +
                    'latitude double precision,' +
                    'longitude double precision,' +
                    'open_data_channel_type text)')


con = psycopg2.connect(host=host, database=database, user=user, password=password)
cur = con.cursor()
cur.execute(text_cre)
con.commit()
cur.close()
con.close()
