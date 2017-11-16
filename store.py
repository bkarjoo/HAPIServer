import sqlite3
import time
import datetime


db_path = 'C:\\Users\\b.karjoo\\Documents\\PycharmProjects\\HAPIStore\\HAPIStore.sqlite'


def execute_query(query):
    con = sqlite3.connect(db_path)
    con.isolation_level = None
    cur = con.cursor()
    cur.execute(query)
    if query.lstrip().upper().startswith('SELECT'):
        return cur.fetchall()
    return None


def select_all_from_es_messages():
    q = 'select * from es_messages'
    return execute_query(q)


def get_time_stamp():
    ts = time.time()
    import datetime
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def insert_message_into_es(message):
    execute_query(
        "INSERT INTO es_messages (time_stamp, message) VALUES ('{0}','{1}');".format(get_time_stamp(),message))


def insert_message_into_is(message):
    execute_query(
        "INSERT INTO is_messages (time_stamp, message) VALUES ('{0}','{1}');".format(get_time_stamp(), message))


def main():
    establish_connection()
    insert_message_into_es('#:31266:S:089:S:N:20171116 113417:ALGOGROUP:1206254:18:100:43.17::NITE:E::GM:B:M:-1:657:')
    res = select_all_from_es_messages()
    for r in res:
        print(r)

if __name__ == '__main__':
    main()