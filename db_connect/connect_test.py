from db_connect import create_session

session = create_session()

session.set_keyspace('732_final')
rows = session.execute('insert into test_table(test_key) values(1)')

for r in rows.current_rows:  
    print("Found : {}".format(r))