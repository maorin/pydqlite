import pydqlite.dbapi2 as dbapi2

# Connect to the database
connection = dbapi2.connect(
    host='192.168.214.101',
    port=9001,
    database="hci_db"
)

try:
    # with connection.cursor() as cursor:
    #     cursor.execute('CREATE TABLE foo33 (id integer not null primary key, name text)')

    with connection.cursor() as cursor:
        #cursor.execute('CREATE TABLE foo (id integer not null primary key, name text)')
        
        
        #sql = "SELECT name FROM sqlite_master WHERE type='table';"
        # cursor.executemany('INSERT INTO foo(name) VALUES(?)', seq_of_parameters=(('a',), ('b',)))
        sql = "SELECT migrate_version.repository_id, migrate_version.repository_path, migrate_version.version FROM migrate_version WHERE migrate_version.repository_id = 'theweb'"
        cursor.execute(sql)
        print(cursor)
        result = cursor.fetchall()
        
        
        print("---------1111")
        print(result)
        print("---------2222")      
        #cursor.execute('INSERT INTO foo(name) VALUES(?)', seq_of_parameters=(('a',), ('b',)))

    # with connection.cursor() as cursor:
    #     # Read a single record with qmark parameter style
    #     sql = "SELECT `id`, `name` FROM `foo` WHERE `name`=?"
    #     cursor.execute(sql, ('a',))
    #     result = cursor.fetchone()
    #     print(result)
    #     # Read a single record with named parameter style
    #     sql = "SELECT `id`, `name` FROM `foo` WHERE `name`=:name"
    #     cursor.execute(sql, {'name': 'b'})
    #     result = cursor.fetchone()
    #     print(result)
finally:
    
    connection.close()