import psycopg2

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
DBhost = "localhost"

Trip_TableName = 'Trip'

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host=DBhost,
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection

# Execute sql query
def execute(conn, cmd):
    with conn.cursor() as cursor:
        try:
            cursor.execute(cmd)
        except psycopg2.IntegrityError:
            conn.rollback()
        else:
            conn.commit()
