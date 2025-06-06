import mysql.connector
from mysql.connector import Error

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='root', password='hoa0976271476', dbname='mysql'):
    """
    Create a connection to MySQL database
    """
    connection = mysql.connector.connect(
        host='localhost',
        user=user,
        password=password,
        database=dbname
    )
    return connection

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data from ratings file to a table
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid INT,
            movieid INT,
            rating FLOAT
        )
    """)
    
    with open(ratingsfilepath, 'r') as file:
        for line in file:
            values = line.strip().split('::')
            userid = int(values[0])
            movieid = int(values[1])
            rating = float(values[2])
            
            cur.execute(f"""
                INSERT INTO {ratingstablename} (userid, movieid, rating)
                VALUES (%s, %s, %s)
            """, (userid, movieid, rating))
    
    cur.close()
    con.commit()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions based on range of ratings
    """
    con = openconnection
    cur = con.cursor()
    delta = 5 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    
    for i in range(numberofpartitions):
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INT,
                movieid INT,
                rating FLOAT
            )
        """)
    
    # Insert data using CASE to determine partition
    for i in range(numberofpartitions):
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM (
                SELECT 
                    userid,
                    movieid,
                    rating,
                    CASE 
                        WHEN rating = 0 THEN 0
                        ELSE FLOOR((rating - 0.0001) / {delta})::integer
                    END as partition_num
                FROM {ratingstablename}
            ) as temp
            WHERE partition_num = {i}
        """)
    
    cur.close()
    con.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions using round robin approach
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        
        # Create partition table
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INT,
                movieid INT,
                rating FLOAT
            )
        """)
        
        # Insert data using round robin
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating 
            FROM (
                SELECT userid, movieid, rating,
                (@row_num:=@row_num+1) as rnum
                FROM {ratingstablename}, (SELECT @row_num:=-1) r
            ) as temp 
            WHERE MOD(temp.rnum, {numberofpartitions}) = {i}
        """)
    
    cur.close()
    con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row using round robin approach
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Insert into main table
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES ({userid}, {itemid}, {rating})
    """)
    
    # Get total rows
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    
    # Calculate partition number
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows - 1) % numberofpartitions
    
    # Insert into partition
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute(f"""
        INSERT INTO {table_name} (userid, movieid, rating)
        VALUES ({userid}, {itemid}, {rating})
    """)
    
    cur.close()
    con.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row based on range
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Calculate partition number
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    
    # Insert into partition
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute(f"""
        INSERT INTO {table_name} (userid, movieid, rating)
        VALUES ({userid}, {itemid}, {rating})
    """)
    
    cur.close()
    con.commit()

def create_db(dbname):
    """
    Create a new database if it doesn't exist
    """
    try:
        connection = getopenconnection(dbname='mysql')
        cursor = connection.cursor()
        
        # Check if database exists
        cursor.execute(f"SHOW DATABASES LIKE '{dbname}'")
        result = cursor.fetchone()
        
        if not result:
            cursor.execute(f"CREATE DATABASE {dbname}")
            print(f"Database {dbname} created successfully")
        else:
            print(f"Database {dbname} already exists")
            
        cursor.close()
        connection.close()
        
    except Error as e:
        print(f"Error creating database: {e}")

def count_partitions(prefix, openconnection):
    """
    Count number of tables with given prefix
    """
    con = openconnection
    cur = con.cursor()
    
    cur.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name LIKE '{prefix}%'
    """)
    
    count = cur.fetchone()[0]
    cur.close()
    
    return count 