import psycopg2
import os
import math
import tempfile

def getopenconnection(user='postgres', password='1234', dbname='movie_rating'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        print('A database named {0} already exists'.format(dbname))
    
    cur.close()
    con.close()

def get_partition_count(prefix, openconnection):
    cur = openconnection.cursor()
    cur.execute(f"""
        SELECT COUNT(table_name) FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name SIMILAR TO '{prefix}[0-9]+'
    """)
    count = cur.fetchone()[0]
    return count

def validate_rating(rating):
    if not isinstance(rating, (int, float)):
        raise ValueError("Rating must be a number")
    if rating < 0 or rating > 5:
        raise ValueError("Rating must be between 0 and 5")
    return float(rating)

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            {USER_ID_COLNAME} INTEGER,
            {MOVIE_ID_COLNAME} INTEGER,
            {RATING_COLNAME} FLOAT
        );
    """)
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            with open(ratingsfilepath, 'r') as input_file:
                for line in input_file:
                    parts = line.strip().split("::")
                    if len(parts) == 4:
                        temp_file.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
        
        with open(temp_file.name, 'r') as temp_file:
            cur.copy_from(temp_file, ratingstablename, sep='\t')
        
        os.unlink(temp_file.name)
        
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{ratingstablename}_rating 
            ON {ratingstablename} ({RATING_COLNAME})
        """)
        
        openconnection.commit()
        
    except Exception as e:
        print(f"Error loading data: {e}")
        openconnection.rollback()
        raise

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    RANGE_TABLE_PREFIX = 'range_part'
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i}")
    
    step = 5.0 / numberofpartitions
    
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE {RANGE_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME} INTEGER,
                {MOVIE_ID_COLNAME} INTEGER,
                {RATING_COLNAME} FLOAT
            )
        """)
    
    try:
        for i in range(numberofpartitions):
            lower = i * step
            upper = (i + 1) * step
            
            if i == 0:
                cur.execute(f"""
                    INSERT INTO {RANGE_TABLE_PREFIX}{i}
                    SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}
                    FROM {ratingstablename}
                    WHERE {RATING_COLNAME} >= {lower} AND {RATING_COLNAME} <= {upper}
                """)
            else:
                cur.execute(f"""
                    INSERT INTO {RANGE_TABLE_PREFIX}{i}
                    SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}
                    FROM {ratingstablename}
                    WHERE {RATING_COLNAME} > {lower} AND {RATING_COLNAME} <= {upper}
                """)
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    
    RANGE_TABLE_PREFIX = 'range_part'
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    rating = validate_rating(rating)
    
    num_partitions = get_partition_count(RANGE_TABLE_PREFIX, openconnection)
    
    if num_partitions == 0:
        rangepartition(ratingstablename, 5, openconnection)
        num_partitions = 5
    
    step = 5.0 / num_partitions
    partition_index = int(rating / step)
    if rating % step == 0 and partition_index != 0:
        partition_index = partition_index - 1
    
    try:
        cur.execute(f"""
            INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))
        
        cur.execute(f"""
            INSERT INTO {RANGE_TABLE_PREFIX}{partition_index} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i}")
    
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME} INTEGER,
                {MOVIE_ID_COLNAME} INTEGER,
                {RATING_COLNAME} FLOAT
            )
        """)
    
    try:
        for i in range(numberofpartitions):
            cur.execute(f"""
                INSERT INTO {RROBIN_TABLE_PREFIX}{i}
                SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}
                FROM (
                    SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME},
                           (ROW_NUMBER() OVER (ORDER BY ctid) - 1) % {numberofpartitions} as partition_id
                    FROM {ratingstablename}
                ) as distributed_data
                WHERE partition_id = {i}
            """)
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    rating = validate_rating(rating)
    
    num_partitions = get_partition_count(RROBIN_TABLE_PREFIX, openconnection)
    
    if num_partitions == 0:
        roundrobinpartition(ratingstablename, 5, openconnection)
        num_partitions = 5
    
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    
    next_partition = total_rows % num_partitions
    
    try:
        cur.execute(f"""
            INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{next_partition}
            ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e