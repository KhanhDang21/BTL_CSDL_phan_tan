import psycopg2
import os
import math
import tempfile

# Tên cơ sở dữ liệu
DATABASE_NAME = 'movie_rating'

# Các hằng số cho tên bảng và cột
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

def getopenconnection(user='postgres', password='21122004', host='localhost', database=DATABASE_NAME):
    return psycopg2.connect("dbname='" + database + "' user='" + user + "' host='" + host + "' password='" + password + "'")

def create_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    # Kiểm tra xem database đã tồn tại chưa
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        print('A database named {0} already exists'.format(dbname))
    
    cur.close()
    con.close()

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    
    # Tạo bảng với index
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            {USER_ID_COLNAME} INTEGER,
            {MOVIE_ID_COLNAME} INTEGER,
            {RATING_COLNAME} FLOAT
        );
    """)
    
    try:
        # Tạo file tạm thời đã được xử lý
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            with open(ratingsfilepath, 'r') as input_file:
                for line in input_file:
                    parts = line.strip().split("::")
                    if len(parts) == 4:
                        # Chỉ lấy 3 cột cần thiết
                        temp_file.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
        
        # Load dữ liệu từ file tạm vào bảng
        with open(temp_file.name, 'r') as temp_file:
            cur.copy_from(temp_file, ratingstablename, sep='\t')
        
        # Xóa file tạm
        os.unlink(temp_file.name)
        
        # Tạo index cho cột rating
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{ratingstablename}_rating 
            ON {ratingstablename} ({RATING_COLNAME})
        """)
        
        openconnection.commit()
        
    except Exception as e:
        print(f"Lỗi khi load dữ liệu: {e}")
        openconnection.rollback()
        raise
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    # Xóa các bảng phân mảnh hiện có
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i}")
    
    # Tính khoảng cho phân mảnh
    step = 5.0 / numberofpartitions
    
    # Tạo tất cả các bảng phân mảnh trước
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE {RANGE_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME} INTEGER,
                {MOVIE_ID_COLNAME} INTEGER,
                {RATING_COLNAME} FLOAT
            )
        """)
    
    try:
        # Phân phối dữ liệu vào các bảng phân mảnh
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
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    
    # Xác định số phân mảnh hiện có
    cur.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE '{RANGE_TABLE_PREFIX}%'
    """)
    num_partitions = cur.fetchone()[0]
    
    if num_partitions == 0:
        rangepartition(ratingstablename, 5, openconnection)
        num_partitions = 5
    
    # Tính toán phân mảnh phù hợp
    step = 5.0 / num_partitions
    partition_index = int(rating / step)
    if rating % step == 0 and partition_index != 0:
        partition_index = partition_index - 1
    
    # Chèn vào bảng chính và phân mảnh
    cur.execute(f"""
        INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
        VALUES (%s, %s, %s)
    """, (userid, movieid, rating))
    
    cur.execute(f"""
        INSERT INTO {RANGE_TABLE_PREFIX}{partition_index} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
        VALUES (%s, %s, %s)
    """, (userid, movieid, rating))
    
    openconnection.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    # Xóa các phân mảnh hiện có
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i}")
    
    # Tạo tất cả các bảng phân mảnh trước
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME} INTEGER,
                {MOVIE_ID_COLNAME} INTEGER,
                {RATING_COLNAME} FLOAT
            )
        """)
    
    try:
        # Phân phối dữ liệu theo round robin sử dụng ROW_NUMBER() của PostgreSQL
        for i in range(numberofpartitions):
            cur.execute(f"""
                INSERT INTO {RROBIN_TABLE_PREFIX}{i}
                SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}
                FROM (
                    SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME},
                           ROW_NUMBER() OVER() as rnum
                    FROM {ratingstablename}
                ) as temp
                WHERE MOD(rnum-1, {numberofpartitions}) = {i}
            """)
        
        openconnection.commit()
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    
    # Xác định số phân mảnh hiện có
    cur.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE '{RROBIN_TABLE_PREFIX}%'
    """)
    num_partitions = cur.fetchone()[0]
    
    if num_partitions == 0:
        roundrobinpartition(ratingstablename, 1, openconnection)
        num_partitions = 1
    
    # Lấy tổng số hàng để xác định phân mảnh tiếp theo
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    next_partition = total_rows % num_partitions
    
    # Chèn vào bảng chính và phân mảnh
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
    cur.close()