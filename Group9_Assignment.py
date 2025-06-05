import mysql.connector
import os
import math

# Tên cơ sở dữ liệu
DATABASE_NAME = 'movie_rating'

# Các hằng số cho tên bảng và cột
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

def getopenconnection(user='root', password='21122004dug', host='localhost', database=DATABASE_NAME):
    return mysql.connector.connect(user=user, password=password, host=host, database=database)

def create_db(dbname):
    con = getopenconnection(database='mysql')
    cur = con.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    con.commit()
    cur.close()
    con.close()

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            {USER_ID_COLNAME} INT,
            {MOVIE_ID_COLNAME} INT,
            {RATING_COLNAME} FLOAT
        );
    """)

    with open(ratingsfilepath, 'r') as file:
        for line in file:
            parts = line.strip().split("::")
            if len(parts) == 4:
                userid = int(parts[0])
                movieid = int(parts[1])
                rating = float(parts[2])
                cur.execute(f"""
                    INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
                    VALUES (%s, %s, %s)
                """, (userid, movieid, rating))

    openconnection.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    # Xóa các bảng phân mảnh hiện có
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i}")

    # Tính khoảng cho phân mảnh
    step = 5.0 / numberofpartitions
    
    for i in range(numberofpartitions):
        lower = i * step
        upper = (i + 1) * step
        
        # Tạo bảng phân mảnh
        cur.execute(f"""
            CREATE TABLE {RANGE_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME} INT,
                {MOVIE_ID_COLNAME} INT,
                {RATING_COLNAME} FLOAT
            )
        """)
        
        # Chèn dữ liệu dựa trên khoảng
        if i == 0:
            # Phân mảnh đầu tiên: rating >= lower VÀ rating <= upper
            condition = f"{RATING_COLNAME} >= {lower} AND {RATING_COLNAME} <= {upper}"
        else:
            # Các phân mảnh khác: rating > lower VÀ rating <= upper
            condition = f"{RATING_COLNAME} > {lower} AND {RATING_COLNAME} <= {upper}"

        cur.execute(f"""
            INSERT INTO {RANGE_TABLE_PREFIX}{i}
            SELECT * FROM {ratingstablename} WHERE {condition}
        """)

    openconnection.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    cursor = openconnection.cursor()
    try:
        # Xác định số phân mảnh hiện có
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name LIKE '{RANGE_TABLE_PREFIX}%'
        """)
        num_partitions = cursor.fetchone()[0]
        
        if num_partitions == 0:
            # Nếu chưa có phân mảnh nào, tạo mặc định 5 phân mảnh
            rangepartition(ratingstablename, 5, openconnection)
            num_partitions = 5

        # Tính toán phân mảnh phù hợp cho giá trị rating này
        # Phải khớp với logic của hàm rangepartition
        step = 5.0 / num_partitions
        partition_index = 0
        
        # Tìm phân mảnh chính xác dùng logic giống như rangepartition
        for i in range(num_partitions):
            lower = i * step
            upper = (i + 1) * step
            if i == 0:
                # Phân mảnh đầu tiên: rating >= lower VÀ rating <= upper  
                if rating >= lower and rating <= upper:
                    partition_index = i
                    break
            else:
                # Các phân mảnh khác: rating > lower VÀ rating <= upper
                if rating > lower and rating <= upper:
                    partition_index = i
                    break
                    
        partition_table = f"{RANGE_TABLE_PREFIX}{partition_index}"

        # Chèn vào bảng ratings chính
        cursor.execute(f"""
            INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))

        # Chèn vào bảng phân mảnh thích hợp
        cursor.execute(f"""
            INSERT INTO {partition_table} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))

        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi trong rangeinsert: {str(e)}")
        raise
    finally:
        cursor.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    try:
        if numberofpartitions <= 0:
            raise ValueError("Số phân mảnh phải là số dương")

        # Xóa các phân mảnh hiện có
        for i in range(numberofpartitions):
            cursor.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i}")

        # Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            cursor.execute(f"""
                CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                    {USER_ID_COLNAME} INT,
                    {MOVIE_ID_COLNAME} INT,
                    {RATING_COLNAME} FLOAT
                )
            """)

        # Lấy tổng số dòng
        cursor.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cursor.fetchone()[0]

        # Lấy dữ liệu từ bảng ratings theo lô
        batch_size = 10000
        offset = 0
        partition_counts = [0] * numberofpartitions
        
        while True:
            cursor.execute(f"""
                SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}
                FROM {ratingstablename}
                ORDER BY {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}
                LIMIT %s OFFSET %s
            """, (batch_size, offset))
            
            rows = cursor.fetchall()
            if not rows:
                break
                
            # Phân phối các hàng theo kiểu luân phiên
            for i, row in enumerate(rows):
                partition_idx = (offset + i) % numberofpartitions
                cursor.execute(
                    f"INSERT INTO {RROBIN_TABLE_PREFIX}{partition_idx} VALUES (%s, %s, %s)",
                    row
                )
                partition_counts[partition_idx] += 1
        
            offset += len(rows)
            openconnection.commit()

    except Exception as e:
        openconnection.rollback()
        print(f"Error in roundrobinpartition: {str(e)}")
        raise
    finally:
        cursor.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    try:
        # Xác định số phân mảnh hiện có
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name LIKE '{RROBIN_TABLE_PREFIX}%'
        """)
        num_partitions = cursor.fetchone()[0]

        if num_partitions == 0:
            # Nếu chưa có phân mảnh nào, tạo một phân mảnh mặc định
            roundrobinpartition(ratingstablename, 1, openconnection)
            num_partitions = 1

        # Lấy tổng số hàng hiện tại để xác định phân mảnh tiếp theo
        cursor.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cursor.fetchone()[0]
        next_partition = total_rows % num_partitions

        # Chèn vào bảng chính
        cursor.execute(f"""
            INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, itemid, rating))

        # Chèn vào bảng phân mảnh thích hợp
        cursor.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{next_partition}
            ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, itemid, rating))

        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi trong roundrobininsert: {str(e)}")
        raise
    finally:
        cursor.close()