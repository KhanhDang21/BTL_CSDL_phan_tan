import psycopg2
import os
import math
import tempfile

def getopenconnection(user='postgres', password='1', dbname='movie_rating'):
    """
    Tạo kết nối đến PostgreSQL database
    Args:
        user: Tên user database
        password: Mật khẩu
        dbname: Tên database
    Returns:
        Connection object
    """
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_db(dbname):
    """
    Tạo database mới nếu chưa tồn tại
    Args:
        dbname: Tên database cần tạo
    """
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

def get_partition_count(prefix, openconnection):
    """
    Đếm số lượng partition hiện có dựa trên prefix - sử dụng như metadata
    Args:
        prefix: Tiền tố của tên bảng partition
        openconnection: Kết nối database
    Returns:
        Số lượng partition
    """
    cur = openconnection.cursor()
    cur.execute(f"""
        SELECT COUNT(table_name) FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name SIMILAR TO '{prefix}[0-9]+'
    """)
    count = cur.fetchone()[0]
    # Không đóng cursor - để connection quản lý
    return count

def validate_rating(rating):
    """
    Kiểm tra tính hợp lệ của giá trị rating
    Args:
        rating: Giá trị rating cần kiểm tra
    Returns:
        Giá trị rating đã được validate
    Raises:
        ValueError: Nếu rating không hợp lệ
    """
    if not isinstance(rating, (int, float)):
        raise ValueError("Rating must be a number")
    if rating < 0 or rating > 5:
        raise ValueError("Rating must be between 0 and 5")
    return float(rating)

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Load dữ liệu từ file ratings vào database - không sửa đổi file gốc
    Args:
        ratingstablename: Tên bảng ratings (không mã hóa cứng)
        ratingsfilepath: Đường dẫn file ratings (không mã hóa cứng)
        openconnection: Kết nối database
    """
    cur = openconnection.cursor()
    
    # Định nghĩa các hằng số cục bộ thay vì toàn cục
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    # Tạo bảng với lược đồ đúng yêu cầu
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            {USER_ID_COLNAME} INTEGER,
            {MOVIE_ID_COLNAME} INTEGER,
            {RATING_COLNAME} FLOAT
        );
    """)
    
    try:
        # Tạo file tạm thời để xử lý - không sửa đổi file gốc
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
        
        # Tạo index cho cột rating để tối ưu performance
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{ratingstablename}_rating 
            ON {ratingstablename} ({RATING_COLNAME})
        """)
        
        openconnection.commit()
        
    except Exception as e:
        print(f"Lỗi khi load dữ liệu: {e}")
        openconnection.rollback()
        raise
    # Không đóng cursor - để connection quản lý

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Phân mảnh bảng ratings theo khoảng giá trị rating
    Số phân mảnh bắt đầu từ 0: range_part0, range_part1, ...
    Args:
        ratingstablename: Tên bảng ratings
        numberofpartitions: Số lượng partition
        openconnection: Kết nối database
    """
    cur = openconnection.cursor()
    
    # Định nghĩa các hằng số cục bộ - không sử dụng tiền tố khác
    RANGE_TABLE_PREFIX = 'range_part'  # Không thay đổi tiền tố đã cho
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    # Xóa các bảng phân mảnh hiện có - bắt đầu từ 0
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i}")
    
    # Tính khoảng cho phân mảnh
    step = 5.0 / numberofpartitions
    
    # Tạo tất cả các bảng phân mảnh trước - lược đồ đúng yêu cầu
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
                # Partition đầu tiên bao gồm cả giá trị lower bound
                cur.execute(f"""
                    INSERT INTO {RANGE_TABLE_PREFIX}{i}
                    SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}
                    FROM {ratingstablename}
                    WHERE {RATING_COLNAME} >= {lower} AND {RATING_COLNAME} <= {upper}
                """)
            else:
                # Các partition khác chỉ lấy giá trị lớn hơn lower bound
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
    # Không đóng cursor - để connection quản lý

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    """
    Chèn dữ liệu mới vào bảng chính và partition tương ứng - có thể gọi nhiều lần
    Args:
        ratingstablename: Tên bảng ratings
        userid: ID người dùng
        movieid: ID phim
        rating: Đánh giá
        openconnection: Kết nối database
    """
    cur = openconnection.cursor()
    
    # Định nghĩa các hằng số cục bộ
    RANGE_TABLE_PREFIX = 'range_part'
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    # Validate input
    rating = validate_rating(rating)
    
    # Sử dụng metadata để đếm số partition
    num_partitions = get_partition_count(RANGE_TABLE_PREFIX, openconnection)
    
    if num_partitions == 0:
        rangepartition(ratingstablename, 5, openconnection)
        num_partitions = 5
    
    # Tính toán phân mảnh phù hợp
    step = 5.0 / num_partitions
    partition_index = int(rating / step)
    # Xử lý trường hợp rating nằm trên biên của partition
    if rating % step == 0 and partition_index != 0:
        partition_index = partition_index - 1
    
    # Sử dụng transaction để đảm bảo consistency
    try:
        # Insert vào bảng chính
        cur.execute(f"""
            INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))
        
        # Insert vào partition
        cur.execute(f"""
            INSERT INTO {RANGE_TABLE_PREFIX}{partition_index} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    # Không đóng cursor - để connection quản lý

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Phân mảnh bảng ratings theo phương pháp round robin
    Số phân mảnh bắt đầu từ 0: rrobin_part0, rrobin_part1, ...
    Args:
        ratingstablename: Tên bảng ratings
        numberofpartitions: Số lượng partition
        openconnection: Kết nối database
    """
    cur = openconnection.cursor()
    
    # Định nghĩa các hằng số cục bộ - không thay đổi tiền tố đã cho
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    # Xóa các phân mảnh hiện có - bắt đầu từ 0
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i}")
    
    # Tạo tất cả các bảng phân mảnh trước - lược đồ đúng yêu cầu
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME} INTEGER,
                {MOVIE_ID_COLNAME} INTEGER,
                {RATING_COLNAME} FLOAT
            )
        """)
    
    try:
        # Phân phối dữ liệu theo round robin sử dụng ctid
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
    # Không đóng cursor - để connection quản lý

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn dữ liệu mới vào bảng chính và partition tương ứng - có thể gọi nhiều lần
    Args:
        ratingstablename: Tên bảng ratings
        userid: ID người dùng
        itemid: ID phim
        rating: Đánh giá
        openconnection: Kết nối database
    """
    cur = openconnection.cursor()
    
    # Định nghĩa các hằng số cục bộ
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    USER_ID_COLNAME = 'userid'
    MOVIE_ID_COLNAME = 'movieid'
    RATING_COLNAME = 'rating'
    
    # Validate input
    rating = validate_rating(rating)
    
    # Sử dụng metadata để đếm số partition
    num_partitions = get_partition_count(RROBIN_TABLE_PREFIX, openconnection)
    
    if num_partitions == 0:
        roundrobinpartition(ratingstablename, 5, openconnection)
        num_partitions = 5
    
    # Lấy tổng số hàng để tính round robin
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    
    # Tính partition theo round robin
    next_partition = total_rows % num_partitions
    
    # Sử dụng transaction để đảm bảo consistency
    try:
        # Insert vào bảng chính
        cur.execute(f"""
            INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        # Insert vào partition
        cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{next_partition}
            ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
            VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    # Không đóng cursor - để connection quản lý