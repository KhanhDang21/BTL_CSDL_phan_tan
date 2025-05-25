import traceback
import mysql.connector
from mysql.connector import errorcode

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# SETUP Functions
def createdb(dbname):
    try:
        con = getopenconnection(dbname=None)
        cur = con.cursor()

        cur.execute(f"SHOW DATABASES LIKE '{dbname}'")
        result = cur.fetchone()

        if not result:
            cur.execute(f"CREATE DATABASE {dbname}")
        else:
            print(f'A database named "{dbname}" already exists')

        cur.close()
        con.close()
    except mysql.connector.Error as err:
        print("Error: {}".format(err))

def delete_db(dbname):
    con = getopenconnection(dbname=None)
    cur = con.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
    cur.close()
    con.close()

def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    for (table_name,) in tables:
        cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    cur.close()

def getopenconnection(user='root', password='', dbname='movie_rating'):
    config = {
        'user': 'root',
        'password': 'khanhdangkhukho',
        'host': 'localhost',
    }
    if dbname:
        config['database'] = dbname
    return mysql.connector.connect(**config)

def getCountrangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    countList = []
    interval = 5.0 / numberofpartitions
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename} WHERE rating >= {0} AND rating <= {interval}")
    countList.append(int(cur.fetchone()[0]))

    lowerbound = interval
    for i in range(1, numberofpartitions):
        upperbound = lowerbound + interval
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename} WHERE rating > {lowerbound} AND rating <= {upperbound}")
        countList.append(int(cur.fetchone()[0]))
        lowerbound = upperbound

    cur.close()
    return countList

def getCountroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    countList = []
    try:
        # Lấy tổng số dòng
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cur.fetchone()[0]
        
        # Tính toán số dòng lý tưởng mỗi partition
        base_count = total_rows // numberofpartitions
        remainder = total_rows % numberofpartitions
        
        # Tạo danh sách count
        countList = [base_count + 1 if i < remainder else base_count 
                    for i in range(numberofpartitions)]
                    
    except Exception as e:
        print(f"Error counting round-robin partitions: {str(e)}")
        raise
    finally:
        cur.close()
    
    return countList

# Helpers for Tester functions
def checkpartitioncount(cursor, expectedpartitions, prefix):
    cursor.execute(
        f"SELECT COUNT(TABLE_NAME) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE '{prefix}%'"
    )
    count = int(cursor.fetchone()[0])
    if count != expectedpartitions:
        raise Exception(
            f'Range partitioning not done properly. Expected {expectedpartitions} table(s) but found {count} table(s)'
        )

def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    selects = [f"SELECT * FROM {rangepartitiontableprefix}{i}" for i in range(partitionstartindex, n + partitionstartindex)]
    cur.execute(f"SELECT COUNT(*) FROM ({' UNION ALL '.join(selects)}) AS T")
    count = int(cur.fetchone()[0])
    return count

def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    with openconnection.cursor() as cur:
        if not isinstance(n, int) or n < 0:
            checkpartitioncount(cur, 0, rangepartitiontableprefix)
        else:
            checkpartitioncount(cur, n, rangepartitiontableprefix)
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count < ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception(f"Completeness test failed. Expected {ACTUAL_ROWS_IN_INPUT_FILE}, got {count}")
            if count > ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception(f"Disjointness test failed. Expected {ACTUAL_ROWS_IN_INPUT_FILE}, got {count}")
            if count != ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception(f"Reconstruction test failed. Expected {ACTUAL_ROWS_IN_INPUT_FILE}, got {count}")

def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    with openconnection.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {expectedtablename} WHERE {USER_ID_COLNAME} = %s AND {MOVIE_ID_COLNAME} = %s AND {RATING_COLNAME} = %s",
            (userid, itemid, rating)
        )
        count = int(cur.fetchone()[0])
        return count == 1

def testEachRangePartition(ratingstablename, n, openconnection, rangepartitiontableprefix):
    countList = getCountrangepartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(n):
        cur.execute(f"SELECT COUNT(*) FROM {rangepartitiontableprefix}{i}")
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception(f"{rangepartitiontableprefix}{i} has {count} rows; expected {countList[i]}")

def testEachRoundrobinPartition(ratingstablename, n, openconnection, roundrobinpartitiontableprefix):
    countList = getCountroundrobinpartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(n):
        cur.execute(f"SELECT COUNT(*) FROM {roundrobinpartitiontableprefix}{i}")
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception(f"{roundrobinpartitiontableprefix}{i} has {count} rows; expected {countList[i]}")

# ##########

def testloadratings(MyAssignment, ratingstablename, filepath, openconnection, rowsininpfile):
    try:
        MyAssignment.loadratings(ratingstablename, filepath, openconnection)
        with openconnection.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
            count = int(cur.fetchone()[0])
            if count != rowsininpfile:
                raise Exception(f'Expected {rowsininpfile} rows, but found {count}')
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testrangepartition(MyAssignment, ratingstablename, n, openconnection, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    try:
        MyAssignment.rangepartition(ratingstablename, n, openconnection)
        testrangeandrobinpartitioning(n, openconnection, RANGE_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRangePartition(ratingstablename, n, openconnection, RANGE_TABLE_PREFIX)
        return [True, None]
    except Exception as e:
        traceback.print_exc()
        return [False, e]

def testroundrobinpartition(MyAssignment, ratingstablename, numberofpartitions, openconnection,
                            partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    try:
        MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)
        testrangeandrobinpartitioning(numberofpartitions, openconnection, RROBIN_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRoundrobinPartition(ratingstablename, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX)
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testroundrobininsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    try:
        expectedtablename = RROBIN_TABLE_PREFIX + expectedtableindex
        MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(f"Insert failed in {expectedtablename}")
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testrangeinsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    try:
        expectedtablename = RANGE_TABLE_PREFIX + expectedtableindex
        MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(f"Insert failed in {expectedtablename}")
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]