#
# Tester for the assignment1 using PostgreSQL
#

DATABASE_NAME = 'movie_rating'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054  # Number of lines in the input file

import psycopg2
import traceback
import testHelper
import Group9_Assignment as TestAssignment


if __name__ == '__main__':
    conn = None
    try:
        testHelper.createdb(DATABASE_NAME)

        conn = testHelper.getopenconnection(dbname=DATABASE_NAME)
        cursor = conn.cursor()

        testHelper.deleteAllPublicTables(conn)

        [result, e] = testHelper.testloadratings(TestAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("loadratings function pass!")
        else:
            print("loadratings function fail!")

        [result, e] = testHelper.testrangepartition(TestAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("rangepartition function pass!")
        else:
            print("rangepartition function fail!")

        # ALERT:: Use only one at a time i.e. uncomment only one line at a time and run the script
        [result, e] = testHelper.testrangeinsert(TestAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
        # [result, e] = testHelper.testrangeinsert(TestAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
        if result:
            print("rangeinsert function pass!")
        else:
            print("rangeinsert function fail!")

        testHelper.deleteAllPublicTables(conn)
        TestAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

        [result, e] = testHelper.testroundrobinpartition(TestAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("roundrobinpartition function pass!")
        else:
            print("roundrobinpartition function fail!")

        # ALERT:: Change the partition index according to your testing sequence.
        [result, e] = testHelper.testroundrobininsert(TestAssignment, RATINGS_TABLE, 100, 1, 3, conn, '4')
        # [result, e] = testHelper.testroundrobininsert(TestAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
        # [result, e] = testHelper.testroundrobininsert(TestAssignment, RATINGS_TABLE, 100, 1, 3, conn, '1')
        # [result, e] = testHelper.testroundrobininsert(TestAssignment, RATINGS_TABLE, 100, 1, 3, conn, '2')
        if result:
            print("roundrobininsert function pass!")
        else:
            print("roundrobininsert function fail!")

        choice = input('Press enter to Delete all tables? ')
        if choice == '':
            testHelper.deleteAllPublicTables(conn)

    except Exception as detail:
        traceback.print_exc()
    finally:
        if conn and not conn.closed:
            conn.close()
