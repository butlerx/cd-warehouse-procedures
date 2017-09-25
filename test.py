import unittest
import psycopg2
import psycopg2.extras

def makeConnection():
    dojos_string = "host=localhost"+" dbname=cp-dojos-development"+" user=postgres"+" password="
    dojos_conn = psycopg2.connect(dojos_string)
    dojos_cursor = dojos_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return dojos_cursor

def readFromTable():
    cursor = makeConnection()
    return cursor.execute('select * from cd_dojos limit 10')

def insertRow():
    dojos_string = "host=localhost"+" dbname=cp-dojos-development"+" user=postgres"+" password="
    dojos_conn = psycopg2.connect(dojos_string)
    dojos_cursor = dojos_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = 'insert into cd_dojos (id, stage, deleted, verified, name) values (\'test-test-1-1-1-test\', 4, 1, 0, \'test\')'
    dojos_cursor.execute(sql)
    return dojos_conn.commit

class EqualityTest(unittest.TestCase):

    #Testing extracting value from json column
    def testJson(self):
        json_data = {"countryName":"Ireland","countryNumber":"372","continent":"EU","alpha2":"IE","alpha3":"IRL"}
        country = json_data['countryName']
        self.assertEqual('Ireland', country)

    #Testing fields which are None
    def testNone(self):
        data = None
        test = data['test'] if (data is not None) else 'Unknown'
        self.assertEqual('Unknown', test)

    #Test database connection
    def testConnection(self):
        try:
            makeConnection()
        except (psycopg2.Error) as e:
            self.fail("makeConnection() raised an error and could not connect to the database")

    #Test reading from connection
    def testRead(self):
        try:
            readFromTable()
        except (psycopg2.Error) as e:
            self.fail("readFromTable() rasied an error and could not read from the dojos table")

    #Test inserting to database
    def testInsert(self):
        try:
            insertRow()
        except (psycopg2.Error) as e:
            self.fail("insertRow() rasied an error and could not insert to the dojos table")


if __name__ == '__main__':
    unittest.main()
