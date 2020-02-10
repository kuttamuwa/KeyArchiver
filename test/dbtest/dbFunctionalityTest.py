import unittest


class MyTestCase(unittest.TestCase):
    dbconnection = None  # you need to fill it before use

    def test_something(self):
        self.assertEqual(True, True)

    def save_text_intodb(self, text):
        pass

    def create_db_connection(self):
        pass

    def find_text_db(self, text):
        pass


if __name__ == '__main__':
    unittest.main()
