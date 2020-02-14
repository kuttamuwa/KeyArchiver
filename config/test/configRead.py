import unittest


class ConfigReadTestDB(unittest.TestCase):
    def test_read_access_config(self):
        self.assertEqual(True, False)


class ConfigReadTestAnotherSection(unittest.TestCase):
    # another section tests
    pass


if __name__ == '__main__':
    unittest.main()
