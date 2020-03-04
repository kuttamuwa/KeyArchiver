from setuptools import setup

setup(
    name='KeyArchiver',
    version='1.0',
    packages=['db', 'db.test', 'db.test.managerTest', 'db.test.connectorTest', 'db.managers', 'db.connectors', 'util',
              'trackers', 'sqlalchemy', 'pandas', 'numpy', 'pynput', 'pymongo', 'pyodbc', 'psycopg2', 'pyperclip'],
    url='https://github.com/kuttamuwa/KeyArchiver',
    license='GNU',
    author='umut ucok',
    author_email='ucok.umut@gmail.com',
    description='see README.md', install_requires=['pynput', 'pyperclip']
)
