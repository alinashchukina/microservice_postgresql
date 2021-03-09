from preprocess import read_file, preprocess
from database import create_connection, create_database, execute_query
from database import insert, create_table
from service import start
from click.testing import CliRunner


def test_preprocess_read():
    """Test preprocess.py read_file function"""
    assert read_file('students.csv')[0]['GPA'][0] == 4.7
    assert read_file('students.xlsx')[0]['GPA'][0] == 4.3
    assert read_file('students.csv')[1] == 'students'
    assert read_file('new_data/students.csv')[1] == 'students'
    assert read_file('students.xlsx')[1] == 'students'


def test_preprocess_preprocess():
    """Test preprocess.py preprocess function"""
    assert preprocess(read_file('students.xlsx')[0])[2][1] == 'age integer'
    assert preprocess(read_file('students.xlsx')[0])[2][0] == 'name text'
    assert preprocess(read_file('students.xlsx')[0])[2][5] == 'GPA double precision'
