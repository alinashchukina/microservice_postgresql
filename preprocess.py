import pandas as pd


def read_file(path: str, n: int = 0) -> tuple:
    """
    Reads a .csv or .xlsx file, finds the name of file
    and reads it into pd.DataFrame
    :param path: full name of file with the path
    :param n: row number with headers
    :return: tuple, where table is pd.DataFrame with the
    content of file, table_name is file name
    """
    if path[-4:] == '.csv':
        table = pd.read_csv(path, sep=';', decimal=',', header=n)
        table_name = path.split('/')[-1][:-4]
    elif path[-5:] == '.xlsx':
        table = pd.read_excel(path, header=n)
        table_name = path.split('/')[-1][:-5]
    else:
        raise Exception('Incorrect format of file! Must be .csv or .xlsx')
    return table, table_name


def preprocess(table: pd.DataFrame) -> tuple:
    """
    Transforms table into list of tuples, where each tuple is for
    each row in table, finds names and types of columns
    :param table: table with content for postgresql
    :return: tuple, columns is the list with column names,
    data is the list with tuples for each row in table,
    types is the list with strings, where each string is a
    joined column name with its type
    """
    columns = list(table.columns)

    types = []
    for col in columns:
        if table[col].dtypes == 'int64':
            types.append(str(col) + ' integer')
        elif table[col].dtypes == 'float64':
            types.append(str(col) + ' double precision')
        elif table[col].dtypes == 'datetime64':
            types.append(str(col) + ' timestamp')
        else:
            types.append(str(col) + ' text')

    data = []
    for i in range(len(table)):
        data.append(tuple(table.iloc[i]))
    return columns, data, types
