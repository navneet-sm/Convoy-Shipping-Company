import pandas as pd
import string
import re
import sqlite3
import csv
import json
from dicttoxml import dicttoxml

x = input('Input file name\n')
file_name = (x.split('.'))[0]


def file_handler():
    if x.endswith('xlsx'):
        my_df = pd.read_excel(x, sheet_name='Vehicles', dtype=str)
        my_df.to_csv(f'{x.rstrip("xlsx")}csv')
        num_of_lines = my_df.shape[0]
        ending = " was" if my_df.shape[0] == 1 else "s were"
        print(f'{num_of_lines} line{ending} imported to {x.rstrip("xlsx")}csv')
        return my_df
    else:
        return pd.read_csv(x)


def cleaner():
    my_df, count = file_handler(), 0
    pattern = f'{string.ascii_letters}_. '
    column_names = [col for col in my_df.columns]
    for column in column_names:
        for cell in my_df[column]:
            if not cell.replace(' ', '').isnumeric():
                count += 1
            index = my_df[my_df[column] == cell].index.values
            my_df.at[index, column] = re.sub(fr'{[pattern]}', '', cell)
    my_df.to_csv(f'{file_name}[CHECKED].csv', header=False, index=None)
    ending = " was" if count == 1 else "s were"
    print(f'{count} cell{ending} corrected in {file_name}[CHECKED].csv')
    return my_df


def to_json(cursor, data, name):
    json_entries, xml_entries, records_counter = [], [], 0
    cursor.execute('SELECT * FROM convoy')
    result = cursor.fetchall()
    for entry in result:
        if entry[4] > 3:
            json_entries.append(sorting(entry, data))
        else:
            xml_entries.append(sorting(entry, data))
    json_convoy = {"convoy": json_entries}
    xml_convoy = {"convoy": xml_entries}
    json_ending = " was" if len(json_entries) == 1 else "s were"
    xml_ending = " was" if len(xml_entries) == 1 else "s were"
    with open(f'{name}.json', 'w') as file:
        json.dump(json_convoy, file)
    print(f'{len(json_entries)} vehicle{json_ending} saved into {name}.json')
    to_xml(xml_convoy, name, len(xml_entries), xml_ending)


def to_xml(data, name, qty, ending):
    xml = dicttoxml(data, root=False, attr_type=False, item_func=lambda a: 'vehicle').decode()
    with open(f'{name}.xml', 'w') as xml_file:
        xml_file.write(xml)
    print(f'{qty} vehicle{ending} saved into {name}.xml')


def sorting(entry, data):
    items = [item for item in entry]
    headers = [item for item in data]
    return dict(map(lambda a, b: (a, b), headers, items))


def score_algo(data):
    points = 0
    pitstops = ((450 / 100) * int(data[2])) / int(data[1])
    fuel_consumed = (450 / 100) * int(data[2])
    if 1 <= pitstops < 2:
        points += 1
    if pitstops < 1:
        points += 2
    if fuel_consumed <= 230:
        points += 2
    if fuel_consumed > 230:
        points += 1
    if int(data[3]) >= 20:  # capacity
        points += 2
    return str(points)


def main():
    num, f_name = 0, file_name
    table = 'CREATE TABLE convoy('
    if x.endswith('s3db'):
        cnx = sqlite3.connect(x)
        cursor_s3db = cnx.cursor()
        headers = ['vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load']
        to_json(cursor_s3db, headers, x.rstrip('.s3db'))
    else:
        if not x.endswith('[CHECKED].csv'):
            my_df = cleaner()
            name = f'{file_name}[CHECKED].csv'
        else:
            my_df = pd.read_csv(x)
            name = f'{x}'
            f_name = file_name.rstrip('[CHECKED]')
        conn = sqlite3.connect(f'{f_name}.s3db')
        cursor_name = conn.cursor()
        for header in my_df.columns:
            ending = 'INTEGER PRIMARY KEY' if header == 'vehicle_id' else 'INTEGER NOT NULL'
            table += f'\n{header} {ending},'
        table += f'\nscore INTEGER NOT NULL\n)'
        cursor_name.execute(table)
        with open(name, 'r') as file:
            file_reader = csv.reader(file)
            for line in file_reader:
                try:
                    line.append(score_algo(line))
                    cursor_name.execute(f'INSERT INTO convoy VALUES {tuple(line)}')
                    num += 1
                except (sqlite3.IntegrityError, ValueError):
                    pass
        ending = " was" if num == 1 else "s were"
        print(f'{num} record{ending} inserted into {f_name}.s3db')
        to_json(cursor_name, my_df.columns, f_name)
        conn.commit()
        conn.close()


main()