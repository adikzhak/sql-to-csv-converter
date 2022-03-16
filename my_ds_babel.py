import sqlite3
import csv


def sql_to_csv(db_name, table_name):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    cur.execute('SELECT* FROM ' + table_name)
    converted_csv_name = db_name[:-3] + '.csv'
    with open(converted_csv_name, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list(map(lambda x: x[0], cur.description)))
        writer.writerows(cur.fetchall())

def csv_to_sql(csv_name):
    column_name = []
    data = []
    with open(csv_name, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            column_name = column_name + row
            break
        for row in reader:
            if len(row) == len(column_name):
                data.append(row)

    column_name = list(map(lambda x: x.replace(' ', '_'), column_name))
    column_name = list(map(lambda x: x.replace('(', ''), column_name))
    column_name = list(map(lambda x: x.replace(')', ''), column_name))
    
    converted_db_name = csv_name[:-4] + '.db'
    con = sqlite3.connect(converted_db_name)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS "+csv_name[:-4]+"("+','.join(column_name)+")")
    cur.executemany("INSERT INTO "+csv_name[:-4]+" VALUES("+','.join(['?']*len(column_name))+")", data)
    con.commit()

if __name__ == "__main__":

    db_name = 'all_fault_line.db'
    table_name = 'fault_lines'
    csv_name = 'list_volcano.csv'

    sql_to_csv(db_name, table_name)
    csv_to_sql(csv_name)