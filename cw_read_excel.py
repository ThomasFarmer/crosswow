import pandas as pd
import numpy as np
import sqlite3, pickle

def list_excel_sheets(excel_path):
    xls = pd.ExcelFile(excel_path)
    print("Sheets found:")
    for sheet in xls.sheet_names:
        print(f"- {sheet}")

    return xls.sheet_names


def read_sheet(excel_path, sheet_name):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    print(df.head())
    return df

def test_get_cell(df,x,y):
    val = df.iat[x, y]
    return val

def make_matrix(df):
    return df.iloc[1:, 1:].to_numpy(dtype=object).tolist()


def store_matrix(conn, mx_blank, mx_sol):
    blob_blank = sqlite3.Binary(pickle.dumps(mx_blank, protocol=pickle.HIGHEST_PROTOCOL))
    blob_sol = sqlite3.Binary(pickle.dumps(mx_sol, protocol=pickle.HIGHEST_PROTOCOL))
    conn.execute("INSERT INTO crosswow(blank, solution) VALUES (?, ?)", (blob_blank, blob_sol))
    conn.commit()

def load_blank_matrix(conn, id):
    (blob_blank, ) = conn.execute("SELECT blank FROM crosswow WHERE id=?", (id,)).fetchone()
    return pickle.loads(blob_blank)

def load_solution_matrix(conn, id):
    (blob_sol, ) = conn.execute("SELECT blank FROM crosswow WHERE id=?", (id,)).fetchone()
    return pickle.loads(blob_sol)

def create_db(dbpath):

    # Connect to SQLite database
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()

    # # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "crosswow" (
        "id"	INTEGER NOT NULL,
        "blank"	BLOB NOT NULL,
        "solution"	BLOB NOT NULL,
        PRIMARY KEY("id" AUTOINCREMENT)
    )""")

    # # Insert data into the table
    # cursor.execute("INSERT INTO STUDENT VALUES ('Raju', '7th', 'A')")
    # cursor.execute("INSERT INTO STUDENT VALUES ('Shyam', '8th', 'B')")
    # cursor.execute("INSERT INTO STUDENT VALUES ('Baburao', '9th', 'C')")

    # # Display inserted data
    # print("Data Inserted in the table: ")
    # cursor.execute("SELECT * FROM STUDENT")
    # for row in cursor.fetchall():
    #     print(row)

    # # Commit changes and close connection
    conn.commit()
    conn.close()

def create_conn(dbpath):
    conn = sqlite3.connect(dbpath)
    #cursor = conn.cursor()
    return conn

def close_conn(conn):
    conn.close()

if __name__ == "__main__":
    file_path = "Crosswow 2025 IPM.xlsx"
    create_db('crosswow.db')

    xs = list_excel_sheets(file_path)
    print(xs)
    df_blank = read_sheet(file_path, xs[0])
    df_sol = read_sheet(file_path, xs[2])
    # print(test_get_cell(df_blank, 2, 2)) # 1
    # print(test_get_cell(df_blank, 0, 0)) # nan
    # print(test_get_cell(df_blank, 2, 3)) # semmi

    

    mx_blank = make_matrix(df_blank)
    mx_sol = make_matrix(df_sol)

    conn = create_conn('crosswow.db')
    #store_matrix(conn, mx_blank, mx_sol)

    mx_blank_loaded = load_blank_matrix(conn, 1)
    print('------------')
    print(mx_blank_loaded)


    close_conn(conn)

    # print(mx[2][2])
    # print(mx[1][1])
    # print(mx)