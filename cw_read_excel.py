import pandas as pd
import numpy as np
import sqlite3
import pickle
import json

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

# Board: 30 rows, 30 columns (B–AE). Include first row (CROSSWOW line) in both Quiz and Solution.
BOARD_ROWS = 30
BOARD_COL_START = 1   # Excel col B (0-based)
BOARD_COL_END = 31    # B–AE = 30 columns

# Main row (fősor) sequence: Excel row 34, columns L to AE (one cell per clue; empties = word boundary).
# With header=0, df row 0 = Excel row 2, so Excel row 34 = df index 32.
MAIN_ROW_EXCEL_ROW_INDEX = 32   # 0-based pandas row index for Excel row 34
MAIN_ROW_COL_START = 11        # L = 0-based 11
MAIN_ROW_COL_END = 31          # AE = 0-based 30, slice 11:31
WORD_BOUNDARY_MARKER = " "     # stored between the two words (one space)


def make_matrix(df):
    """Legacy: first row and column skipped. Prefer make_board_matrix for B2:AE31."""
    return df.iloc[1:, 1:].to_numpy(dtype=object).tolist()


def _sanitize_cell(c):
    """NaN/None -> '' (black/blocked). Keep '.' as-is (white placeholder in Excel, shown as empty in UI)."""
    if c is None:
        return ""
    if isinstance(c, float) and pd.isna(c):
        return ""
    return c


def make_board_matrix(df):
    """Extract the board: first 30 rows (include CROSSWOW row). Same for Quiz and Solution."""
    raw = df.iloc[0:BOARD_ROWS, BOARD_COL_START:BOARD_COL_END].to_numpy(dtype=object).tolist()
    return [[_sanitize_cell(c) for c in row] for row in raw]


def ensure_main_row_clues_column(conn):
    cur = conn.execute("PRAGMA table_info(crosswow)")
    if any(row[1] == "main_row_clues" for row in cur.fetchall()):
        return
    conn.execute("ALTER TABLE crosswow ADD COLUMN main_row_clues TEXT")
    conn.commit()


def _is_empty(v):
    if v is None or v == "":
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def read_main_row_sequence(df, row_index=None, col_start=None, col_end=None):
    """Read the main row (fősor) from L34:AE34. Clue IDs in order; one space marker between words; trailing empties discarded."""
    col_start = col_start if col_start is not None else MAIN_ROW_COL_START
    col_end = col_end if col_end is not None else MAIN_ROW_COL_END
    # Excel row 34 → df index 32 when header=0; fallback 33, 31 if needed
    rows_to_try = (row_index,) if row_index is not None else (MAIN_ROW_EXCEL_ROW_INDEX, 33, 31)
    for try_row in rows_to_try:
        if try_row is None or try_row >= len(df):
            continue
        try:
            row = df.iloc[try_row, col_start:col_end]
        except (IndexError, KeyError):
            continue
        cells = [_sanitize_cell(c) for c in row]
        out = []
        i = 0
        while i < len(cells):
            v = cells[i]
            if not _is_empty(v):
                out.append(str(v).strip() if not isinstance(v, str) else v.strip())
                i += 1
                continue
            j = i
            while j < len(cells) and _is_empty(cells[j]):
                j += 1
            if j < len(cells):
                out.append(WORD_BOUNDARY_MARKER)
            i = j
        if out:
            return out
    return []


def store_matrix(conn, mx_blank, mx_sol, main_row_clues=None):
    ensure_main_row_clues_column(conn)
    blob_blank = sqlite3.Binary(pickle.dumps(mx_blank, protocol=pickle.HIGHEST_PROTOCOL))
    blob_sol = sqlite3.Binary(pickle.dumps(mx_sol, protocol=pickle.HIGHEST_PROTOCOL))
    clues_json = json.dumps(main_row_clues) if main_row_clues is not None else None
    conn.execute(
        "INSERT INTO crosswow(blank, solution, main_row_clues) VALUES (?, ?, ?)",
        (blob_blank, blob_sol, clues_json),
    )
    conn.commit()


def update_matrix(conn, id, mx_blank, mx_sol, main_row_clues=None):
    """Overwrite existing puzzle row so the app (puzzle/1) shows the new data."""
    ensure_main_row_clues_column(conn)
    blob_blank = sqlite3.Binary(pickle.dumps(mx_blank, protocol=pickle.HIGHEST_PROTOCOL))
    blob_sol = sqlite3.Binary(pickle.dumps(mx_sol, protocol=pickle.HIGHEST_PROTOCOL))
    clues_json = json.dumps(main_row_clues) if main_row_clues is not None else None
    conn.execute(
        "UPDATE crosswow SET blank=?, solution=?, main_row_clues=? WHERE id=?",
        (blob_blank, blob_sol, clues_json, id),
    )
    conn.commit()


def load_main_row_clues(conn, id):
    row = conn.execute("SELECT main_row_clues FROM crosswow WHERE id=?", (id,)).fetchone()
    if not row or row[0] is None:
        return None
    try:
        return json.loads(row[0])
    except (TypeError, ValueError):
        return None

def ensure_definitions_tables(conn):
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='definitions_horizontal'"
    )
    if cur.fetchone():
        return
    conn.execute("""
        CREATE TABLE "definitions_horizontal" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "puzzle_id" INTEGER NOT NULL,
        "clue" TEXT NOT NULL,
        "definition" TEXT NOT NULL
    )""")
    conn.execute("""
        CREATE TABLE "definitions_vertical" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "puzzle_id" INTEGER NOT NULL,
        "clue" TEXT NOT NULL,
        "definition" TEXT NOT NULL
    )""")
    conn.execute("""
        CREATE TABLE "puzzle_fosor_text" (
        "puzzle_id" INTEGER PRIMARY KEY,
        "solution_text" TEXT NOT NULL
    )""")
    conn.commit()


def _normalize_direction(val):
    if val is None or val == "":
        return None
    s = str(val).strip().upper()
    if s in ("H", "HORIZONTAL", "VÍZSZINTES", "VIZSZINTES"):
        return "H"
    if s in ("V", "VERTICAL", "FÜGGŐLEGES", "FUGGOLEGES"):
        return "V"
    if s.startswith("H") or "vízs" in s.lower() or "vizs" in s.lower():
        return "H"
    if s.startswith("V") or "függ" in s.lower() or "fugg" in s.lower():
        return "V"
    return None


def read_definitions_from_sheet(df):
    """
    Parse Meghatározások sheet. Only columns A and B are used.
    - Column A = section marker or clue number; column B = definition text (always).
    - "FS" or "fősor" in A → B is the fősor definition.
    - "Vízszintes" in A → following rows (A=number, B=text) go to horizontal table.
    - "Függőleges" in A → following rows go to vertical table.
    Returns { horizontal: [(clue, definition), ...], vertical: [...], fosor_text: str }.
    """
    horizontal = []
    vertical = []
    fosor_text = ""
    section = None
    for i in range(len(df)):
        a_raw = _sanitize_cell(df.iloc[i].iloc[0]) if len(df.columns) > 0 else ""
        b_raw = _sanitize_cell(df.iloc[i].iloc[1]) if len(df.columns) > 1 else ""
        a = str(a_raw).strip() if not _is_empty(a_raw) else ""
        b = str(b_raw).strip() if not _is_empty(b_raw) else ""
        a_low = a.lower()
        if a == "FS" or "fősor" in a_low or "fosor" in a_low:
            fosor_text = b
            section = None
            continue
        if "vízs" in a_low or "vizs" in a_low or a == "Vízszintes":
            section = "H"
            continue
        if "függ" in a_low or "fugg" in a_low or a == "Függőleges":
            section = "V"
            continue
        if section and a and b:
            if section == "H":
                horizontal.append((a, b))
            elif section == "V":
                vertical.append((a, b))
    return {"horizontal": horizontal, "vertical": vertical, "fosor_text": fosor_text}


def store_definitions(conn, puzzle_id, horizontal, vertical, fosor_text):
    ensure_definitions_tables(conn)
    conn.execute("DELETE FROM definitions_horizontal WHERE puzzle_id = ?", (puzzle_id,))
    conn.execute("DELETE FROM definitions_vertical WHERE puzzle_id = ?", (puzzle_id,))
    conn.execute("DELETE FROM puzzle_fosor_text WHERE puzzle_id = ?", (puzzle_id,))
    for clue, definition in horizontal or []:
        conn.execute(
            "INSERT INTO definitions_horizontal (puzzle_id, clue, definition) VALUES (?, ?, ?)",
            (puzzle_id, str(clue), str(definition)),
        )
    for clue, definition in vertical or []:
        conn.execute(
            "INSERT INTO definitions_vertical (puzzle_id, clue, definition) VALUES (?, ?, ?)",
            (puzzle_id, str(clue), str(definition)),
        )
    if fosor_text:
        conn.execute(
            "INSERT OR REPLACE INTO puzzle_fosor_text (puzzle_id, solution_text) VALUES (?, ?)",
            (puzzle_id, str(fosor_text)),
        )
    conn.commit()


def load_definitions(conn, puzzle_id):
    ensure_definitions_tables(conn)
    h = [
        {"clue": row[0], "definition": row[1]}
        for row in conn.execute(
            "SELECT clue, definition FROM definitions_horizontal WHERE puzzle_id = ? ORDER BY id",
            (puzzle_id,),
        ).fetchall()
    ]
    v = [
        {"clue": row[0], "definition": row[1]}
        for row in conn.execute(
            "SELECT clue, definition FROM definitions_vertical WHERE puzzle_id = ? ORDER BY id",
            (puzzle_id,),
        ).fetchall()
    ]
    fosor_row = conn.execute(
        "SELECT solution_text FROM puzzle_fosor_text WHERE puzzle_id = ?", (puzzle_id,)
    ).fetchone()
    fosor_text = fosor_row[0] if fosor_row else ""
    return {"horizontal": h, "vertical": v, "main_row_text": fosor_text}


def load_blank_matrix(conn, id):
    (blob_blank, ) = conn.execute("SELECT blank FROM crosswow WHERE id=?", (id,)).fetchone()
    return pickle.loads(blob_blank)

def load_solution_matrix(conn, id):
    (blob_sol, ) = conn.execute("SELECT solution FROM crosswow WHERE id=?", (id,)).fetchone()
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "definitions_horizontal" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "puzzle_id" INTEGER NOT NULL,
        "clue" TEXT NOT NULL,
        "definition" TEXT NOT NULL
    )""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "definitions_vertical" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "puzzle_id" INTEGER NOT NULL,
        "clue" TEXT NOT NULL,
        "definition" TEXT NOT NULL
    )""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "puzzle_fosor_text" (
        "puzzle_id" INTEGER PRIMARY KEY,
        "solution_text" TEXT NOT NULL
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
    file_path = "Crosswow 2025 IPM-mod.xlsx"
    create_db('crosswow.db')

    xs = list_excel_sheets(file_path)
    print(xs)
    df_blank = read_sheet(file_path, xs[0])
    df_sol = read_sheet(file_path, xs[2])
    # print(test_get_cell(df_blank, 2, 2)) # 1
    # print(test_get_cell(df_blank, 0, 0)) # nan
    # print(test_get_cell(df_blank, 2, 3)) # semmi

    

    mx_blank = make_board_matrix(df_blank)
    mx_sol = make_board_matrix(df_sol)

    main_row = read_main_row_sequence(df_blank)
    n = len(main_row)
    if n:
        print('Main row (fősor) sequence:', main_row[:25], '...' if n > 25 else '')
        print('Stored', n, 'items in main_row_clues.')
    else:
        print('Main row (fősor): no data found. Check Excel row 34, columns L–AE on the Quiz sheet.')

    conn = create_conn('crosswow.db')
    ensure_main_row_clues_column(conn)
    ensure_definitions_tables(conn)
    update_matrix(conn, 1, mx_blank, mx_sol, main_row_clues=main_row)

    meghat = [s for s in xs if "eghat" in s.lower() or "meghat" in s.lower()]
    if meghat:
        df_def = read_sheet(file_path, meghat[0])
        defs = read_definitions_from_sheet(df_def)
        store_definitions(conn, 1, defs["horizontal"], defs["vertical"], defs.get("fosor_text") or "")
        print("Definitions: %d horizontal, %d vertical, fősor: %s" % (
            len(defs["horizontal"]), len(defs["vertical"]), "yes" if defs.get("fosor_text") else "no"))
    else:
        print("No 'Meghatározások' sheet found; definitions not imported.")

    print('Updated puzzle id=1 in DB. Refresh the browser to see the new board.')

    mx_blank_loaded = load_blank_matrix(conn, 1)
    print('------------')
    print(mx_blank_loaded)


    close_conn(conn)

    # print(mx[2][2])
    # print(mx[1][1])
    # print(mx)