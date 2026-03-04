from flask import Flask, g, jsonify, render_template
import sqlite3
import pickle
import math
import cw_read_excel as cwr

DB_PATH = "crosswow.db"

app = Flask(__name__)

# ---------- database helpers ----------

def get_db():
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        g.db = conn
    return g.db

@app.teardown_appcontext
def close_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

# ---------- helpers ----------

def cell_to_json(v):
    """Make a cell value JSON-serializable (nan/None -> null)."""
    if v is None or v == "":
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (int, str)):
        return v
    return str(v)


# ---------- routes ----------

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/api/puzzle/<int:matrix_id>", methods=["GET"])
def get_puzzle(matrix_id):
    conn = get_db()
    cwr.ensure_main_row_clues_column(conn)
    try:
        mx = cwr.load_blank_matrix(conn, matrix_id)
    except (TypeError, sqlite3.OperationalError):
        return jsonify({"error": "not found"}), 404
    if not mx:
        return jsonify({"id": matrix_id, "rows": 0, "cols": 0, "data": [], "main_row_clues": []})
    rows, cols = len(mx), max(len(r) for r in mx)
    data = []
    for row in mx:
        padded = list(row) + [None] * (cols - len(row))
        data.append([cell_to_json(c) for c in padded])
    # Send "" for empty/white cells so frontend doesn't treat them as blocked (null)
    data = [[(x if x is not None else "") for x in row] for row in data]
    main_row_clues = cwr.load_main_row_clues(conn, matrix_id)
    if main_row_clues is None:
        main_row_clues = []
    return jsonify({"id": matrix_id, "rows": rows, "cols": cols, "data": data, "main_row_clues": main_row_clues})


@app.route("/api/solution/<int:matrix_id>", methods=["GET"])
def get_solution(matrix_id):
    conn = get_db()
    try:
        blank_mx = cwr.load_blank_matrix(conn, matrix_id)
        sol_mx = cwr.load_solution_matrix(conn, matrix_id)
    except (TypeError, sqlite3.OperationalError):
        return jsonify({"error": "not found"}), 404
    # Force solution to exact same shape as puzzle (blank) so cells align 1:1
    blank_rows, blank_cols = len(blank_mx), max(len(r) for r in blank_mx) if blank_mx else 0
    sol_rows, sol_cols = len(sol_mx), max(len(r) for r in sol_mx) if sol_mx else 0
    # Trim or pad solution to match blank dimensions
    out = []
    for i in range(blank_rows):
        row = sol_mx[i] if i < sol_rows else []
        if not isinstance(row, (list, tuple)):
            row = []
        out_row = []
        for j in range(blank_cols):
            c = row[j] if j < len(row) else None
            out_row.append(cell_to_json(c))
        out.append(out_row)
    return jsonify({"id": matrix_id, "rows": blank_rows, "cols": blank_cols, "data": out})


@app.route("/api/puzzle/<int:matrix_id>/definitions", methods=["GET"])
def get_definitions(matrix_id):
    conn = get_db()
    cwr.ensure_definitions_tables(conn)
    try:
        out = cwr.load_definitions(conn, matrix_id)
    except (TypeError, sqlite3.OperationalError):
        return jsonify({"error": "not found"}), 404
    return jsonify(out)


# ---------- entry point ----------

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8089)
