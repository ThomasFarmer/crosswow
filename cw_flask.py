from flask import Flask, g, jsonify
import sqlite3
import pickle
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

# ---------- routes ----------

#@app.route("/matrix/<int:matrix_id>", methods=["GET"]) def get_matrix(matrix_id):
@app.route("/", methods=["GET"])
def get_matrix():
    conn = get_db()
    mx_blank_loaded = cwr.load_blank_matrix(conn, 1)
    return  mx_blank_loaded
    
    # row = conn.execute(
    #     "SELECT data FROM crosswow WHERE id=?",
    #     (matrix_id,)
    # ).fetchone()

    # if row is None:
    #     return jsonify({"error": "not found"}), 404

    # matrix = pickle.loads(row["data"])

    # return jsonify({
    #     "id": matrix_id,
    #     "rows": len(matrix),
    #     "cols": len(matrix[0]) if matrix else 0,
    #     "data": matrix
    # })

# ---------- entry point ----------

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8089)
