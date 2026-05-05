"""
VESC Airgap Database Backend — Single Unified Table
v2: adds flux_linkage_initial_mwb, flux_linkage_final_mwb, flux_delta_mwb,
    flux_initial_ts, flux_final_ts
Run: python server.py
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3, datetime

app = Flask(__name__)
CORS(app)

DB_PATH = "airgap.db"

# ─── DB helpers ───────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def migrate_db(conn):
    """Add any missing columns to existing tables — safe to run on every startup."""
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(motor_records)").fetchall()}

    migrations = [
        ("rpm_per_volt",                "ALTER TABLE motor_records ADD COLUMN rpm_per_volt               REAL"),
        ("rpm_at_48v",                  "ALTER TABLE motor_records ADD COLUMN rpm_at_48v                 REAL"),
        ("tach_at_ramp",                "ALTER TABLE motor_records ADD COLUMN tach_at_ramp               INTEGER"),
        ("motor_notes",                 "ALTER TABLE motor_records ADD COLUMN motor_notes                TEXT"),
        ("tach_moves",                  "ALTER TABLE motor_records ADD COLUMN tach_moves                 TEXT"),
        # legacy single-measurement flux (kept for backward compat)
        ("flux_linkage_mwb",            "ALTER TABLE motor_records ADD COLUMN flux_linkage_mwb           REAL"),
        ("resistance_mohm",             "ALTER TABLE motor_records ADD COLUMN resistance_mohm            REAL"),
        ("inductance_uh",               "ALTER TABLE motor_records ADD COLUMN inductance_uh              REAL"),
        ("battery_current_a",           "ALTER TABLE motor_records ADD COLUMN battery_current_a          REAL"),
        ("duty_pct",                    "ALTER TABLE motor_records ADD COLUMN duty_pct                   REAL"),
        ("voltage_v",                   "ALTER TABLE motor_records ADD COLUMN voltage_v                  REAL"),
        ("rpm_at_95",                   "ALTER TABLE motor_records ADD COLUMN rpm_at_95                  INTEGER"),
        # ── NEW flux columns ──────────────────────────────────────────────────
        ("flux_linkage_initial_mwb",    "ALTER TABLE motor_records ADD COLUMN flux_linkage_initial_mwb  REAL"),
        ("flux_linkage_final_mwb",      "ALTER TABLE motor_records ADD COLUMN flux_linkage_final_mwb    REAL"),
        ("flux_delta_mwb",              "ALTER TABLE motor_records ADD COLUMN flux_delta_mwb            REAL"),
        ("flux_initial_ts",             "ALTER TABLE motor_records ADD COLUMN flux_initial_ts            TEXT"),
        ("flux_final_ts",               "ALTER TABLE motor_records ADD COLUMN flux_final_ts              TEXT"),
    ]

    for col_name, sql in migrations:
        if col_name not in existing_cols:
            try:
                conn.execute(sql)
                print(f"[migrate] Added column: {col_name}")
            except sqlite3.OperationalError as e:
                print(f"[migrate] Skipped {col_name}: {e}")

    conn.commit()

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS motor_records (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Motor identity
            motor_no                    TEXT    NOT NULL,
            motor_notes                 TEXT,

            -- Airgap adjustment
            tach_cumulative             INTEGER,
            tach_delta                  INTEGER,
            direction                   TEXT,
            tach_moves                  TEXT,

            -- Motor parameters (R, L)
            resistance_mohm             REAL,
            inductance_uh               REAL,

            -- Flux linkage — legacy (kept for backward compat, populated with final value)
            flux_linkage_mwb            REAL,

            -- Flux linkage — initial (before airgap adjustment)
            flux_linkage_initial_mwb    REAL,
            flux_initial_ts             TEXT,

            -- Flux linkage — final (after airgap adjustment)
            flux_linkage_final_mwb      REAL,
            flux_final_ts               TEXT,

            -- Flux delta (final − initial)
            flux_delta_mwb              REAL,

            -- Ramp test results at 95% duty
            motor_current_a             REAL,
            battery_current_a           REAL,
            duty_pct                    REAL,
            voltage_v                   REAL,
            rpm_at_95                   INTEGER,
            tach_at_ramp                INTEGER,
            rpm_per_volt                REAL,
            rpm_at_48v                  REAL,

            -- record_type: "registration" | "airgap" | "ramp" | "full"
            record_type                 TEXT    NOT NULL DEFAULT 'airgap',

            timestamp                   TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_flux_init  ON motor_records(flux_linkage_initial_mwb);
        CREATE INDEX IF NOT EXISTS idx_flux_final ON motor_records(flux_linkage_final_mwb);
        CREATE INDEX IF NOT EXISTS idx_flux       ON motor_records(flux_linkage_mwb);
        CREATE INDEX IF NOT EXISTS idx_motor      ON motor_records(motor_no);
    """)
    conn.commit()

    # Run migrations for existing DBs that may be missing newer columns
    migrate_db(conn)
    conn.close()
    print("Database ready:", DB_PATH)

# ─── Save record ──────────────────────────────────────────────────────────────
@app.route("/api/record", methods=["POST"])
def save_record():
    d = request.json
    if not d.get("motor_no"):
        return jsonify({"error": "motor_no required"}), 400

    tach_cumulative = d.get("tach_cumulative")
    tach_delta      = d.get("tach_delta")
    direction       = d.get("direction")
    tach_moves      = d.get("tach_moves")

    # flux_linkage_mwb legacy: prefer final, fall back to initial
    flux_legacy = d.get("flux_linkage_mwb") or d.get("flux_linkage_final_mwb") or d.get("flux_linkage_initial_mwb")

    conn = get_db()
    cur = conn.execute(
        """INSERT INTO motor_records
           (motor_no, motor_notes,
            tach_cumulative, tach_delta, direction, tach_moves,
            resistance_mohm, inductance_uh,
            flux_linkage_mwb,
            flux_linkage_initial_mwb, flux_initial_ts,
            flux_linkage_final_mwb,   flux_final_ts,
            flux_delta_mwb,
            motor_current_a, battery_current_a, duty_pct, voltage_v,
            rpm_at_95, tach_at_ramp,
            rpm_per_volt, rpm_at_48v,
            record_type, timestamp)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            d.get("motor_no"),
            d.get("motor_notes", ""),
            tach_cumulative,
            tach_delta,
            direction,
            tach_moves,
            d.get("resistance_mohm"),
            d.get("inductance_uh"),
            flux_legacy,
            d.get("flux_linkage_initial_mwb"),
            d.get("flux_initial_ts"),
            d.get("flux_linkage_final_mwb"),
            d.get("flux_final_ts"),
            d.get("flux_delta_mwb"),
            d.get("motor_current_a"),
            d.get("battery_current_a"),
            d.get("duty_pct"),
            d.get("voltage_v"),
            d.get("rpm_at_95"),
            d.get("tach_at_ramp"),
            d.get("rpm_per_volt"),
            d.get("rpm_at_48v"),
            d.get("record_type", "airgap"),
            datetime.datetime.now().isoformat()
        )
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return jsonify({"ok": True, "id": new_id})

# ─── Get records ──────────────────────────────────────────────────────────────
@app.route("/api/records", methods=["GET"])
def get_records():
    motor_no = request.args.get("motor_no", "").strip()
    limit    = request.args.get("limit", 300)
    conn = get_db()
    if motor_no:
        rows = conn.execute(
            "SELECT * FROM motor_records WHERE motor_no=? ORDER BY timestamp DESC",
            (motor_no,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM motor_records ORDER BY timestamp DESC LIMIT ?",
            (int(limit),)
        ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

# ─── Flux linkage auto-suggest (uses final flux if available, else initial) ───
@app.route("/api/flux-suggest", methods=["GET"])
def flux_suggest():
    try:
        flux = float(request.args.get("flux", 0))
    except Exception:
        return jsonify({"suggestions": []})
    tol = float(request.args.get("tol", 0.05))
    conn = get_db()
    # prefer final flux for matching; fall back to legacy
    rows = conn.execute(
        """SELECT motor_no, tach_cumulative, tach_delta, direction,
                  motor_current_a, voltage_v,
                  flux_linkage_mwb,
                  flux_linkage_initial_mwb,
                  flux_linkage_final_mwb,
                  flux_delta_mwb,
                  timestamp
           FROM motor_records
           WHERE (
               (flux_linkage_final_mwb IS NOT NULL AND ABS(flux_linkage_final_mwb - ?) <= ?)
               OR
               (flux_linkage_final_mwb IS NULL AND flux_linkage_mwb IS NOT NULL AND ABS(flux_linkage_mwb - ?) <= ?)
           )
           AND tach_cumulative IS NOT NULL
           ORDER BY timestamp DESC LIMIT 10""",
        (flux, tol, flux, tol)
    ).fetchall()
    conn.close()
    return jsonify({"flux": flux, "tolerance": tol, "suggestions": [dict(r) for r in rows]})

# ─── Delete record ────────────────────────────────────────────────────────────
@app.route("/api/record/<int:record_id>", methods=["DELETE"])
def delete_record(record_id):
    conn = get_db()
    conn.execute("DELETE FROM motor_records WHERE id=?", (record_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

# ─── Replace latest record for a motor ───────────────────────────────────────
@app.route("/api/record/replace", methods=["POST"])
def replace_record():
    d = request.json
    if not d.get("motor_no") or not d.get("record_type"):
        return jsonify({"error": "motor_no and record_type required"}), 400

    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM motor_records WHERE motor_no=? ORDER BY timestamp DESC LIMIT 1",
        (d.get("motor_no"),)
    ).fetchone()

    replaced_id = None
    if existing:
        replaced_id = existing["id"]
        conn.execute("DELETE FROM motor_records WHERE id=?", (replaced_id,))

    flux_legacy = d.get("flux_linkage_mwb") or d.get("flux_linkage_final_mwb") or d.get("flux_linkage_initial_mwb")

    cur = conn.execute(
        """INSERT INTO motor_records
           (motor_no, motor_notes,
            tach_cumulative, tach_delta, direction, tach_moves,
            resistance_mohm, inductance_uh,
            flux_linkage_mwb,
            flux_linkage_initial_mwb, flux_initial_ts,
            flux_linkage_final_mwb,   flux_final_ts,
            flux_delta_mwb,
            motor_current_a, battery_current_a, duty_pct, voltage_v,
            rpm_at_95, tach_at_ramp,
            rpm_per_volt, rpm_at_48v,
            record_type, timestamp)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            d.get("motor_no"), d.get("motor_notes", ""),
            d.get("tach_cumulative"), d.get("tach_delta"), d.get("direction"), d.get("tach_moves"),
            d.get("resistance_mohm"), d.get("inductance_uh"),
            flux_legacy,
            d.get("flux_linkage_initial_mwb"), d.get("flux_initial_ts"),
            d.get("flux_linkage_final_mwb"),   d.get("flux_final_ts"),
            d.get("flux_delta_mwb"),
            d.get("motor_current_a"), d.get("battery_current_a"),
            d.get("duty_pct"), d.get("voltage_v"),
            d.get("rpm_at_95"), d.get("tach_at_ramp"),
            d.get("rpm_per_volt"), d.get("rpm_at_48v"),
            d.get("record_type", "airgap"),
            datetime.datetime.now().isoformat()
        )
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return jsonify({"ok": True, "id": new_id, "replaced": replaced_id})

# ─── Motor stats ──────────────────────────────────────────────────────────────
@app.route("/api/stats/<motor_no>", methods=["GET"])
def motor_stats(motor_no):
    conn = get_db()
    row = conn.execute(
        """SELECT COUNT(*) as total_records,
                  MAX(tach_cumulative) as max_tach,
                  AVG(motor_current_a) as avg_current,
                  MAX(voltage_v) as max_voltage,
                  AVG(flux_linkage_initial_mwb) as avg_flux_initial,
                  AVG(flux_linkage_final_mwb) as avg_flux_final,
                  AVG(flux_delta_mwb) as avg_flux_delta,
                  MIN(timestamp) as first_seen,
                  MAX(timestamp) as last_seen
           FROM motor_records WHERE motor_no=?""",
        (motor_no,)
    ).fetchone()
    conn.close()
    return jsonify(dict(row) if row else {})

if __name__ == "__main__":
    init_db()
    print("Server running at http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)