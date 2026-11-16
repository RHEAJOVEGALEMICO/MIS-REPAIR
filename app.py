from flask import Flask, render_template_string, request, session, redirect, url_for
from functools import wraps
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import os
import datetime
import sqlite3
import secrets
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(32)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

# ==============================================
# DATABASE (PostgreSQL, automatic SQLite fallback)
# ==============================================
DATABASE_URL = os.environ.get("DATABASE_URL")
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432"),
    "dbname": os.environ.get("DB_NAME", "it_service_system"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}


def _timestamp_to_str(value, cur):
    return value[:19] if value else None


psycopg2.extensions.register_type(
    psycopg2.extensions.new_type(psycopg2.extensions.PYDATETIME.values,
                                 "TIMESTAMP_STR", _timestamp_to_str))

DB_BACKEND = None
DB_SQLITE_PATH = os.environ.get("DB_SQLITE_PATH", "it_service_system.db")


def _adapt_sqlite_sql(sql):
    sql = sql.replace('%s', '?')
    sql = sql.replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT')
    sql = sql.replace("date_trunc('month', NOW() AT TIME ZONE 'Asia/Manila') - INTERVAL '11 months'",
                      "datetime('now','+8 hours','start of month','-11 months')")
    sql = sql.replace("NOW() AT TIME ZONE 'Asia/Manila'", "datetime('now','+8 hours')")
    sql = sql.replace("TO_CHAR(submitted_at, 'YYYY-MM')", "strftime('%Y-%m', submitted_at)")
    sql = sql.replace("TO_CHAR(submitted_at, 'YYYY')", "strftime('%Y', submitted_at)")
    return sql


class _SQLiteCursor:
    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, params=None):
        sql = _adapt_sqlite_sql(sql)
        if params is None:
            return self._cur.execute(sql)
        return self._cur.execute(sql, params)

    def fetchone(self): return self._cur.fetchone()
    def fetchall(self): return self._cur.fetchall()

    @property
    def rowcount(self): return self._cur.rowcount

    def close(self): return self._cur.close()


class _SQLiteConn:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self): return _SQLiteCursor(self._conn.cursor())
    def commit(self): return self._conn.commit()
    def rollback(self): return self._conn.rollback()
    def close(self): return self._conn.close()


def _detect_backend():
    global DB_BACKEND
    try:
        if DATABASE_URL:
            t = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
        else:
            t = psycopg2.connect(cursor_factory=psycopg2.extras.DictCursor, **DB_CONFIG)
        t.close()
        DB_BACKEND = 'postgres'
        print("[DB] Backend: PostgreSQL")
    except Exception as e:
        DB_BACKEND = 'sqlite'
        print(f"[DB] PostgreSQL unavailable ({e}) -> using SQLite file: {DB_SQLITE_PATH}")


def get_db():
    if DB_BACKEND == 'sqlite':
        try:
            conn = sqlite3.connect(DB_SQLITE_PATH)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            return _SQLiteConn(conn)
        except Exception as e:
            print(f"[DB] SQLite Error: {e}")
            return None
    try:
        if DATABASE_URL:
            return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
        return psycopg2.connect(cursor_factory=psycopg2.extras.DictCursor, **DB_CONFIG)
    except Exception as e:
        print(f"[DB] PostgreSQL Error: {e}")
        return None


def _add_column(cur, conn, table, coldef):
    """Add a column to an existing table. Returns True only if it was newly added."""
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False


def init_db():
    conn = get_db()
    if not conn:
        return
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'requestor',
        department TEXT,
        contact_number TEXT,
        is_active INTEGER DEFAULT 1,
        account_type TEXT,
        terms_accepted_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tickets (
        id SERIAL PRIMARY KEY,
        reference_no TEXT UNIQUE NOT NULL,
        requestor_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        department TEXT NOT NULL,
        contact_number TEXT NOT NULL,
        equipment_type TEXT NOT NULL,
        brand_model TEXT,
        serial_property_no TEXT,
        location_room TEXT NOT NULL,
        problem_description TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'medium',
        status TEXT NOT NULL DEFAULT 'pending_admin',
        assigned_to INTEGER REFERENCES users(id) ON DELETE SET NULL,
        scheduled_date TEXT,
        work_done TEXT,
        parts_replaced TEXT,
        resolution_notes TEXT,
        requestor_feedback TEXT,
        admin_approved INTEGER DEFAULT 0,
        staff_approved INTEGER DEFAULT 0,
        submitted_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Manila'),
        updated_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Manila'),
        completed_at TIMESTAMP,
        approved_at TIMESTAMP
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS status_history (
        id SERIAL PRIMARY KEY,
        ticket_id INTEGER NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
        status TEXT NOT NULL,
        notes TEXT,
        updated_by INTEGER NOT NULL REFERENCES users(id),
        updated_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        ticket_id INTEGER REFERENCES tickets(id) ON DELETE CASCADE,
        message TEXT NOT NULL,
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS csm_responses (
        id SERIAL PRIMARY KEY,
        ticket_id INTEGER NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
        client_type TEXT, sex TEXT, age TEXT, region TEXT, service_availed TEXT,
        cc1 TEXT, cc2 TEXT, cc3 TEXT,
        sqd0 TEXT, sqd1 TEXT, sqd2 TEXT, sqd3 TEXT, sqd4 TEXT,
        sqd5 TEXT, sqd6 TEXT, sqd7 TEXT, sqd8 TEXT,
        suggestions TEXT, email TEXT,
        submitted_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
    )""")
    conn.commit()

    # Migration for databases created by the older version
    _add_column(cur, conn, 'users', 'account_type TEXT')
    if _add_column(cur, conn, 'users', 'terms_accepted_at TIMESTAMP'):
        # users who registered before this update already saw the terms
        cur.execute("UPDATE users SET terms_accepted_at = created_at")
        conn.commit()
    conn.close()


def seed_admin():
    """Admin accounts can no longer self-register. Create the first one here."""
    conn = get_db()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
    if cur.fetchone()[0] > 0:
        conn.close()
        return
    email = (os.environ.get("ADMIN_EMAIL") or "admin@slsu.local").lower()
    pw = os.environ.get("ADMIN_PASSWORD")
    generated = False
    if not pw:
        pw = secrets.token_urlsafe(9)
        generated = True
    cur.execute("""INSERT INTO users (full_name, email, password_hash, role, department,
                   contact_number, account_type, terms_accepted_at)
                   VALUES (%s,%s,%s,'admin','MIS-ICT','-','Staff',%s)""",
                ("System Administrator", email, generate_password_hash(pw), ph_now_str()))
    conn.commit()
    conn.close()
    print("=" * 60)
    print(f"[ADMIN] Created admin account: {email}")
    if generated:
        print(f"[ADMIN] Temporary password: {pw}   (change ADMIN_PASSWORD env to set your own)")
    print("=" * 60)


# ==============================================
# PHILIPPINE TIME (UTC+8) -- single source of truth for all timestamps
# ==============================================
def ph_now():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))


def ph_now_str():
    return ph_now().strftime('%Y-%m-%d %H:%M:%S')


def generate_ref_no():
    year = ph_now().year
    prefix = f"REQ-{year}-"
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT reference_no FROM tickets WHERE reference_no LIKE %s ORDER BY reference_no DESC LIMIT 1",
                (prefix + '%',))
    row = cur.fetchone()
    conn.close()
    n = int(row[0].split('-')[-1]) + 1 if row else 1
    return f"{prefix}{n:05d}"


def dicts(rows):
    return [dict(r) for r in rows]


# ==============================================
# DECORATORS
# ==============================================
def login_required(f):
    @wraps(f)
    def wrap(*a, **k):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*a, **k)
    return wrap


def role_required(role, msg):
    def deco(f):
        @wraps(f)
        def wrap(*a, **k):
            if 'user_id' not in session:
                return redirect(url_for('login'))
            if session.get('role') != role:
                return msg, 403
            return f(*a, **k)
        return wrap
    return deco


staff_required = role_required('staff', "Access Denied - IT Staff Only")
admin_required = role_required('admin', "Access Denied - Admin Only")

STATUS_LABELS = {
    'pending_admin': 'Pending Admin Review',
    'assigned': 'Assigned - Staff Coming',
    'completed_pending_staff': 'Work Done - Staff Approve',
    'pending_requestor_feedback': 'Waiting Requestor Feedback',
    'pending_admin_final': 'Ready for Final Approval',
    'completed_approved': 'Completed & Approved',
}


@app.template_global()
def status_label(s):
    return STATUS_LABELS.get(s, (s or '').replace('_', ' ').title())


# ==============================================
# PAGE RENDERING (one responsive layout for every page)
# ==============================================
def render_page(tpl, title="MIS-ICT Service & Repair", **ctx):
    content = render_template_string(tpl, **ctx)
    notif = 0
    if session.get('user_id'):
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM notifications WHERE user_id=%s AND is_read=0", (session['user_id'],))
        notif = cur.fetchone()[0]
        conn.close()
    return render_template_string(LAYOUT, content=content, title=title, css=CSS, notif=notif)


CSS = """
:root{--g:#1e5631;--y:#d4a017}
*,*::before,*::after{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;font-family:'Segoe UI',Arial,sans-serif;background:#f1f4f0;color:#222;line-height:1.45}
img,svg{max-width:100%;height:auto}
a{color:var(--g)}
.hdr{background:var(--g);color:#fff;padding:.6rem 1rem;display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:.5rem}
.hdr .t{font-weight:700}.hdr .t small{display:block;font-weight:400;opacity:.85}
.hdr nav{display:flex;flex-wrap:wrap;gap:.4rem;align-items:center;font-size:.9rem}
.hdr nav a{color:#fff;text-decoration:none;background:rgba(255,255,255,.14);padding:.35rem .65rem;border-radius:6px}
.hdr .who{opacity:.9}.pill{background:var(--y);color:#fff;border-radius:10px;padding:0 .45rem;font-size:.75rem;margin-left:.2rem}
.container{max-width:1100px;margin:0 auto;padding:1rem}
.card{background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.12);padding:1.2rem;margin-bottom:1rem}
.narrow{max-width:520px;margin:1rem auto}
.brand{text-align:center;margin-bottom:.8rem}.brand h1{font-size:1.1rem;margin:0;color:var(--g)}.brand p{margin:.1rem 0}.brand h2{font-size:1rem;margin:.4rem 0 0}
h2,h3{color:var(--g)}
label{display:block;font-weight:600;margin:.7rem 0 .2rem}
label.inline,label.opt{display:flex;gap:.45rem;align-items:flex-start;font-weight:400;margin:.25rem 0}
input:not([type=checkbox]):not([type=radio]),select,textarea{width:100%;padding:.6rem;border:1px solid #bbb;border-radius:6px;font:inherit;font-size:16px;background:#fff}
input[readonly],textarea[readonly]{background:#eef1ee}
.btn,button{display:inline-block;background:var(--g);color:#fff;border:0;border-radius:8px;padding:.7rem 1.1rem;font:inherit;font-weight:700;cursor:pointer;text-decoration:none;text-align:center}
.btn.gold{background:var(--y)}.btn.red{background:#b3261e}.btn.full,form>button.full{width:100%;margin-top:1rem}
button:disabled{opacity:.5;cursor:not-allowed}
.alert{padding:.7rem;border-radius:6px;margin:.6rem 0}.alert.err{background:#fde8e6;color:#8a1c14}.alert.info{background:#fff3cd;color:#664d03}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:.8rem}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:.8rem;margin-bottom:1rem}
.stat{background:#fff;border-radius:10px;padding:.9rem;text-align:center;box-shadow:0 1px 4px rgba(0,0,0,.12)}.stat b{display:block;font-size:1.7rem;color:var(--g)}
.tablewrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
table{border-collapse:collapse;width:100%}th,td{padding:.55rem;border-bottom:1px solid #e3e6e3;text-align:left;vertical-align:top;font-size:.92rem}
th{background:#eaf1ea}
.badge{display:inline-block;padding:.15rem .55rem;border-radius:12px;font-size:.78rem;font-weight:700;background:#e5e5e5;white-space:nowrap}
.s-pending_admin{background:#fff3cd}.s-assigned{background:#cfe2ff}.s-completed_pending_staff{background:#e0cffc}
.s-pending_requestor_feedback{background:#ffe5d0}.s-pending_admin_final{background:#d1ecf1}.s-completed_approved{background:#d1e7dd}
.p-high{color:#b3261e}.p-medium{color:#b26a00}.p-low{color:#2e7d32}
.kv td:first-child{font-weight:700;width:34%;background:#f7f9f7}
.bars{display:flex;align-items:flex-end;gap:6px;height:170px;overflow-x:auto;padding-top:1rem}
.bar{flex:1 0 34px;display:flex;flex-direction:column;justify-content:flex-end;align-items:center;height:100%;background:none;color:#222;padding:0;font-weight:400;font-size:.8rem}
.bar i{display:block;width:100%;background:var(--g);border-radius:4px 4px 0 0;min-height:2px}
.notif{padding:.7rem;border-left:4px solid var(--y);background:#fffdf5;margin-bottom:.6rem;border-radius:4px}
.terms-box{max-height:55vh;overflow-y:auto;border:1px solid #ccc;padding:.8rem;border-radius:6px;background:#fafafa}
.terms-box h3{font-size:1rem;margin:.8rem 0 .2rem}
footer{text-align:center;color:#555;font-size:.8rem;padding:1.2rem .5rem}
/* CSM form */
.arta{border:1.5px solid #333;padding:.4rem .6rem;font-size:.8rem;float:right;text-align:left}
table.sqd td,table.sqd th{text-align:center;padding:.4rem .3rem;font-size:.82rem}table.sqd td:first-child,table.sqd th:first-child{text-align:left}
.qb{margin:.7rem 0}
@media(max-width:900px){.stats{grid-template-columns:1fr 1fr}.container{padding:.7rem}}
@media(max-width:640px){
 .grid2{grid-template-columns:1fr}
 .hdr{flex-direction:column;align-items:flex-start}
 .btn,button{width:100%}
 .card{padding:.9rem}.kv td:first-child{width:42%}
 .arta{float:none;margin-bottom:.5rem}
}
@media print{
 @page{size:A4 portrait;margin:8mm}
 *{-webkit-print-color-adjust:exact;print-color-adjust:exact}
 body{background:#fff;font-size:8pt;line-height:1.2}
 .hdr,footer,.no-print,.back,button{display:none!important}
 .container{max-width:100%;padding:0}
 .card{box-shadow:none;border:0;padding:0;margin:0}
 label{margin:.25rem 0 .1rem}label.opt{margin:.05rem 0}
 .qb{margin:.25rem 0;page-break-inside:avoid}
 h2,h3{margin:.2rem 0}
 input:not([type=checkbox]):not([type=radio]),textarea{padding:.1rem .3rem;font-size:8pt;border:1px solid #888}
 textarea{height:28px}
 table.sqd th,table.sqd td{padding:1px 2px;font-size:7pt}
 input[type=radio]{-webkit-appearance:radio;appearance:radio;opacity:1}
 input:disabled,textarea[readonly],input[readonly]{opacity:1;background:#fff}
 .grid2{grid-template-columns:1fr 1fr}
}
"""

LAYOUT = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{title}}</title><style>{{css|safe}}</style></head><body>
<header class="hdr">
 <div class="t">SOUTHERN LUZON STATE UNIVERSITY<small>JGE &ndash; Tagkawayan Campus &middot; MIS ICT System &amp; Repair</small></div>
 {% if session.get('user_id') %}
 <nav>
  <a href="{{url_for('dashboard')}}">Dashboard</a>
  {% if session.role == 'admin' %}<a href="{{url_for('admin_reports')}}">Staff Reports</a><a href="{{url_for('admin_staff')}}">IT Staff</a>{% endif %}
  {% if session.role == 'requestor' %}<a href="{{url_for('new_ticket')}}">+ New Request</a>{% endif %}
  <a href="{{url_for('notifications')}}">Notifications{% if notif %}<span class="pill">{{notif}}</span>{% endif %}</a>
  <span class="who">{{session.role|upper}} &middot; {{session.full_name}}</span>
  <a href="{{url_for('logout')}}">Logout</a>
 </nav>
 {% endif %}
</header>
<main class="container">{{content|safe}}</main>
<footer>SOUTHERN LUZON STATE UNIVERSITY &mdash; JGE Tagkawayan Campus<br>Brgy. Rizal, Tagkawayan, Quezon &middot; MIS ICT System &amp; Repair</footer>
</body></html>"""


# ==============================================
# AUTH: REGISTER -> TERMS (once) -> LOGIN
# ==============================================
ACCOUNT_TYPES = ('Faculty', 'Staff', 'Others')

REGISTER_T = """
<div class="card narrow">
 <div class="brand"><h1>SOUTHERN LUZON STATE UNIVERSITY</h1><p>JGE &ndash; Tagkawayan Campus</p><h2>Create New Account</h2></div>
 {% if error %}<div class="alert err">{{error|safe}}</div>{% endif %}
 <form method="post">
  <div class="grid2">
   <div><label>Last Name *</label><input name="last_name" value="{{f.get('last_name','')}}" required></div>
   <div><label>First Name *</label><input name="first_name" value="{{f.get('first_name','')}}" required></div>
  </div>
  <label>Middle Name (Optional)</label><input name="middle_name" value="{{f.get('middle_name','')}}">
  <label>Email Address *</label><input type="email" name="email" value="{{f.get('email','')}}" required>
  <label>Account Type *</label>
  <select name="account_type" required>
   <option value="">-- Select Account Type --</option>
   {% for a in types %}<option value="{{a}}" {{'selected' if f.get('account_type')==a}}>{{a}}</option>{% endfor %}
  </select>
  <label>Department / Office *</label><input name="department" value="{{f.get('department','')}}" required>
  <label>Contact Number *</label><input name="contact_number" value="{{f.get('contact_number','')}}" required>
  <label>Password *</label><input type="password" name="password" minlength="6" required>
  <button class="full">Register Account</button>
 </form>
 <p>Already have an account? <a href="/login">Login here</a></p>
</div>"""


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        return render_page(REGISTER_T, "Register", f={}, types=ACCOUNT_TYPES, error=None)

    d = request.form
    for k in ['last_name', 'first_name', 'email', 'password', 'account_type', 'department', 'contact_number']:
        if not d.get(k, '').strip():
            return render_page(REGISTER_T, "Register", f=d, types=ACCOUNT_TYPES,
                               error="Please fill in all required fields.")
    if d.get('account_type') not in ACCOUNT_TYPES:
        return render_page(REGISTER_T, "Register", f=d, types=ACCOUNT_TYPES, error="Invalid account type.")

    full_name = ' '.join(p for p in (d.get('first_name', '').strip(), d.get('middle_name', '').strip(),
                                     d.get('last_name', '').strip()) if p)
    email = d.get('email').strip().lower()

    conn = get_db()
    cur = conn.cursor()
    try:
        # discard an abandoned (terms never accepted) registration with the same email
        cur.execute("DELETE FROM users WHERE email = %s AND terms_accepted_at IS NULL", (email,))
        # Everyone who self-registers is a requestor. Admin/IT staff are created by the admin.
        cur.execute("""INSERT INTO users (full_name,email,password_hash,role,department,contact_number,account_type)
                       VALUES (%s,%s,%s,'requestor',%s,%s,%s)""",
                    (full_name, email, generate_password_hash(d.get('password')),
                     d.get('department').strip(), d.get('contact_number').strip(), d.get('account_type')))
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        uid = cur.fetchone()[0]
        conn.commit()
        session.clear()
        session['pending_user_id'] = uid
        session['pending_email'] = email
        return redirect(url_for('terms'))
    except (psycopg2.IntegrityError, sqlite3.IntegrityError):
        conn.rollback()
        return render_page(REGISTER_T, "Register", f=d, types=ACCOUNT_TYPES,
                           error="Email already registered! <a href='/login'>Login instead</a>")
    except Exception as e:
        conn.rollback()
        return render_page(REGISTER_T, "Register", f=d, types=ACCOUNT_TYPES, error=f"Error: {e}")
    finally:
        conn.close()


TERMS_T = """
<div class="card" style="max-width:820px;margin:1rem auto">
 <div class="brand"><h1>SOUTHERN LUZON STATE UNIVERSITY</h1><p>JGE &ndash; Tagkawayan Campus &middot; MIS-ICT Service &amp; Repair System</p><h2>TERMS &amp; CONDITIONS</h2></div>
 <div class="terms-box">
 <h3>1. Acceptance of Terms</h3><p>By registering and using this MIS-ICT Service &amp; Repair System, you ("the Requisitioner") agree to be bound by these Terms &amp; Conditions. If you do not agree, click <b>I Disagree</b>; your registration will be cancelled and you may not use the system.</p>
 <h3>2. Purpose of the System</h3><p>This system is provided solely for requesting ICT / equipment repair and maintenance services of the Southern Luzon State University &ndash; JGE Tagkawayan Campus. It may not be used for personal, non-university, or fraudulent requests.</p>
 <h3>3. Accurate Information</h3><p>You must provide complete, current, and truthful information, including your name, department, contact number, equipment details, location/room, and an accurate description of the problem. Providing false information may result in rejection of your request and appropriate university action.</p>
 <h3>4. Scope of Service</h3><p>Services cover repair and maintenance of university-owned ICT equipment on campus, during office hours, subject to availability of IT staff and spare parts. The University does not guarantee repair of equipment that is beyond economical repair or obsolete.</p>
 <h3>5. Scheduling &amp; Access</h3><p>Once assigned, IT staff may visit your room or office at the scheduled date and time. A responsible person must be present to grant access. Equipment may be picked up for repair in the office and returned thereafter; you will be notified of the schedule.</p>
 <h3>6. Data &amp; Backup</h3><p>You are responsible for backing up all data stored in your equipment before repair. The University and its IT staff shall not be liable for any loss of data, programs, or files, nor for pre-existing damage or defects not caused by the repair.</p>
 <h3>7. Data Privacy</h3><p>Personal information collected through this system is used solely for processing your service request and is protected under the <b>Data Privacy Act of 2012 (Republic Act 10173)</b>. Your information will not be shared with unauthorized parties.</p>
 <h3>8. Client Satisfaction Measurement (CSM)</h3><p>After completion of the service, you agree to receive and may answer the Client Satisfaction Measurement (CSM) feedback form. Your responses help improve the quality of ICT services and are kept confidential.</p>
 <h3>9. Acceptable Use &amp; Conduct</h3><p>You shall treat IT staff with courtesy and respect. You shall not submit frivolous, duplicate, or malicious requests, nor misuse any feature of the system. Violations may be reported to proper university authorities.</p>
 <h3>10. Limitation of Liability</h3><p>Services are provided on an "as-is, as-available" basis. The University shall not be liable for delays caused by unavailability of spare parts, force majeure, natural calamities, or your own failure to grant access or provide accurate information.</p>
 <h3>11. Amendments</h3><p>The University reserves the right to update or modify these Terms &amp; Conditions at any time. Continued use of the system after any amendment constitutes acceptance of the revised terms.</p>
 <h3>12. Contact</h3><p>For questions or concerns, contact the MIS-ICT Office, SLSU &ndash; JGE Tagkawayan Campus, Brgy. Rizal, Tagkawayan, Quezon.</p>
 </div>
 <form method="post">
  <label class="inline"><input type="checkbox" id="ok" onchange="a.disabled=!this.checked"> I have read and understood the Terms &amp; Conditions above.</label>
  <div class="grid2" style="margin-top:.8rem">
   <button id="a" name="action" value="agree" disabled>I Agree</button>
   <button class="red btn" name="action" value="disagree" formnovalidate>I Disagree</button>
  </div>
  <p style="font-size:.85rem">If you disagree, your registration will be cancelled.</p>
 </form>
</div>"""


@app.route('/terms', methods=['GET', 'POST'])
def terms():
    """Shown ONLY right after a new account is registered (once per account)."""
    uid = session.get('pending_user_id')
    if not uid:
        return redirect(url_for('login'))
    if request.method == 'POST':
        conn = get_db()
        cur = conn.cursor()
        if request.form.get('action') == 'agree':
            cur.execute("UPDATE users SET terms_accepted_at = %s WHERE id = %s AND terms_accepted_at IS NULL",
                        (ph_now_str(), uid))
            conn.commit()
            conn.close()
            email = session.get('pending_email', '')
            session.clear()
            return redirect(url_for('login', email=email, registered=1))
        cur.execute("DELETE FROM users WHERE id = %s AND terms_accepted_at IS NULL", (uid,))
        conn.commit()
        conn.close()
        session.clear()
        return redirect(url_for('login'))
    return render_page(TERMS_T, "Terms & Conditions")


LOGIN_T = """
<div class="card narrow">
 <div class="brand"><h1>SOUTHERN LUZON STATE UNIVERSITY</h1><p>JGE &ndash; Tagkawayan Campus</p><h2>MIS-ICT SERVICE &amp; REPAIR SYSTEM</h2></div>
 {% if registered %}<div class="alert info">Account created. You may now log in.</div>{% endif %}
 {% if error %}<div class="alert err">{{error}}</div>{% endif %}
 <form method="post">
  <label>Email Address</label><input type="email" name="email" value="{{email}}" required>
  <label>Password</label><input type="password" name="password" id="pw" required>
  <label class="inline"><input type="checkbox" onclick="document.getElementById('pw').type=this.checked?'text':'password'"> Show Password</label>
  <button class="full">Login</button>
 </form>
 <p>Don't have an account? <a href="/register">Register here</a></p>
</div>"""


@app.route('/')
def index():
    return redirect(url_for('dashboard' if 'user_id' in session else 'login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_page(LOGIN_T, "Login", email=request.args.get('email', ''),
                           registered=request.args.get('registered'), error=None)
    email = request.form.get('email', '').strip().lower()
    password = request.form.get('password', '')
    conn = get_db()
    if not conn:
        return render_page(LOGIN_T, "Login", email=email, registered=None,
                           error="Database unavailable. Please try again later.")
    cur = conn.cursor()
    cur.execute("SELECT id, full_name, password_hash, role, is_active, terms_accepted_at FROM users WHERE email = %s",
                (email,))
    u = cur.fetchone()
    conn.close()

    def err(m):
        return render_page(LOGIN_T, "Login", email=email, registered=None, error=m)

    if not u:
        return err("Email not yet registered. Please create an account first.")
    if not check_password_hash(u[2], password):
        return err("Incorrect password.")
    if not u[4]:
        return err("Account is deactivated. Contact Admin.")
    if not u[5]:
        return err("Registration was not completed. Please register again.")
    session.clear()
    session['user_id'], session['full_name'], session['role'] = u[0], u[1], u[3]
    return redirect(url_for('dashboard'))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ==============================================
# DASHBOARD
# ==============================================
DASH_T = """
<div class="card"><h2 style="margin-top:0">{{welcome}}</h2>
 <p>Logged in as: <b>{{role|title}}</b></p>
 {% if role == 'requestor' %}<a class="btn" href="{{url_for('new_ticket')}}">Submit New Repair Request</a>{% endif %}</div>
<div class="stats">
 <div class="stat"><b>{{stats.total}}</b>Total Requests</div><div class="stat"><b>{{stats.open}}</b>In Progress</div>
 <div class="stat"><b>{{stats.completed}}</b>Completed</div><div class="stat"><b>{{stats.this_month}}</b>This Month</div>
</div>
{% if role == 'admin' %}
<div class="card"><h3 style="margin-top:0">Monthly Requests &mdash; Last 12 Months</h3>
 <small>Tap a bar to view the equipment breakdown.</small>
 <div class="bars">{% for m in monthly_data %}
  <button type="button" class="bar" onclick="showEq({{loop.index0}})"><span>{{m.count}}</span><i style="height:{{(m.count/max_count*100)|round|int}}%"></i><span>{{m.label}}</span></button>
 {% endfor %}</div>
 <div id="eq" style="display:none;margin-top:.8rem"><b id="eqt"></b><div class="tablewrap"><table id="eqb"></table></div></div>
</div>
<script>
const EQ={{monthly_equipment|tojson}};
function showEq(i){const d=EQ[i];document.getElementById('eq').style.display='block';
 document.getElementById('eqt').textContent=d.label+' \u2014 '+d.total+' request(s)';
 const t=document.getElementById('eqb');t.innerHTML='';
 if(!d.items.length){t.innerHTML='<tr><td>No requests this month.</td></tr>';return;}
 d.items.forEach(x=>{const r=t.insertRow();r.insertCell().textContent=x.type;r.insertCell().textContent=x.count;});}
</script>
{% endif %}
<div class="card"><h3 style="margin-top:0">{% if role=='requestor' %}My Requests{% elif role=='admin' %}All Requests &mdash; Manage &amp; Approve{% else %}Assigned to Me{% endif %}</h3>
{% if tickets %}<div class="tablewrap"><table>
 <tr><th>Reference No.</th><th>Equipment</th>{% if role!='requestor' %}<th>Requestor / Dept</th>{% endif %}<th>Status</th><th>Priority</th>{% if role!='requestor' %}<th>Assigned To</th>{% endif %}<th>Date</th><th></th></tr>
 {% for t in tickets %}<tr>
  <td><b>{{t.reference_no}}</b></td><td>{{t.equipment_type}}</td>
  {% if role!='requestor' %}<td>{{t.requestor_name}}<br><small>{{t.department}}</small></td>{% endif %}
  <td><span class="badge s-{{t.status}}">{{status_label(t.status)}}</span></td>
  <td class="p-{{t.priority}}">&#9679; {{t.priority|title}}</td>
  {% if role!='requestor' %}<td>{{t.assigned_name or 'Not assigned'}}</td>{% endif %}
  <td>{{t.submitted_at[:10]}}</td><td><a href="/tickets/{{t.id}}">View</a></td></tr>{% endfor %}
</table></div>
{% else %}<p>{% if role=='requestor' %}No requests yet. Click "Submit New Repair Request" to create one.{% elif role=='admin' %}No requests submitted yet.{% else %}No tasks assigned to you yet.{% endif %}</p>{% endif %}
</div>"""


@app.route('/dashboard')
@login_required
def dashboard():
    conn = get_db()
    cur = conn.cursor()
    role, uid = session.get('role'), session['user_id']

    if role == 'requestor':
        cur.execute("""SELECT id, reference_no, equipment_type, status, priority, submitted_at
                       FROM tickets WHERE requestor_id = %s ORDER BY submitted_at DESC""", (uid,))
    else:
        where, params = ("", ())
        if role == 'staff':
            where, params = "WHERE t.assigned_to = %s", (uid,)
        cur.execute(f"""SELECT t.id, t.reference_no, t.equipment_type, t.department, t.status, t.priority,
                        t.submitted_at, u.full_name AS requestor_name, ass.full_name AS assigned_name
                        FROM tickets t JOIN users u ON t.requestor_id = u.id
                        LEFT JOIN users ass ON t.assigned_to = ass.id
                        {where} ORDER BY t.submitted_at DESC""", params)
    tickets = dicts(cur.fetchall())

    base_where, fp = "WHERE 1=1", []
    if role == 'requestor':
        base_where += " AND requestor_id = %s"; fp = [uid]
    elif role == 'staff':
        base_where += " AND assigned_to = %s"; fp = [uid]

    cur.execute(f"""SELECT TO_CHAR(submitted_at, 'YYYY-MM') AS ym, COUNT(*) AS cnt FROM tickets {base_where}
                    AND submitted_at >= date_trunc('month', NOW() AT TIME ZONE 'Asia/Manila') - INTERVAL '11 months'
                    GROUP BY ym ORDER BY ym""", fp)
    monthly_map = {r[0]: r[1] for r in cur.fetchall()}

    today = ph_now()
    months = []
    for back in range(11, -1, -1):
        m_idx = today.month - back
        yy, mm = today.year + (m_idx - 1) // 12, (m_idx - 1) % 12 + 1
        months.append((yy, mm, f"{yy:04d}-{mm:02d}"))
    monthly_data = [{'label': datetime.date(y, m, 1).strftime('%b'), 'count': monthly_map.get(ym, 0)}
                    for y, m, ym in months]
    max_count = max([m['count'] for m in monthly_data] + [1])

    monthly_equipment = []
    if role == 'admin':
        cur.execute("""SELECT TO_CHAR(submitted_at, 'YYYY-MM') AS ym, equipment_type FROM tickets
                       WHERE submitted_at >= date_trunc('month', NOW() AT TIME ZONE 'Asia/Manila') - INTERVAL '11 months'""")
        per = {}
        for r in cur.fetchall():
            bucket = per.setdefault(r[0], {})
            for item in (r[1] or '').split(', '):   # one request can have several complaints
                item = item.strip()
                if item:
                    bucket[item] = bucket.get(item, 0) + 1
        for y, m, ym in months:
            b = per.get(ym, {})
            items = [{'type': k, 'count': v} for k, v in sorted(b.items(), key=lambda x: -x[1])]
            monthly_equipment.append({'label': datetime.date(y, m, 1).strftime('%B %Y'),
                                      'items': items, 'total': sum(b.values())})

    cur.execute(f"SELECT status, COUNT(*) FROM tickets {base_where} GROUP BY status", fp)
    sc = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    stats = {'total': sum(sc.values()),
             'open': sum(v for k, v in sc.items() if k != 'completed_approved'),
             'completed': sc.get('completed_approved', 0),
             'this_month': monthly_data[-1]['count']}
    welcome = {'admin': "Welcome, Admin! Review requests, assign to Staff, and approve completed work.",
               'staff': "Welcome, Staff! See tasks assigned to you, report work done, then approve.",
               'requestor': "Welcome! Submit repair requests, track status, and give feedback once fixed."}.get(role, "Welcome!")
    return render_page(DASH_T, "Dashboard", welcome=welcome, tickets=tickets, monthly_data=monthly_data,
                       max_count=max_count, stats=stats, monthly_equipment=monthly_equipment, role=role)


# ==============================================
# NEW REQUEST (Service Assistance Form) -- date/time recorded by the server
# ==============================================
SAF_COMPLAINTS = ["Internet Connection", "Printer", "SEEDS Connection", "Video Setup",
                  "Computer Hardware", "Desktop Publishing", "Software Installation"]

NEW_T = """
<a class="back" href="/dashboard">&larr; Back to Dashboard</a>
<div class="card" style="margin-top:.6rem">
 <div class="brand"><h1>SOUTHERN LUZON STATE UNIVERSITY</h1><p>JGE &ndash; Tagkawayan Campus</p><h2>SERVICE ASSISTANCE FORM</h2><p>(Maintenance &amp; Repair Purposes)</p>
 <p><small>SAF No. (YY:MM:SRN) &mdash; auto-assigned on submit</small></p></div>
 {% if error %}<div class="alert err">{{error}}</div>{% endif %}
 <form method="post">
  <div class="grid2">
   <div><label>Date Request (automatic)</label><input id="d" value="{{now[:10]}}" readonly></div>
   <div><label>Time (automatic, PH time)</label><input id="t" value="{{now[11:]}}" readonly></div>
  </div>
  <div class="grid2">
   <div><label>Requisitioner Office *</label><input name="department" value="{{f.get('department','')}}" required></div>
   <div><label>Contact Number *</label><input name="contact_number" value="{{f.get('contact_number','')}}" required></div>
  </div>
  <label>Requisitioner (Full Name)</label><input value="{{session.full_name}}" readonly>
  <label>Name of Complaint / Service Needed * (check all that apply)</label>
  <div class="grid2">
   {% for c in complaints %}<label class="opt"><input type="checkbox" name="equipment_type" value="{{c}}" {{'checked' if c in f.getlist('equipment_type')}}> {{c}}</label>{% endfor %}
   <label class="opt"><input type="checkbox" name="equipment_type" value="Other" {{'checked' if 'Other' in f.getlist('equipment_type')}}> Other:</label>
   <input name="other_text" placeholder="Specify other" value="{{f.get('other_text','')}}">
  </div>
  <label>Problem Reported *</label><textarea name="problem_description" rows="4" required>{{f.get('problem_description','')}}</textarea>
  <div class="grid2">
   <div><label>Brand / Model</label><input name="brand_model" value="{{f.get('brand_model','')}}"></div>
   <div><label>Serial / Property No.</label><input name="serial_property_no" value="{{f.get('serial_property_no','')}}"></div>
  </div>
  <label>Location / Room No. *</label><input name="location_room" value="{{f.get('location_room','')}}" required>
  <label>Priority Level *</label>
  <label class="opt"><input type="radio" name="priority" value="low" required> Low &mdash; Not urgent</label>
  <label class="opt"><input type="radio" name="priority" value="medium" checked> Medium &mdash; Needs attention</label>
  <label class="opt"><input type="radio" name="priority" value="high"> High &mdash; Affecting work / classes!</label>
  <button class="full">Submit Request to Admin</button>
 </form>
</div>
<script>
// Display only: ticks in Philippine time. The saved date/time always comes from the server.
const base=new Date("{{iso}}").getTime(),t0=performance.now();
setInterval(()=>{const x=new Date(base+performance.now()-t0),o={timeZone:'Asia/Manila'};
 document.getElementById('d').value=x.toLocaleDateString('en-CA',o);
 document.getElementById('t').value=x.toLocaleTimeString('en-GB',o);},1000);
</script>"""


@app.route('/tickets/new', methods=['GET', 'POST'])
@login_required
def new_ticket():
    if session['role'] != 'requestor':
        return "Only Requestors can submit new requests", 403

    def form(f, error=None):
        n = ph_now()
        return render_page(NEW_T, "New Request", f=f, error=error, complaints=SAF_COMPLAINTS,
                           now=n.strftime('%Y-%m-%d %H:%M:%S'), iso=n.isoformat())

    if request.method == 'GET':
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT department, contact_number FROM users WHERE id = %s", (session['user_id'],))
        u = cur.fetchone()
        conn.close()
        from werkzeug.datastructures import MultiDict
        return form(MultiDict({'department': u[0] or '', 'contact_number': u[1] or ''}))

    d = request.form
    items = d.getlist('equipment_type')
    if 'Other' in items:
        items = [i for i in items if i != 'Other'] + [d.get('other_text', '').strip() or 'Other']
    equipment = ', '.join(items)
    if (not equipment or not d.get('department', '').strip() or not d.get('contact_number', '').strip()
            or not d.get('location_room', '').strip() or not d.get('problem_description', '').strip()
            or d.get('priority') not in ('low', 'medium', 'high')):
        return form(d, "Please complete all required fields and select at least one service needed.")

    ref_no = generate_ref_no()
    stamp = ph_now_str()   # server-side, Philippine time
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""INSERT INTO tickets (reference_no, requestor_id, department, contact_number,
            equipment_type, brand_model, serial_property_no, location_room, problem_description,
            priority, status, submitted_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending_admin',%s,%s)""",
                    (ref_no, session['user_id'], d.get('department').strip(), d.get('contact_number').strip(),
                     equipment, d.get('brand_model', '').strip(), d.get('serial_property_no', '').strip(),
                     d.get('location_room').strip(), d.get('problem_description').strip(),
                     d.get('priority'), stamp, stamp))
        cur.execute("SELECT id FROM tickets WHERE reference_no = %s", (ref_no,))
        tid = cur.fetchone()[0]
        cur.execute("""INSERT INTO status_history (ticket_id,status,notes,updated_by,updated_at)
                       VALUES (%s,'pending_admin','Request submitted - waiting for Admin review',%s,%s)""",
                    (tid, session['user_id'], stamp))
        cur.execute("SELECT id FROM users WHERE role = 'admin'")
        for a in cur.fetchall():
            cur.execute("INSERT INTO notifications (user_id,ticket_id,message,created_at) VALUES (%s,%s,%s,%s)",
                        (a[0], tid, f"New request: {ref_no} - needs assignment", stamp))
        conn.commit()
        return redirect(url_for('ticket_detail', ticket_id=tid))
    except Exception as e:
        conn.rollback()
        return form(d, f"Error: {e}")
    finally:
        conn.close()


# ==============================================
# TICKET DETAIL + WORKFLOW
# ==============================================
DETAIL_T = """
<a class="back" href="/dashboard">&larr; Back to Dashboard</a>
<div class="card" style="margin-top:.6rem">
 <div style="display:flex;flex-wrap:wrap;gap:.6rem;justify-content:space-between;align-items:center">
  <h2 style="margin:0">Ticket &mdash; {{t.reference_no}}</h2>
  <span class="badge s-{{t.status}}">{{status_label(t.status)}}</span></div>
 <p><a class="btn gold" target="_blank" href="/tickets/{{t.id}}/print">Print Service Assistance Form</a></p>
 <h3>Request Details</h3>
 <div class="tablewrap"><table class="kv">
  <tr><td>Requisitioner</td><td>{{t.requestor_name}}{% if t.requestor_type %} ({{t.requestor_type}}){% endif %}</td></tr>
  <tr><td>Department</td><td>{{t.department}}</td></tr><tr><td>Contact Number</td><td>{{t.contact_number}}</td></tr>
  <tr><td>Complaint / Service</td><td>{{t.equipment_type}}</td></tr>
  {% if t.brand_model %}<tr><td>Brand / Model</td><td>{{t.brand_model}}</td></tr>{% endif %}
  {% if t.serial_property_no %}<tr><td>Serial / Property No</td><td>{{t.serial_property_no}}</td></tr>{% endif %}
  <tr><td>Location / Room</td><td>{{t.location_room}}</td></tr>
  <tr><td>Priority</td><td class="p-{{t.priority}}">&#9679; {{t.priority|title}}</td></tr>
  {% if t.assigned_name %}<tr><td>Assigned To</td><td>{{t.assigned_name}}</td></tr>{% endif %}
  {% if t.scheduled_date %}<tr><td>Scheduled Date</td><td>{{t.scheduled_date}}</td></tr>{% endif %}
  <tr><td>Problem Description</td><td>{{t.problem_description}}</td></tr>
  <tr><td>Date / Time Submitted</td><td>{{t.submitted_at}}</td></tr>
 </table></div>
 {% if t.work_done %}<h3>Work Report</h3><div class="tablewrap"><table class="kv">
  <tr><td>Work Done</td><td>{{t.work_done}}</td></tr>
  {% if t.parts_replaced %}<tr><td>Parts Replaced / Used</td><td>{{t.parts_replaced}}</td></tr>{% endif %}
  {% if t.resolution_notes %}<tr><td>Remarks</td><td>{{t.resolution_notes}}</td></tr>{% endif %}</table></div>{% endif %}
 {% if t.requestor_feedback %}<h3>Feedback from Requisitioner</h3>
  <p><a class="btn" href="/tickets/{{t.id}}/csm/view">CSM Form Submitted &mdash; Click to View</a></p>{% endif %}
 {% if t.admin_approved == 1 %}<div class="alert info"><b>FULLY APPROVED &amp; CLOSED</b></div>{% endif %}
</div>

{% if role=='admin' and t.status=='pending_admin' %}<div class="card"><h3 style="margin-top:0">Assign to Staff</h3>
 <form method="post" action="/tickets/{{t.id}}/assign">
  <label>Select Staff *</label><select name="assigned_to" required><option value="">-- Choose Staff --</option>
   {% for s in staff_list %}<option value="{{s.id}}">{{s.full_name}}</option>{% endfor %}</select>
  <label>Scheduled Date</label><input type="date" name="scheduled_date">
  <button class="full">Assign Staff</button></form></div>{% endif %}

{% if role=='staff' and t.status=='assigned' %}<div class="card"><h3 style="margin-top:0">Submit Work Report</h3>
 <form method="post" action="/tickets/{{t.id}}/submit-work">
  <label>What Work Was Done? *</label><textarea name="work_done" rows="3" required></textarea>
  <label>Parts Replaced / Used</label><textarea name="parts_replaced" rows="2"></textarea>
  <label>Additional Notes / Remarks</label><textarea name="resolution_notes" rows="2"></textarea>
  <button class="full">Submit Work Report</button></form></div>{% endif %}

{% if role=='staff' and t.status=='completed_pending_staff' %}<div class="card"><h3 style="margin-top:0">Approve Work</h3>
 <p>Review the work report above. Confirm it is complete.</p>
 <form method="post" action="/tickets/{{t.id}}/staff-approve"><button>Approve &amp; Notify Requestor</button></form></div>{% endif %}

{% if role=='requestor' and t.status=='pending_requestor_feedback' %}<div class="card"><h3 style="margin-top:0">Client Satisfaction Measurement (CSM) Form</h3>
 <p>The work has been completed. Please fill out the ARTA Client Satisfaction Measurement Form. It only takes a minute. Thank you!</p>
 <a class="btn" href="/tickets/{{t.id}}/csm">Fill Out CSM Form &rarr;</a></div>{% endif %}

{% if role=='admin' and t.status=='pending_admin_final' %}<div class="card"><h3 style="margin-top:0">Final Approval</h3>
 <p>Review work and feedback above, then close this request.</p>
 <form method="post" action="/tickets/{{t.id}}/admin-approve"><button>Approve &amp; Close Request</button></form></div>{% endif %}

{% if t.status=='completed_approved' %}<div class="card"><h3 style="margin-top:0">REQUEST COMPLETED</h3><p>This request has been fully completed and approved. Thank you!</p></div>{% endif %}

<div class="card"><h3 style="margin-top:0">Status History</h3><div class="tablewrap"><table>
 <tr><th>#</th><th>Status</th><th>Updated By</th><th>Date &amp; Time</th><th>Notes</th></tr>
 {% for h in history %}<tr><td>{{loop.index}}</td><td>{{status_label(h.status)}}</td><td>{{h.full_name}}</td><td>{{h.updated_at}}</td><td>{{h.notes or '-'}}</td></tr>{% endfor %}
</table></div></div>"""


def _get_ticket(cur, ticket_id):
    cur.execute("""SELECT t.*, req.full_name AS requestor_name, req.email AS requestor_email,
                   req.account_type AS requestor_type, ass.full_name AS assigned_name
                   FROM tickets t JOIN users req ON t.requestor_id = req.id
                   LEFT JOIN users ass ON t.assigned_to = ass.id WHERE t.id = %s""", (ticket_id,))
    r = cur.fetchone()
    return dict(r) if r else None


def _can_view(t, role, uid):
    if role == 'admin':
        return True
    if role == 'requestor':
        return t['requestor_id'] == uid
    if role == 'staff':
        return t['assigned_to'] == uid
    return False


@app.route('/tickets/<int:ticket_id>')
@login_required
def ticket_detail(ticket_id):
    conn = get_db()
    cur = conn.cursor()
    t = _get_ticket(cur, ticket_id)
    if not t:
        conn.close()
        return "Ticket not found", 404
    role, uid = session['role'], session['user_id']
    if not _can_view(t, role, uid):
        conn.close()
        return "Access Denied", 403
    cur.execute("""SELECT sh.*, u.full_name FROM status_history sh JOIN users u ON sh.updated_by = u.id
                   WHERE sh.ticket_id = %s ORDER BY sh.updated_at ASC, sh.id ASC""", (ticket_id,))
    history = dicts(cur.fetchall())
    staff_list = []
    if role == 'admin':
        cur.execute("SELECT id, full_name FROM users WHERE role = 'staff' AND is_active = 1")
        staff_list = dicts(cur.fetchall())
    cur.execute("UPDATE notifications SET is_read = 1 WHERE ticket_id = %s AND user_id = %s", (ticket_id, uid))
    conn.commit()
    conn.close()
    return render_page(DETAIL_T, f"Ticket {t['reference_no']}", t=t, history=history, staff_list=staff_list, role=role)


def _notify(cur, user_id, ticket_id, msg):
    cur.execute("INSERT INTO notifications (user_id,ticket_id,message,created_at) VALUES (%s,%s,%s,%s)",
                (user_id, ticket_id, msg, ph_now_str()))


def _history(cur, ticket_id, status, notes, uid):
    cur.execute("INSERT INTO status_history (ticket_id,status,notes,updated_by,updated_at) VALUES (%s,%s,%s,%s,%s)",
                (ticket_id, status, notes, uid, ph_now_str()))


def _admins(cur):
    cur.execute("SELECT id FROM users WHERE role = 'admin'")
    return [a[0] for a in cur.fetchall()]


@app.route('/tickets/<int:ticket_id>/assign', methods=['POST'])
@admin_required
def assign_ticket(ticket_id):
    try:
        staff_id = int(request.form.get('assigned_to', ''))
    except ValueError:
        return "Select a Staff to assign", 400
    sched = request.form.get('scheduled_date', '')
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT full_name FROM users WHERE id = %s AND role = 'staff' AND is_active = 1", (staff_id,))
    s = cur.fetchone()
    if not s:
        conn.close()
        return "Select an active Staff account", 400
    cur.execute("""UPDATE tickets SET assigned_to=%s, scheduled_date=%s, status='assigned', updated_at=%s
                   WHERE id=%s AND status='pending_admin'""", (staff_id, sched, ph_now_str(), ticket_id))
    if cur.rowcount == 0:
        conn.close()
        return "Ticket not found or already assigned", 400
    cur.execute("SELECT reference_no, requestor_id FROM tickets WHERE id = %s", (ticket_id,))
    ref_no, req_id = cur.fetchone()
    _history(cur, ticket_id, 'assigned', f"Assigned to {s[0]} - Schedule: {sched or 'Not set'}", session['user_id'])
    _notify(cur, staff_id, ticket_id, f"You have been assigned: {ref_no} - Schedule: {sched or 'Not set yet'}")
    _notify(cur, req_id, ticket_id, f"Your request {ref_no} - Staff {s[0]} is assigned and will attend to you soon!")
    conn.commit()
    conn.close()
    return redirect(url_for('ticket_detail', ticket_id=ticket_id))


@app.route('/tickets/<int:ticket_id>/submit-work', methods=['POST'])
@staff_required
def submit_work(ticket_id):
    work = request.form.get('work_done', '').strip()
    if not work:
        return "Describe what work was done", 400
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""UPDATE tickets SET work_done=%s, parts_replaced=%s, resolution_notes=%s,
                   status='completed_pending_staff', updated_at=%s
                   WHERE id=%s AND assigned_to=%s AND status='assigned'""",
                (work, request.form.get('parts_replaced', '').strip(), request.form.get('resolution_notes', '').strip(),
                 ph_now_str(), ticket_id, session['user_id']))
    if cur.rowcount == 0:
        conn.close()
        return "Access denied or this ticket is no longer assigned to you", 403
    cur.execute("SELECT reference_no, requestor_id FROM tickets WHERE id = %s", (ticket_id,))
    ref_no, req_id = cur.fetchone()
    _history(cur, ticket_id, 'completed_pending_staff', 'Work completed - waiting for staff approval', session['user_id'])
    _notify(cur, req_id, ticket_id, f"Work done on your request {ref_no}! Please check and confirm.")
    for a in _admins(cur):
        _notify(cur, a, ticket_id, f"Staff submitted work: {ref_no}")
    conn.commit()
    conn.close()
    return redirect(url_for('ticket_detail', ticket_id=ticket_id))


@app.route('/tickets/<int:ticket_id>/staff-approve', methods=['POST'])
@staff_required
def staff_approve_ticket(ticket_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""UPDATE tickets SET staff_approved=1, status='pending_requestor_feedback', updated_at=%s
                   WHERE id=%s AND assigned_to=%s AND status='completed_pending_staff'""",
                (ph_now_str(), ticket_id, session['user_id']))
    if cur.rowcount == 0:
        conn.close()
        return "Access denied or this ticket is not awaiting staff approval", 403
    cur.execute("SELECT reference_no, requestor_id FROM tickets WHERE id = %s", (ticket_id,))
    ref_no, req_id = cur.fetchone()
    _history(cur, ticket_id, 'pending_requestor_feedback', 'Staff approved - waiting for requestor feedback', session['user_id'])
    _notify(cur, req_id, ticket_id, f"Staff approved work on {ref_no}! Please give your feedback.")
    conn.commit()
    conn.close()
    return redirect(url_for('ticket_detail', ticket_id=ticket_id))


@app.route('/tickets/<int:ticket_id>/admin-approve', methods=['POST'])
@admin_required
def admin_approve_ticket(ticket_id):
    conn = get_db()
    cur = conn.cursor()
    now = ph_now_str()
    cur.execute("""UPDATE tickets SET admin_approved=1, status='completed_approved', completed_at=%s,
                   approved_at=%s, updated_at=%s WHERE id=%s AND status='pending_admin_final'""",
                (now, now, now, ticket_id))
    if cur.rowcount == 0:
        conn.close()
        return "Ticket not found or is not ready for final approval", 400
    cur.execute("SELECT reference_no, requestor_id, assigned_to FROM tickets WHERE id = %s", (ticket_id,))
    ref_no, req_id, staff_id = cur.fetchone()
    _history(cur, ticket_id, 'completed_approved', 'Fully approved and closed', session['user_id'])
    _notify(cur, req_id, ticket_id, f"{ref_no} - FULLY COMPLETED & APPROVED! Thank you!")
    if staff_id:
        _notify(cur, staff_id, ticket_id, f"{ref_no} - Closed with positive feedback. Great work!")
    conn.commit()
    conn.close()
    return redirect(url_for('ticket_detail', ticket_id=ticket_id))


# ==============================================
# PRINT THE EXACT SUBMITTED FORM (SAF) -- one A4 page
# ==============================================
SAF_PRINT_T = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>SAF {{t.reference_no}}</title>
<style>
@page{size:A4 portrait;margin:10mm}
*{box-sizing:border-box}
body{font-family:Arial,sans-serif;font-size:11pt;max-width:190mm;margin:0 auto;padding:10px;color:#000}
h2,h3,p{margin:2px 0;text-align:center}
.row{display:flex;gap:10px;margin:6px 0}.row>div{flex:1;border-bottom:1px solid #000;padding:2px 0;min-height:30px}
small{display:block;color:#555;font-size:8pt}
.box{border:1px solid #000;padding:6px;min-height:50px;margin:6px 0;white-space:pre-wrap}
.chk{display:inline-block;width:48%;margin:2px 0}
.sig{display:flex;gap:20px;margin-top:34px;text-align:center}.sig div{flex:1;border-top:1px solid #000;padding-top:3px;font-size:9pt}
.bar{text-align:center;margin-bottom:10px}.bar button,.bar a{padding:.5rem 1rem;font-size:1rem}
@media print{.bar{display:none}body{padding:0}}
@media(max-width:600px){.row{flex-direction:column;gap:0}.chk{width:100%}}
</style></head><body>
<div class="bar"><button onclick="window.print()">Print</button> <a href="/tickets/{{t.id}}">Back</a></div>
<p>SAF No.: <b>{{t.reference_no}}</b></p>
<h3>SOUTHERN LUZON STATE UNIVERSITY</h3><p>JGE &ndash; Tagkawayan Campus</p>
<h2>SERVICE ASSISTANCE FORM</h2><p>(Maintenance &amp; Repair Purposes)</p>
<div class="row"><div><small>Date Request</small>{{t.submitted_at[:10]}}</div><div><small>Time</small>{{t.submitted_at[11:16]}}</div></div>
<div class="row"><div><small>Requisitioner Office</small>{{t.department}}</div><div><small>Contact Number</small>{{t.contact_number}}</div></div>
<div class="row"><div><small>Requisitioner (Full Name)</small>{{t.requestor_name}}</div></div>
<b>Name of Complaint / Service Needed</b><br>
{% for c in complaints %}<span class="chk">{{ '&#9745;'|safe if c in selected else '&#9744;'|safe }} {{c}}</span>{% endfor %}
<span class="chk">{{ '&#9745;'|safe if others else '&#9744;'|safe }} Other: {{ others|join(', ') }}</span>
<b style="display:block;margin-top:8px">Problem Reported</b><div class="box">{{t.problem_description}}</div>
<div class="row"><div><small>Brand / Model</small>{{t.brand_model or '-'}}</div><div><small>Serial / Property No.</small>{{t.serial_property_no or '-'}}</div></div>
<div class="row"><div><small>Location / Room No.</small>{{t.location_room}}</div><div><small>Priority</small>{{t.priority|title}}</div></div>
{% if t.work_done %}<b>Work Done</b><div class="box">{{t.work_done}}</div>
{% if t.parts_replaced %}<b>Parts Replaced / Used</b><div class="box">{{t.parts_replaced}}</div>{% endif %}
{% if t.resolution_notes %}<b>Remarks</b><div class="box">{{t.resolution_notes}}</div>{% endif %}{% endif %}
<div class="sig"><div>{{t.requestor_name}}<br>Requisitioner</div><div>{{t.assigned_name or 'IT Staff'}}<br>Assigned Staff</div><div>Approved by (Admin)</div></div>
</body></html>"""


@app.route('/tickets/<int:ticket_id>/print')
@login_required
def print_saf(ticket_id):
    conn = get_db()
    cur = conn.cursor()
    t = _get_ticket(cur, ticket_id)
    conn.close()
    if not t:
        return "Ticket not found", 404
    if not _can_view(t, session['role'], session['user_id']):
        return "Access Denied", 403
    selected = [s.strip() for s in (t['equipment_type'] or '').split(',') if s.strip()]
    others = [s for s in selected if s not in SAF_COMPLAINTS]
    return render_template_string(SAF_PRINT_T, t=t, selected=selected, others=others, complaints=SAF_COMPLAINTS)


# ==============================================
# CSM FORM (fill + view-only, both print on ONE page)
# ==============================================
SQD = [("sqd0", "SQD0. I am satisfied with the service that I availed."),
       ("sqd1", "SQD1. I spent a reasonable amount of time for my transaction."),
       ("sqd2", "SQD2. The office followed the transaction's requirements and steps based on the information provided."),
       ("sqd3", "SQD3. The steps I needed to do for my transaction were easy and simple."),
       ("sqd4", "SQD4. I easily found information about my transaction from the office or its website."),
       ("sqd5", "SQD5. I paid a reasonable amount of fees for my transactions."),
       ("sqd6", "SQD6. I feel the office was fair to everyone, or \"walang palakasan\", during my transaction."),
       ("sqd7", "SQD7. I was treated courteously by the staff, and (if asked for help) the staff was helpful."),
       ("sqd8", "SQD8. I got what I needed, or (if denied) the denial was sufficiently explained to me.")]
SQD_COLS = [("1", "😟", "Strongly Disagree"), ("2", "😕", "Disagree"), ("3", "😐", "Neither Agree nor Disagree"),
            ("4", "🙂", "Agree"), ("5", "😄", "Strongly Agree"), ("NA", "", "N/A")]

CSM_T = """
{% macro radio(name, val, label) %}<label class="opt"><input type="radio" name="{{name}}" value="{{val}}" {{'checked' if c.get(name)==val}} {{'disabled' if ro}}> {{label}}</label>{% endmacro %}
<a class="back" href="/tickets/{{t.id}}">&larr; Back to Ticket</a>
<div class="card" style="margin-top:.6rem">
 <div class="arta">ANTI-RED TAPE AUTHORITY<br>Client Satisfaction Measurement Form<br>PSA Approved No. ARTA-2242-3<br>Control No: <b>{{t.reference_no}}</b></div>
 <div class="brand" style="text-align:left"><h1>SOUTHERN LUZON STATE UNIVERSITY</h1><h2>HELP US SERVE YOU BETTER!</h2></div>
 <p style="clear:both">This Client Satisfaction Measurement (CSM) tracks the customer experience of our office. Your feedback on your <b>recently concluded transaction</b> will help us provide a better service. Personal information shared will be kept confidential and you always have the option to not answer this form.</p>
 {% if ro %}<div class="alert info no-print"><b>VIEW ONLY</b> &mdash; CSM response submitted by Requisitioner ({{t.reference_no}})</div>{% endif %}
 <form method="post">
  <div class="qb"><b>Client type:</b>
   {{radio('client_type','Citizen','Citizen')}}{{radio('client_type','Business','Business')}}{{radio('client_type','Government','Government (Employee / agency)')}}</div>
  <div class="grid2">
   <div class="qb"><b>Sex:</b> {{radio('sex','Male','Male')}}{{radio('sex','Female','Female')}}</div>
   <div class="qb"><b>Age:</b><input name="age" value="{{c.get('age') or ''}}" {{'readonly' if ro}} placeholder="e.g. 25"></div>
  </div>
  <div class="grid2">
   <div class="qb"><b>Region of residence:</b><input name="region" value="{{c.get('region') or ''}}" {{'readonly' if ro}} placeholder="e.g. CALABARZON (Region IV-A)"></div>
   <div class="qb"><b>Service Availed:</b><input name="service_availed" value="{{c.get('service_availed') or t.equipment_type}}" readonly></div>
  </div>
  <div class="qb"><b>CC1. Which of the following best describes your awareness of a Citizen's Charter (CC)?</b>
   {{radio('cc1','1','1. I know what a CC is, and I saw this office\\'s CC.')}}
   {{radio('cc1','2','2. I know what a CC is, but I did NOT see this office\\'s CC.')}}
   {{radio('cc1','3','3. I learned of the CC only when I saw this office\\'s CC.')}}
   {{radio('cc1','4','4. I do not know what a CC is, and I did not see one in this office.')}}</div>
  <div class="grid2">
   <div class="qb"><b>CC2. If aware of CC, would you say the CC of this office was...?</b>
    {{radio('cc2','1','1. Easy to see')}}{{radio('cc2','2','2. Somewhat easy to see')}}{{radio('cc2','3','3. Difficult to see')}}{{radio('cc2','4','4. Not visible at all')}}{{radio('cc2','5','5. N/A')}}</div>
   <div class="qb"><b>CC3. If aware of CC, how much did the CC help you in your transaction?</b>
    {{radio('cc3','1','1. Helped very much')}}{{radio('cc3','2','2. Somewhat helped')}}{{radio('cc3','3','3. Did not help')}}{{radio('cc3','4','4. N/A')}}</div>
  </div>
  <div class="qb"><b>SQD 0&ndash;8:</b> Please put a check mark on the column that best corresponds to your answer.
   <div class="tablewrap"><table class="sqd">
    <tr><th>Question</th>{% for v,e,l in cols %}<th>{{e}}<br>{{l}}</th>{% endfor %}</tr>
    {% for k,q in sqd %}<tr><td>{{q}}</td>{% for v,e,l in cols %}<td><input type="radio" name="{{k}}" value="{{v}}" {{'checked' if c.get(k)==v}} {{'disabled' if ro}}></td>{% endfor %}</tr>{% endfor %}
   </table></div></div>
  <div class="qb"><b>Suggestions on how we can further improve our services (optional):</b>
   <textarea name="suggestions" rows="3" {{'readonly' if ro}}>{{c.get('suggestions') or ''}}</textarea></div>
  <div class="qb"><b>Email address (optional):</b><input type="email" name="email" value="{{c.get('email') or ''}}" {{'readonly' if ro}}></div>
  {% if ro %}<button type="button" class="full gold no-print" onclick="window.print()">Print this CSM Form</button>
  {% else %}<button class="full">Submit Client Satisfaction Form &mdash; THANK YOU!</button>{% endif %}
 </form>
</div>"""


def _csm_page(t, c, ro):
    return render_page(CSM_T, f"CSM Form - {t['reference_no']}", t=t, c=c, ro=ro, sqd=SQD, cols=SQD_COLS)


@app.route('/tickets/<int:ticket_id>/csm', methods=['GET', 'POST'])
@login_required
def csm_form(ticket_id):
    if session['role'] != 'requestor':
        return "Only Requestors can submit the Client Satisfaction form", 403
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets WHERE id = %s AND requestor_id = %s", (ticket_id, session['user_id']))
    r = cur.fetchone()
    if not r:
        conn.close()
        return "Ticket not found", 404
    t = dict(r)
    if t['status'] != 'pending_requestor_feedback':
        conn.close()
        return "This ticket is not awaiting your feedback", 403
    cur.execute("SELECT id FROM csm_responses WHERE ticket_id = %s", (ticket_id,))
    if cur.fetchone():
        conn.close()
        return redirect(url_for('ticket_detail', ticket_id=ticket_id))
    if request.method == 'GET':
        conn.close()
        return _csm_page(t, {}, False)

    f = request.form
    cur.execute("""INSERT INTO csm_responses (ticket_id, client_type, sex, age, region, service_availed,
        cc1, cc2, cc3, sqd0, sqd1, sqd2, sqd3, sqd4, sqd5, sqd6, sqd7, sqd8, suggestions, email, submitted_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (ticket_id, f.get('client_type'), f.get('sex'), f.get('age'), f.get('region'), t['equipment_type'],
                 f.get('cc1'), f.get('cc2'), f.get('cc3'), *[f.get(f'sqd{i}') for i in range(9)],
                 f.get('suggestions', '').strip(), f.get('email', '').strip(), ph_now_str()))
    summary = "CSM Form submitted. Satisfaction (SQD0): " + (f.get('sqd0') or 'N/A')
    if f.get('suggestions', '').strip():
        summary += " | Suggestions: " + f.get('suggestions').strip()[:200]
    cur.execute("""UPDATE tickets SET requestor_feedback=%s, status='pending_admin_final', updated_at=%s
                   WHERE id=%s AND requestor_id=%s AND status='pending_requestor_feedback'""",
                (summary, ph_now_str(), ticket_id, session['user_id']))
    _history(cur, ticket_id, 'pending_admin_final', 'CSM feedback submitted - waiting Admin final approval', session['user_id'])
    if t['assigned_to']:
        _notify(cur, t['assigned_to'], ticket_id, f"CSM feedback received on {t['reference_no']}")
    for a in _admins(cur):
        _notify(cur, a, ticket_id, f"CSM feedback received for {t['reference_no']} - Ready for final approval")
    conn.commit()
    conn.close()
    return redirect(url_for('ticket_detail', ticket_id=ticket_id))


@app.route('/tickets/<int:ticket_id>/csm/view')
@login_required
def view_csm_response(ticket_id):
    conn = get_db()
    cur = conn.cursor()
    t = _get_ticket(cur, ticket_id)
    if not t or not _can_view(t, session['role'], session['user_id']):
        conn.close()
        return "Access Denied", 403
    cur.execute("SELECT * FROM csm_responses WHERE ticket_id = %s", (ticket_id,))
    c = cur.fetchone()
    conn.close()
    if not c:
        return "No CSM form response found for this ticket.", 404
    return _csm_page(t, dict(c), True)


# ==============================================
# NOTIFICATIONS, REPORTS, STAFF MANAGEMENT
# ==============================================
NOTIF_T = """<a class="back" href="/dashboard">&larr; Back to Dashboard</a><h2>Notifications</h2>
{% for n in notifs %}<div class="notif"><b>{{n.message}}</b>{% if n.ticket_id %} &mdash; <a href="/tickets/{{n.ticket_id}}">View Details &rarr;</a>{% endif %}<br><small>{{n.created_at}}</small></div>
{% else %}<div class="card">No notifications yet.</div>{% endfor %}"""


@app.route('/notifications')
@login_required
def notifications():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM notifications WHERE user_id = %s ORDER BY created_at DESC, id DESC", (session['user_id'],))
    notifs = dicts(cur.fetchall())
    cur.execute("UPDATE notifications SET is_read = 1 WHERE user_id = %s", (session['user_id'],))
    conn.commit()
    conn.close()
    return render_page(NOTIF_T, "Notifications", notifs=notifs)


REPORTS_T = """<a class="back" href="/dashboard">&larr; Back to Dashboard</a><h2>Staff Work Reports</h2>
<p>Reports submitted by staff after completing assigned work.</p>
{% for r in reports %}<div class="card"><div style="display:flex;flex-wrap:wrap;justify-content:space-between;gap:.5rem">
 <h3 style="margin:0">{{r.reference_no}} &mdash; {{r.equipment_type}}</h3><span class="badge s-{{r.status}}">{{status_label(r.status)}}</span></div>
 <div class="tablewrap"><table class="kv">
 <tr><td>Assigned Staff</td><td>{{r.staff_name or 'Not assigned'}}</td></tr>
 <tr><td>Requestor / Dept</td><td>{{r.requestor_name}} / {{r.department}}</td></tr>
 <tr><td>Submitted / Updated</td><td>{{r.updated_at}}</td></tr>
 <tr><td>Work completed</td><td>{{r.work_done}}</td></tr>
 {% if r.parts_replaced %}<tr><td>Parts replaced</td><td>{{r.parts_replaced}}</td></tr>{% endif %}
 {% if r.resolution_notes %}<tr><td>Remarks</td><td>{{r.resolution_notes}}</td></tr>{% endif %}</table></div>
 <p><a href="/tickets/{{r.id}}">View Full Ticket</a></p></div>
{% else %}<div class="card">No staff work reports have been submitted yet.</div>{% endfor %}"""


@app.route('/admin/reports')
@admin_required
def admin_reports():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""SELECT t.id, t.reference_no, t.equipment_type, t.department, t.status, t.work_done,
                   t.parts_replaced, t.resolution_notes, t.updated_at, req.full_name AS requestor_name,
                   ass.full_name AS staff_name
                   FROM tickets t JOIN users req ON t.requestor_id = req.id
                   LEFT JOIN users ass ON t.assigned_to = ass.id
                   WHERE t.work_done IS NOT NULL AND TRIM(t.work_done) != '' ORDER BY t.updated_at DESC""")
    reports = dicts(cur.fetchall())
    conn.close()
    return render_page(REPORTS_T, "Staff Reports", reports=reports)


STAFF_T = """<a class="back" href="/dashboard">&larr; Back to Dashboard</a><h2>IT Staff Accounts</h2>
<div class="card"><h3 style="margin-top:0">Add IT Staff</h3>
 {% if msg %}<div class="alert {{'err' if err else 'info'}}">{{msg}}</div>{% endif %}
 <form method="post"><div class="grid2">
  <div><label>Full Name *</label><input name="full_name" required></div>
  <div><label>Email *</label><input type="email" name="email" required></div>
  <div><label>Contact Number *</label><input name="contact_number" required></div>
  <div><label>Temporary Password *</label><input type="password" name="password" minlength="6" required></div></div>
  <button class="full">Create Staff Account</button></form></div>
<div class="card"><h3 style="margin-top:0">Current IT Staff</h3><div class="tablewrap"><table>
 <tr><th>Name</th><th>Email</th><th>Contact</th></tr>
 {% for s in staff %}<tr><td>{{s.full_name}}</td><td>{{s.email}}</td><td>{{s.contact_number}}</td></tr>
 {% else %}<tr><td colspan="3">No IT staff yet.</td></tr>{% endfor %}</table></div></div>"""


@app.route('/admin/staff', methods=['GET', 'POST'])
@admin_required
def admin_staff():
    msg, err = None, False
    conn = get_db()
    cur = conn.cursor()
    if request.method == 'POST':
        d = request.form
        if not all(d.get(k, '').strip() for k in ('full_name', 'email', 'contact_number', 'password')):
            msg, err = "Please fill in all fields.", True
        else:
            try:
                cur.execute("""INSERT INTO users (full_name,email,password_hash,role,department,contact_number,
                               account_type,terms_accepted_at) VALUES (%s,%s,%s,'staff','MIS-ICT',%s,'Staff',%s)""",
                            (d['full_name'].strip(), d['email'].strip().lower(), generate_password_hash(d['password']),
                             d['contact_number'].strip(), ph_now_str()))
                conn.commit()
                msg = "Staff account created."
            except (psycopg2.IntegrityError, sqlite3.IntegrityError):
                conn.rollback()
                msg, err = "Email already registered.", True
    cur.execute("SELECT full_name, email, contact_number FROM users WHERE role = 'staff' ORDER BY full_name")
    staff = dicts(cur.fetchall())
    conn.close()
    return render_page(STAFF_T, "IT Staff", staff=staff, msg=msg, err=err)


# ==============================================
# START
# ==============================================
_detect_backend()
init_db()
seed_admin()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
