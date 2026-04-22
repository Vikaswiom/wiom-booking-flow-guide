"""
Wiom Acquisition Funnel Dashboard -- One-click Data Refresh
Usage: python refresh_dashboard.py
- Queries Metabase with dynamic POST window (Mar 28 to today-7days)
- Updates dashboard_data.js + index.html
- Pushes to GitHub
"""
import json, os, ssl, re, subprocess, shutil, urllib.request
from datetime import datetime, timedelta

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOY_DIR = os.path.join(BASE_DIR, "booking-flow-dashboard-deploy")
HTML_FILE = os.path.join(BASE_DIR, "index.html")
ENV_FILE = r"C:\credentials\.env"
METABASE_URL = "https://metabase.wiom.in/api/dataset"
DB_ID = 113

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def load_api_key():
    with open(ENV_FILE) as f:
        for line in f:
            if line.startswith("METABASE_API_KEY"):
                return line.split("=", 1)[1].strip().strip('"')
    raise RuntimeError("METABASE_API_KEY not found")

API_KEY = load_api_key()

def run_query(sql):
    payload = json.dumps({"database": DB_ID, "type": "native", "native": {"query": sql}}).encode()
    req = urllib.request.Request(METABASE_URL, data=payload, headers={
        "x-api-key": API_KEY, "Content-Type": "application/json"
    })
    resp = urllib.request.urlopen(req, context=ctx, timeout=300)
    data = json.loads(resp.read())
    if data.get("error"):
        raise RuntimeError(f"Query error: {data['error']}")
    cols = [c["name"].upper() for c in data["data"]["cols"]]
    rows = data["data"]["rows"]
    return cols, rows

def read_sql(fn):
    with open(os.path.join(BASE_DIR, fn), encoding="utf-8") as f:
        return f.read()

def pct(n, t):
    return round(n / t * 100, 1) if t > 0 else 0.0

def fmt(n):
    return f"{int(n):,}"

# === DATE CALC ===
today = datetime.now()
post_end = (today - timedelta(days=7)).strftime("%Y-%m-%d")
post_end_display = (today - timedelta(days=7)).strftime("%b %d")
ts = today.strftime("%b %d %H:%M")

print("=" * 60)
print(f"Wiom Dashboard Refresh -- {ts}")
print(f"POST window: Mar 28 to {post_end_display} (today-7days)")
print("=" * 60)

# === QUERIES ===
print("\n[1/4] POST Section B Funnel...")
cols, rows = run_query(read_sql("funnel_query_post.sql"))
post = dict(zip(cols, rows[0]))
post_end_actual = str(post.get('POST_END_DATE', post_end))[:10]
print(f"  {fmt(post['TOTAL_CUSTOMERS'])} customers, {fmt(post['OTP_VERIFIED'])} installed ({pct(post['OTP_VERIFIED'], post['TOTAL_CUSTOMERS'])}%)")
print(f"  POST end date from query: {post_end_actual}")

print("[2/4] POST Churn Matrix...")
cols, rows = run_query(read_sql("churn_matrix_query.sql"))
churn = dict(zip(cols, rows[0]))
print(f"  S9: cust={int(churn['S9_CUST'])}, sys={int(churn['S9_SYS'])}")

print("[3/4] New Release Overall...")
try:
    cols, rows = run_query(read_sql("clevertap_funnel_apr15.sql"))
    nr = dict(zip(cols, rows[0]))
    print(f"  {fmt(nr['APP_INSTALLED'])} installs, {fmt(nr['FEE_CAPTURED'])} paid ({pct(nr['FEE_CAPTURED'], nr['APP_INSTALLED'])}%)")
except Exception as e:
    print(f"  SKIP: {e}")
    nr = None

print("[4/4] A/B/C/D Variants...")
try:
    cols, rows = run_query(read_sql("abcd_funnel.sql"))
    variants = {}
    for row in rows:
        v = row[0] if row[0] else "empty"
        variants[v] = dict(zip(cols, row))
    for v in ["A", "B", "C", "D"]:
        if v in variants:
            d = variants[v]
            print(f"  {v}: n={int(d['GET_STARTED'])}, paid={int(d['FEE_CAPTURED'])}, conv={pct(d['FEE_CAPTURED'], d['GET_STARTED'])}%")
except Exception as e:
    print(f"  SKIP: {e}")
    variants = None

# === BUILD DATA ===
p = post
data_obj = {
    "refreshed_at": today.strftime("%Y-%m-%d %H:%M"),
    "post_end_date": post_end_actual,
    "post_funnel": {
        "total": int(p['TOTAL_CUSTOMERS']), "ssid": int(p['SSID_SET']),
        "address": int(p['ADDRESS_UPDATED']), "verified": int(p['BOOKING_VERIFIED']),
        "notif": int(p['NOTIF_SENT']), "interested": int(p['INTERESTED']),
        "slot": int(p['SLOT_SELECTED']), "confirmed": int(p['CUSTOMER_SLOT_CONFIRMED']),
        "assigned": int(p['ASSIGNED']), "otp": int(p['OTP_VERIFIED']),
        "cancelled": int(p['CANCELLED']),
    },
}
if nr:
    data_obj["new_release"] = {k: int(v) for k, v in {
        "installs": nr['APP_INSTALLED'], "homepage": nr['HOMEPAGE'],
        "check": nr['CHECK_CLICKED'], "serviceable": nr['SERVICEABLE'],
        "unserviceable": nr['UNSERVICEABLE'], "how_works": nr['HOW_WORKS'],
        "get_started": nr['GET_STARTED'], "cost": nr['COST_TODAY'],
        "pay100": nr['PAY_100'], "location": nr.get('LOCATION_CONFIRM', 0),
        "paid": nr['FEE_CAPTURED'],
    }.items()}
if variants:
    data_obj["variants"] = {}
    for v in ["A", "B", "C", "D"]:
        if v in variants:
            d = variants[v]
            data_obj["variants"][v] = {k: int(d[c]) for k, c in {
                "entry": "GET_STARTED", "cost": "COST_TODAY",
                "pay100": "PAY_100", "location": "LOCATION_CONFIRM",
                "paid": "FEE_CAPTURED",
            }.items() if c in d}

# Write dashboard_data.js
js_content = f"// Auto-generated {today.strftime('%Y-%m-%d %H:%M')}\nconst DASHBOARD_DATA = {json.dumps(data_obj, indent=2)};\n"
with open(os.path.join(BASE_DIR, "dashboard_data.js"), "w", encoding="utf-8") as f:
    f.write(js_content)

# === UPDATE HTML ===
print("\nUpdating index.html...")
with open(HTML_FILE, "r", encoding="utf-8") as f:
    html = f.read()

pf = data_obj["post_funnel"]
installed_pct = pct(pf["otp"], pf["total"])
lost = pf["total"] - pf["otp"]
lost_pct = pct(lost, pf["total"])

# Update POST period box
html = re.sub(
    r'(POST.*?Bookings: Mar 28.*?Apr )\d+(, 2026)',
    lambda m: m.group(1) + post_end_actual.split("-")[2].lstrip("0") + m.group(2),
    html, count=1
)
html = re.sub(
    r'(POST.*?Installed:.*?<b>)[\d,]+\s*\([\d.]+%\)',
    lambda m: m.group(1) + f"{fmt(pf['otp'])} ({installed_pct}%)",
    html, count=1
)
html = re.sub(
    r'(POST.*?Lost:.*?<b>)[\d,]+\s*\([\d.]+%\)',
    lambda m: m.group(1) + f"{fmt(lost)} ({lost_pct}%)",
    html, count=1
)
html = re.sub(
    r'(POST.*?Customers:.*?<b>)[\d,]+',
    lambda m: m.group(1) + fmt(pf["total"]),
    html, count=1
)

# Update POST success badge
html = re.sub(
    r'(\d[\d,]+ / \d[\d,]+ = [\d.]+%</div>\s*<div[^>]*>Mar 28)',
    f"{fmt(pf['otp'])} / {fmt(pf['total'])} = {installed_pct}%</div>\n    <div style=\"font-size:11px;opacity:0.7;margin-top:4px;font-weight:500;\">Mar 28",
    html, count=1
)

# Update POST date in caveat
html = re.sub(
    r'(Bookings made.*?Mar 28.*?Apr )\d+',
    lambda m: m.group(1) + post_end_actual.split("-")[2].lstrip("0"),
    html, count=1
)

with open(HTML_FILE, "w", encoding="utf-8") as f:
    f.write(html)

# === SUMMARY ===
print(f"\nPOST: {fmt(pf['total'])} -> {fmt(pf['otp'])} installed ({installed_pct}%), {fmt(pf['cancelled'])} cancelled")
if "new_release" in data_obj:
    n = data_obj["new_release"]
    print(f"New Release: {fmt(n['installs'])} -> {fmt(n['paid'])} paid ({pct(n['paid'], n['installs'])}%)")
if "variants" in data_obj:
    ranked = sorted(data_obj["variants"].items(), key=lambda x: x[1].get("paid",0)/max(x[1].get("entry",1),1), reverse=True)
    for i, (v, d) in enumerate(ranked):
        print(f"  #{i+1} {v}: {d.get('entry',0)} -> {d.get('paid',0)} ({pct(d.get('paid',0), d.get('entry',1))}%)")

# === DEPLOY ===
print(f"\nCopying + pushing...")
for f in ["index.html", "dashboard_data.js"]:
    src, dst = os.path.join(BASE_DIR, f), os.path.join(DEPLOY_DIR, f)
    if os.path.exists(src): shutil.copy2(src, dst)

os.chdir(DEPLOY_DIR)
subprocess.run(["git", "add", "-A"], check=True)
result = subprocess.run(["git", "diff", "--cached", "--quiet"])
if result.returncode != 0:
    msg = f"Auto-refresh {ts}: POST Mar28-{post_end_display}, {int(pf['otp'])} installed"
    subprocess.run(["git", "commit", "-m", msg], check=True)
    subprocess.run(["git", "push", "origin", "master"], check=True)
    print(f"\n[OK] Pushed! POST window: Mar 28 - {post_end_display}")
    print(f"   https://vikaswiom.github.io/wiom-booking-flow-guide/")
else:
    print("\n[SKIP] No changes.")

print(f"\nDone! {today.strftime('%Y-%m-%d %H:%M')}")
