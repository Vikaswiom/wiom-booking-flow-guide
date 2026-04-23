"""Refresh script for GitHub Actions — generates dashboard_data.js"""
import json, os, ssl, urllib.request
from datetime import datetime, timedelta

METABASE_URL = "https://metabase.wiom.in/api/dataset"
DB_ID = 113
API_KEY = os.environ.get("METABASE_API_KEY", "")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def run_query(sql):
    payload = json.dumps({"database": DB_ID, "type": "native", "native": {"query": sql}}).encode()
    req = urllib.request.Request(METABASE_URL, data=payload, headers={
        "x-api-key": API_KEY, "Content-Type": "application/json"
    })
    resp = urllib.request.urlopen(req, context=ctx, timeout=300)
    data = json.loads(resp.read())
    if data.get("error"): raise RuntimeError(data["error"])
    cols = [c["name"].upper() for c in data["data"]["cols"]]
    return cols, data["data"]["rows"]

def pct(n,t): return round(n/t*100,1) if t>0 else 0

# Read SQL files from repo
def read_sql(fn):
    for d in [".", "sql"]:
        p = os.path.join(d, fn)
        if os.path.exists(p):
            return open(p, encoding="utf-8").read()
    raise FileNotFoundError(fn)

print("Refreshing dashboard data...")

# POST funnel
cols, rows = run_query(read_sql("funnel_query_post.sql"))
post = dict(zip(cols, rows[0]))
print(f"POST: {int(post['TOTAL_CUSTOMERS'])} customers, {int(post['OTP_VERIFIED'])} installed")

# Churn
cols, rows = run_query(read_sql("churn_matrix_query.sql"))
churn = dict(zip(cols, rows[0]))

# New release
try:
    cols, rows = run_query(read_sql("clevertap_funnel_apr15.sql"))
    nr = dict(zip(cols, rows[0]))
except: nr = None

# Variants
try:
    cols, rows = run_query(read_sql("abcd_funnel.sql"))
    variants = {r[0]: dict(zip(cols, r)) for r in rows if r[0] in ["A","B","C","D"]}
except: variants = None

# Build data
data = {
    "refreshed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    "post_end_date": str(post.get("POST_END_DATE", ""))[:10],
    "post_funnel": {k: int(post[v]) for k,v in {
        "total":"TOTAL_CUSTOMERS","ssid":"SSID_SET","address":"ADDRESS_UPDATED",
        "verified":"BOOKING_VERIFIED","notif":"NOTIF_SENT","interested":"INTERESTED",
        "slot":"SLOT_SELECTED","confirmed":"CUSTOMER_SLOT_CONFIRMED",
        "assigned":"ASSIGNED","otp":"OTP_VERIFIED","cancelled":"CANCELLED"
    }.items()},
}
if nr:
    data["new_release"] = {k: int(nr.get(v,0)) for k,v in {
        "installs":"APP_INSTALLED","homepage":"HOMEPAGE","check":"CHECK_CLICKED",
        "serviceable":"SERVICEABLE","unserviceable":"UNSERVICEABLE","how_works":"HOW_WORKS",
        "get_started":"GET_STARTED","cost":"COST_TODAY","pay100":"PAY_100",
        "location":"LOCATION_CONFIRM","paid":"FEE_CAPTURED"
    }.items()}
if variants:
    data["variants"] = {v: {k: int(d.get(c,0)) for k,c in {
        "entry":"GET_STARTED","cost":"COST_TODAY","pay100":"PAY_100",
        "location":"LOCATION_CONFIRM","paid":"FEE_CAPTURED"
    }.items()} for v,d in variants.items()}

with open("dashboard_data.js", "w") as f:
    f.write(f"const DASHBOARD_DATA = {json.dumps(data, indent=2)};\n")
print("Done! dashboard_data.js updated.")
