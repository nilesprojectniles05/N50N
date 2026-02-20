import os
import base64
import json
import time, datetime, math, requests
from dhanhq import dhanhq
import gspread
from google.oauth2.service_account import Credentials

print("🔥 N50 FINAL MASTER ENGINE V5.1 ULTRA RUNNING 🔥")

# ================= GOOGLE AUTH =================

GOOGLE_B64 = os.getenv("BASE64")

if not GOOGLE_B64:
    raise ValueError("❌ BASE64 environment variable not found")

creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(GOOGLE_B64).decode()),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)
ws = gc.open("N50").sheet1
# ================= ULTRA SHEET MODE =================
SHEET_CACHE = {}
WRITE_CACHE = {}

LAST_CPR=None

STATE = {}

STATE["predictive_gamma"] = "NONE"
STATE["premium_velocity"] = 0
# ================= GOD TIER STATE =================


def set_state(key,value):
    STATE[key] = value

def get_state(key,default=""):
    return STATE.get(key,default)
# ================= SAFE =================

def safe(c):
    v = SHEET_CACHE.get(c,"")
    return "" if v is None else str(v).strip()

def ultra_write(cell,value):
    WRITE_CACHE[cell] = value

# ================= DHAN CLIENT =================

CLIENT_ID = ws.acell("B1").value
ACCESS_TOKEN = ws.acell("B2").value


dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# ================= HEADERS =================

def headers():
    return {
        "client-id": CLIENT_ID,
        "access-token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }


# ================= MARKET =================

# ================= MARKET (ULTRA SAFE) =================

def market():

    try:

        r = requests.post(
            "https://api.dhan.co/v2/marketfeed/quote",
            headers=headers(),
            json={"IDX_I":[13]}
        ).json()

        # ----- SAFE PARSE -----
        if "data" not in r:
            print("MARKET ERROR:", r)
            return None, None

        data = r.get("data", {})

        if "IDX_I" not in data:
            print("MARKET IDX_I MISSING:", r)
            return None, None

        idx = data["IDX_I"]

        if "13" not in idx:
            print("MARKET SECURITY ID MISSING:", r)
            return None, None

        ltp = float(idx["13"]["last_price"])

    except Exception as e:

        print("MARKET API ERROR:", e)
        return None, None


    # ---------- INDIA VIX ----------
    vix = 15

    try:

        s = requests.Session()
        h = {"User-Agent":"Mozilla/5.0"}

        s.get("https://www.nseindia.com", headers=h)

        j = s.get("https://www.nseindia.com/api/allIndices", headers=h).json()

        for i in j.get("data", []):
            if i.get("index") == "INDIA VIX":
                vix = float(i.get("last",15))

    except:
        pass

    return ltp, vix


# ================= CPR ENGINE (ULTRA SAFE FINAL) =================

def cpr_engine(ltp):

    global LAST_CPR

    try:

        hist = requests.post(
            "https://api.dhan.co/v2/charts/historical",
            headers=headers(),
            json={
                "securityId":"13",
                "exchangeSegment":"IDX_I",
                "instrument":"INDEX",
                "fromDate":(datetime.date.today()-datetime.timedelta(days=10)).strftime("%Y-%m-%d"),
                "toDate":datetime.date.today().strftime("%Y-%m-%d")
            }
        ).json()

        highs=[]
        lows=[]
        closes=[]

        # ---- SAFE PARSE ----
        if "high" in hist:
            highs = hist.get("high",[])
            lows  = hist.get("low",[])
            closes= hist.get("close",[])

        elif "data" in hist:
            data = hist.get("data",{})
            highs = data.get("high",[])
            lows  = data.get("low",[])
            closes= data.get("close",[])

        else:
            print("CPR STRUCTURE UNKNOWN:",hist)
            return

        if len(highs)<2 or len(lows)<2 or len(closes)<2:
            print("CPR INSUFFICIENT DATA")
            return

        H=highs[-2]
        L=lows[-2]
        C=closes[-2]

        pivot=(H+L+C)/3
        bc=(H+L)/2
        tc=pivot*2-bc

        tc,bc=max(tc,bc),min(tc,bc)

        width=abs(tc-bc)

        if width<=40:
            typ="ULTRA NARROW ⚡"
        elif width<=70:
            typ="NARROW 🔥"
        elif width<=120:
            typ="NORMAL"
        else:
            typ="WIDE 🧊"

        ws.update(range_name="H4:H6", values=[[tc],[pivot],[bc]])

        ultra_write("H7",width)
        ultra_write("H8",typ)

        relation="INSIDE CPR"

        if ltp>tc:
            relation="ABOVE CPR"
        elif ltp<bc:
            relation="BELOW CPR"

        set_state("relation", relation)
        set_state("tc", tc)
        set_state("pivot", pivot)
        set_state("bc", bc)

        ultra_write("H9",relation)

    except Exception as e:

        print("CPR ENGINE ERROR:",e)


#==================== RESIS/SUPP/MAX PAIN ======================

def oi_levels_engine(ltp, oc):

    max_ce_oi = 0
    max_pe_oi = 0

    resistance = None
    support = None
    pain_map = {}

    atm = round(ltp/50)*50

    for k,v in oc.items():

        strike = int(float(k))

        # only relevant strikes
        if abs(strike-atm) > 500:
            continue

        ce = v.get("ce")
        pe = v.get("pe")

        ce_oi = ce.get("oi",0) if ce else 0
        pe_oi = pe.get("oi",0) if pe else 0

        if strike > ltp and ce_oi > max_ce_oi:
            max_ce_oi = ce_oi
            resistance = strike

        if strike < ltp and pe_oi > max_pe_oi:
            max_pe_oi = pe_oi
            support = strike

        pain_map[strike] = abs(ce_oi - pe_oi)

    if resistance:
        ultra_write("B31", resistance)

    if support:
        ultra_write("B33", support)

    if pain_map:
        maxpain = min(pain_map, key=pain_map.get)
        ultra_write("B32", maxpain)
# ================= VIX RANGE =================

def vix_range_engine(ltp, vix):

    # Daily sigma formula
    pct = vix / math.sqrt(365)
    pts = (pct / 100) * ltp

    vix_high = round(ltp + pts)
    vix_low = round(ltp - pts)

    ultra_write("D31", vix_high)   # VIX HIGH
    ultra_write("D32", vix_low)    # VIX LOW


# ================= VWAP =================

# ================= VWAP ULTRA ENGINE v2 (LIVE SAFE) =================

LAST_VWAP = None

def vwap(ltp, oc_data):

    global LAST_VWAP

    try:

        # =====================================================
        # 1️⃣ PRIMARY — TRUE INDEX VWAP (INTRADAY CANDLES)
        # =====================================================

        intraday = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=headers(),
            json={
                "securityId":"13",
                "exchangeSegment":"IDX_I",
                "instrument":"INDEX",
                "interval":"1"
            }
        ).json()

        data = intraday.get("data", {})

        price = []
        vol   = []

        # ----- handle multiple API structures -----

        if "candles" in data:

            # format: [time, open, high, low, close, volume]

            for c in data["candles"]:
                try:
                    price.append(float(c[4]))
                    vol.append(float(c[5]))
                except:
                    continue

        elif "close" in data and "volume" in data:

            # alternate dhan format

            for p,v in zip(data["close"], data["volume"]):
                price.append(float(p))
                vol.append(float(v))

        # ----- calculate VWAP if valid -----

        if price and vol and sum(vol) > 0:

            v = sum(p*v for p,v in zip(price,vol)) / sum(vol)

            LAST_VWAP = round(v,2)

            ultra_write("Q3", LAST_VWAP)

            print("VWAP LIVE:", LAST_VWAP)

            return

        
        # =====================================================
        # 2️⃣ FALLBACK — INSTITUTIONAL OPTIONCHAIN VWAP
        # =====================================================

        total_weight   = 0
        weighted_price = 0

        atm = round(ltp/50)*50

        for i in range(-200,250,50):

            k = f"{atm+i:.6f}"

            if k not in oc_data:
                continue

            ce = oc_data[k].get("ce")
            pe = oc_data[k].get("pe")

            if ce:
                p = float(ce.get("last_price",0))
                v = float(ce.get("volume",0))
                if v > 0:
                    weighted_price += p*v
                    total_weight   += v

            if pe:
                p = float(pe.get("last_price",0))
                v = float(pe.get("volume",0))
                if v > 0:
                    weighted_price += p*v
                    total_weight   += v

        if total_weight > 0:

            LAST_VWAP = round(weighted_price/total_weight,2)

            ultra_write("Q3", LAST_VWAP)

            print("VWAP OPTIONCHAIN FALLBACK:", LAST_VWAP)

            return


        # =====================================================
        # 3️⃣ EMERGENCY FALLBACK — NEVER LEAVE VWAP EMPTY
        # =====================================================

        if LAST_VWAP is None:
            LAST_VWAP = ltp

        ultra_write("Q3", LAST_VWAP)

        print("VWAP EMERGENCY (LTP):", LAST_VWAP)


    except Exception as e:

        print("VWAP ERROR:", e)

    
    if LAST_VWAP is not None:
        set_state("vwap", LAST_VWAP)



#=====================================================

def expiry():

 current=safe("B4")

 if current:
  return current

 r=requests.post(
  "https://api.dhan.co/v2/optionchain/expirylist",
  headers=headers(),
  json={"UnderlyingScrip":13,"UnderlyingSeg":"IDX_I"}
 ).json()

 exp=r.get("data",[])

 if not exp:
  raise Exception("No expiry received")

 first=exp[0]

 ultra_write("B4",first)

 print("AUTO EXPIRY SET:",first)

 return first

# ================= OPTIONCHAIN (FIXED PARSER) =================

# ================= OPTIONCHAIN (ULTRA SAFE PARSER) =================

def optionchain():

    try:

        response = requests.post(
            "https://api.dhan.co/v2/optionchain",
            headers=headers(),
            json={
                "UnderlyingScrip":13,
                "UnderlyingSeg":"IDX_I",
                "Expiry":expiry()
            }
        )

        r = response.json()

        # ---------- DEBUG ----------
        print("OPTIONCHAIN RAW KEYS:", list(r.keys()))

        # ---------- ERROR HANDLING ----------
        # Some Dhan errors return:
        # { "status":"error", "errorCode":811 ... }

        if r.get("status") == "error":

            error_code = str(r.get("errorCode",""))

            if error_code == "811":

                print("⚠️ INVALID EXPIRY (811) — resetting expiry")

                ultra_write("B4","")   # force expiry refresh

                return {}

            print("OPTIONCHAIN ERROR:", r)
            return {}

        # ---------- NORMAL DATA ----------
        data = r.get("data", {})

        if "oc" in data:

            print("OPTIONCHAIN OK — strikes loaded:", len(data["oc"]))

            return data["oc"]

        # ---------- FALLBACK FORMAT ----------
        # Sometimes API returns strikes directly

        if isinstance(data, dict):

            print("OPTIONCHAIN ALT FORMAT DETECTED")

            return data

        print("OPTIONCHAIN UNKNOWN STRUCTURE:", r)

        return {}

    except Exception as e:

        print("OPTIONCHAIN ERROR:", e)

        return {}


# ================= GAMMA ENGINE =================

def gamma_engine(ltp,oc):

    atm=round(ltp/50)*50

    ce_build=0
    pe_build=0

    for i in range(-150,200,50):

        k=f"{atm+i:.6f}"

        if k not in oc:
            continue

        ce=oc[k].get("ce")
        pe=oc[k].get("pe")

        if ce and ce["oi"]>ce.get("previous_oi",0):
            ce_build+=1

        if pe and pe["oi"]>pe.get("previous_oi",0):
            pe_build+=1

    state="NEUTRAL"

    velocity = get_state("premium_velocity",0)

    if ce_build>=3 and pe_build>=3:

        if velocity > 0.5:
            state="TRUE GAMMA UP 🚀"
        else:
            state="PREMIUM TRAP ⚠️"

    elif ce_build>pe_build:
        state="TRUE GAMMA UP 🚀"

    elif pe_build>ce_build:
        state="TRUE GAMMA DOWN 🔻"

    set_state("gamma", state)
    ultra_write("M17",state)



def god_mode_engine(ltp, oc):

    atm = round(ltp/50)*50

    ce_unwind = 0
    pe_unwind = 0
    ce_build = 0
    pe_build = 0

    for shift in [-100,-50,0,50,100]:

        k=f"{atm+shift:.6f}"

        if k not in oc:
            continue

        ce=oc[k].get("ce")
        pe=oc[k].get("pe")

        if ce:
            if ce.get("oi",0) < ce.get("previous_oi",0):
                ce_unwind += 1
            else:
                ce_build += 1

        if pe:
            if pe.get("oi",0) < pe.get("previous_oi",0):
                pe_unwind += 1
            else:
                pe_build += 1

    signal = "NO GOD SIGNAL"

    # Liquidity vacuum up
    if ce_unwind >=3 and pe_build <=1:
        signal = "🔥 GOD MODE LONG"

    # Liquidity vacuum down
    elif pe_unwind >=3 and ce_build <=1:
        signal = "🔻 GOD MODE SHORT"

    set_state("god_signal", signal)
    ultra_write("N9", signal)
# ================= DEALER INTENT RADAR =================

def dealer_intent_radar(ltp, oc):

    atm = round(ltp/50)*50

    ce_build = 0
    ce_unwind = 0
    pe_build = 0
    pe_unwind = 0

    for shift in [-100,-50,0,50,100]:

        k = f"{atm+shift:.6f}"

        if k not in oc:
            continue

        ce = oc[k].get("ce")
        pe = oc[k].get("pe")

        if ce:
            if ce.get("oi",0) > ce.get("previous_oi",0):
                ce_build += 1
            else:
                ce_unwind += 1

        if pe:
            if pe.get("oi",0) > pe.get("previous_oi",0):
                pe_build += 1
            else:
                pe_unwind += 1

    intent = "NEUTRAL"

    # hidden bullish prep
    if pe_build >=3 and ce_unwind >=2:
        intent = "🚀 DEALER PREPARE LONG"

    # hidden bearish prep
    elif ce_build >=3 and pe_unwind >=2:
        intent = "🔻 DEALER PREPARE SHORT"

    ultra_write("N25", intent)
# ================= TRUE SNIPER V2 — PREDICTIVE GAMMA =================

LAST_PREMIUM = None
LAST_PREMIUM_TIME = None

def predictive_gamma_engine(ltp, oc):

    global LAST_PREMIUM, LAST_PREMIUM_TIME

    atm = round(ltp/50)*50

    ce_pressure = 0
    pe_pressure = 0
    premium_sum = 0
    count = 0

    for shift in [-100,-50,0,50,100]:

        k = f"{atm+shift:.6f}"

        if k not in oc:
            continue

        ce = oc[k].get("ce")
        pe = oc[k].get("pe")

        if ce:
            ce_pressure += ce.get("oi",0) - ce.get("previous_oi",0)
            premium_sum += ce.get("last_price",0)
            count += 1

        if pe:
            pe_pressure += pe.get("oi",0) - pe.get("previous_oi",0)
            premium_sum += pe.get("last_price",0)
            count += 1

    signal = "NONE"

    # ---------- EARLY OI IMBALANCE ----------
    if ce_pressure > abs(pe_pressure)*1.5:
        signal = "EARLY UP PRESSURE"

    elif pe_pressure > abs(ce_pressure)*1.5:
        signal = "EARLY DOWN PRESSURE"

    # ---------- PREMIUM VELOCITY ----------
    if count > 0:

        avg_premium = premium_sum / count
        now = time.time()

        velocity = 0

        if LAST_PREMIUM and LAST_PREMIUM_TIME:

            dt = now - LAST_PREMIUM_TIME

            if dt > 0:
                velocity = abs(avg_premium - LAST_PREMIUM) / dt

        LAST_PREMIUM = avg_premium
        LAST_PREMIUM_TIME = now

        set_state("premium_velocity", velocity)

        if velocity > 0.8 and "PRESSURE" in signal:
            signal = "PREDICTIVE GAMMA IGNITION"

    set_state("predictive_gamma", signal)
#===============================DEALER =====================

def dealer_trap_engine(ltp, oc):

    relation = get_state("relation")
    gamma_state = get_state("gamma")
    vwap_val = get_state("vwap")


    resistance = safe("B31")
    support = safe("B33")

    trap = "NO TRAP"

    try:
        vwap_val = float(vwap_val)
        resistance = float(resistance) if resistance else None
        support = float(support) if support else None
    except:
        ultra_write("N19", trap)
        return




    atm = round(ltp/50)*50

    ce_build = 0
    pe_build = 0

    # ----- dealer behaviour -----
    for shift in [-100,-50,0,50,100]:

        strike = atm + shift
        key = f"{strike:.6f}"

        if key not in oc:
            continue

        ce = oc[key].get("ce")
        pe = oc[key].get("pe")

        if ce and ce.get("oi",0) > ce.get("previous_oi",0):
            ce_build += 1

        if pe and pe.get("oi",0) > pe.get("previous_oi",0):
            pe_build += 1

    # ----- distance from walls -----
    near_resistance = abs(ltp - resistance) <= 30
    near_support = abs(ltp - support) <= 30

    # ---------- PRO TRAP LOGIC ----------

    # Bull trap
    if (relation=="ABOVE CPR" and
        ltp>vwap_val and
        near_resistance and
        ce_build>pe_build and
        "UP" in gamma_state):

        trap="🔥 DEALER BULL TRAP PRO"

    # Bear trap
    elif (relation=="BELOW CPR" and
          ltp<vwap_val and
          near_support and
          pe_build>ce_build and
          "DOWN" in gamma_state):

        trap="🔥 DEALER BEAR TRAP PRO"

    ultra_write("N19", trap)

# ================= INSIDE CPR PRO MODE =================

def inside_cpr_pro_engine(ltp, oc):

    relation = get_state("relation")

    if relation != "INSIDE CPR":
        ultra_write("N21","OUTSIDE CPR")
        return

    try:
        tc=float(safe("H4"))
        pivot=float(safe("H5"))
        bc=float(safe("H6"))
    except:
        return

    atm=round(ltp/50)*50

    ce_build=0
    pe_build=0
    ce_unwind=0
    pe_unwind=0

    for shift in [-100,-50,0,50,100]:

        key=f"{atm+shift:.6f}"

        if key not in oc:
            continue

        ce=oc[key].get("ce")
        pe=oc[key].get("pe")

        if ce:
            if ce.get("oi",0)>ce.get("previous_oi",0):
                ce_build+=1
            else:
                ce_unwind+=1

        if pe:
            if pe.get("oi",0)>pe.get("previous_oi",0):
                pe_build+=1
            else:
                pe_unwind+=1

    signal="INSIDE CPR — WAIT"

    # BC Defense
    if abs(ltp-bc)<=20 and pe_build>ce_build:
        signal="🔥 BC DEFENSE → CE BIAS"

    # TC Rejection
    elif abs(ltp-tc)<=20 and ce_build>pe_build:
        signal="🔥 TC REJECTION → PE BIAS"

    # Pivot Gamma Flip
    elif abs(ltp-pivot)<=20:
        if ce_unwind>pe_unwind:
            signal="🚀 PIVOT GAMMA FLIP UP"
        elif pe_unwind>ce_unwind:
            signal="🔻 PIVOT GAMMA FLIP DOWN"

    # Compression
    elif ce_build>=3 and pe_build>=3:
        signal="⚡ CPR COMPRESSION — GAMMA BUILD"

    ultra_write("N21",signal)
# ================= CPR MAGNET + LIQUIDITY ENGINE =================

def liquidity_target_engine(ltp):

    relation = get_state("relation")

    try:
        tc=float(safe("H4"))
        pivot=float(safe("H5"))
        bc=float(safe("H6"))

        resistance=float(safe("B31"))
        support=float(safe("B33"))
        maxpain=float(safe("B32"))

    except:
        return

    # --- Fetch PDH / PDL (ULTRA SAFE) ---

    pdh = None
    pdl = None

    try:

        hist = requests.post(
            "https://api.dhan.co/v2/charts/historical",
            headers=headers(),
            json={
                "securityId":"13",
                "exchangeSegment":"IDX_I",
                "instrument":"INDEX",
                "fromDate":(datetime.date.today()-datetime.timedelta(days=5)).strftime("%Y-%m-%d"),
                "toDate":datetime.date.today().strftime("%Y-%m-%d")
            }
        ).json()

        highs = []
        lows = []

        if "high" in hist:
            highs = hist.get("high",[])
            lows  = hist.get("low",[])

        elif "data" in hist:
            data = hist.get("data",{})
            highs = data.get("high",[])
            lows  = data.get("low",[])

        if len(highs) >= 2 and len(lows) >= 2:
            pdh = highs[-2]
            pdl = lows[-2]

    except Exception as e:

        print("LIQUIDITY HIST ERROR:", e)

    target="NO CLEAR TARGET"

    # ---- INSIDE CPR MAGNET ----
    if relation=="INSIDE CPR":

        if abs(ltp-pivot)<=20:
            target="🧲 CPR PIVOT MAGNET"

        elif ltp<pivot:
            target="🎯 TARGET → TC (upper magnet)"

        elif ltp>pivot:
            target="🎯 TARGET → BC (lower magnet)"

    # ---- ABOVE CPR ----
    elif relation=="ABOVE CPR":

        if pdh and abs(ltp-pdh)<=50:
            target="🎯 TARGET → PDH LIQUIDITY"

        elif resistance:
            target=f"🎯 TARGET → RESIST {int(resistance)}"

    # ---- BELOW CPR ----
    elif relation=="BELOW CPR":

        if pdl and abs(ltp-pdl)<=50:
            target="🎯 TARGET → PDL LIQUIDITY"

        elif support:
            target=f"🎯 TARGET → SUPPORT {int(support)}"

    # ---- MAX PAIN PIN ----
    if maxpain and abs(ltp-maxpain)<=30:
        target="🧲 MAX PAIN PIN ZONE"

    ultra_write("N23",target)
# ================= INSTITUTIONAL CONFIRMATION =================

LAST_LTP = None
LAST_TIME = None

def institutional_confirmation(ltp, oc):

    global LAST_LTP, LAST_TIME

    relation = get_state("relation")
    gamma_state = get_state("gamma")
    vwap_val = get_state("vwap")


    status = "NO FLOW"
    set_state("flow", status)

    try:
        vwap_val = float(vwap_val)
    except:
        ultra_write("N17", status)
        return

    # ---------- SPEED / VELOCITY ----------
    now = time.time()
    velocity = 0

    if LAST_LTP is not None and LAST_TIME is not None:

        dt = now - LAST_TIME

        if dt > 0:
            velocity = abs(ltp - LAST_LTP) / dt

    LAST_LTP = ltp
    LAST_TIME = now

    # ---------- ULTRA FLOW DETECTION (NO GAMMA REQUIRED) ----------

    if relation == "ABOVE CPR" and ltp > vwap_val:

        if velocity > 0.6:
            status = "ULTRA FLOW 🚀"
        elif velocity > 0.25:
            status = "PROBABLE FLOW"

    elif relation == "BELOW CPR" and ltp < vwap_val:

        if velocity > 0.6:
            status = "ULTRA FLOW 🔻"
        elif velocity > 0.25:
            status = "PROBABLE FLOW"

    # ---------- GAMMA ACCELERATION LABEL ----------
    if "UP" in gamma_state and "FLOW" in status:
        status += " + GAMMA ACCEL"

    elif "DOWN" in gamma_state and "FLOW" in status:
        status += " + GAMMA ACCEL"

    ultra_write("N17", status)
# ================= OPENING SNIPER =================

OPENING_DONE = False

def opening_sniper(ltp):

    global OPENING_DONE

    if OPENING_DONE:
        return

    relation = get_state("relation")
    vwap_val = get_state("vwap")


    try:
        vwap_val = float(vwap_val)
    except:
        return

    sniper = "WAIT"

    # ---- Opening bias logic ----
    if relation == "ABOVE CPR" and ltp > vwap_val:
        sniper = "OPENING LONG SNIPER 🚀"

    elif relation == "BELOW CPR" and ltp < vwap_val:
        sniper = "OPENING SHORT SNIPER 🔻"

    ultra_write("N3", sniper)

    OPENING_DONE = True


# ==========================================================
# ================= TRUE SNIPER MODE =================
# ================= TRUE SNIPER MODE =================

def true_sniper_mode():

    predictive = get_state("predictive_gamma")
    relation = get_state("relation")
    vwap_val = get_state("vwap")
    gamma = get_state("gamma")
    inst_flow = get_state("flow")
    god = get_state("god_signal")
    floating = safe("A17")

    sniper = "WAIT"

    try:
        vwap_val = float(vwap_val)
        ltp = float(safe("C6"))
    except:
        ultra_write("N5", sniper)
        return

    # ================= PRIORITY ORDER =================

    # ---- GOD MODE ----
    if "GOD MODE LONG" in god:
        sniper = "⚡ GOD EARLY LONG"

    elif "GOD MODE SHORT" in god:
        sniper = "⚡ GOD EARLY SHORT"

    # ---- NEWS MODE ----
    elif NEWS_MODE and relation=="ABOVE CPR" and ltp > vwap_val:
        sniper = "🚨 NEWS SNIPER LONG"

    elif NEWS_MODE and relation=="BELOW CPR" and ltp < vwap_val:
        sniper = "🚨 NEWS SNIPER SHORT"

    # ---- PREDICTIVE GAMMA ----
    elif predictive == "PREDICTIVE GAMMA IGNITION":

        if relation=="ABOVE CPR" and ltp > vwap_val:
            sniper = "⚡ V2 PREDICTIVE LONG"

        elif relation=="BELOW CPR" and ltp < vwap_val:
            sniper = "⚡ V2 PREDICTIVE SHORT"

    # ---- EARLY SNIPER ----
    elif (relation=="ABOVE CPR"
          and ltp > vwap_val
          and ("STRONG" in floating or "SUPER STRONG" in floating)
          and ("FLOW" in inst_flow)
          and "PREMIUM TRAP" not in gamma):

        sniper = "⚡ EARLY SNIPER LONG"

    elif (relation=="BELOW CPR"
          and ltp < vwap_val
          and ("STRONG" in floating or "SUPER STRONG" in floating)
          and ("FLOW" in inst_flow)
          and "PREMIUM TRAP" not in gamma):

        sniper = "⚡ EARLY SNIPER SHORT"

    # ---- TRUE SNIPER FULL ----
    elif (relation=="ABOVE CPR"
          and ltp > vwap_val
          and ("STRONG" in floating or "SUPER STRONG" in floating)
          and ("FLOW" in inst_flow)
          and "UP" in gamma):

        sniper = "🔥 TRUE SNIPER LONG READY"

    elif (relation=="BELOW CPR"
          and ltp < vwap_val
          and ("STRONG" in floating or "SUPER STRONG" in floating)
          and ("FLOW" in inst_flow)
          and "DOWN" in gamma):

        sniper = "🔻 TRUE SNIPER SHORT READY"

    ultra_write("N5", sniper)



# ================= SNIPER ANTI-TRAP FILTER =================

def sniper_antitrap_filter():

    sniper = safe("N5")
    gamma = safe("M17")
    inst_flow = safe("N17")
    target = safe("N23")
    floating = safe("A17")

    filtered = sniper

    if "PREMIUM TRAP" in gamma:
        filtered = "⚠️ SNIPER BLOCKED — PREMIUM TRAP"

    elif "MAX PAIN" in target:
        filtered = "⚠️ SNIPER BLOCKED — MAGNET ZONE"

    elif "NO FLOW" in inst_flow:
        filtered = "⚠️ SNIPER BLOCKED — NO FLOW"

    elif "WEAKENING" in floating:
        filtered = "⚠️ SNIPER BLOCKED — WEAK STRUCTURE"

    ultra_write("N6", filtered)


# ================= AUTO SNIPER EXECUTION =================

def auto_sniper_execution():

    sniper_ready = safe("N5")
    sniper_block = safe("N6")
    gamma = safe("M17")
    flow = safe("N17")
    relation = safe("H9")
    vwap_val = safe("Q3")
    ltp = safe("C6")

    trend = safe("N27")
    trend_regime = safe("N29")
    dark = safe("N31")

    execution = "WAIT"

    # ---------- SAFE CONVERSION ----------
    try:
        vwap_val = float(vwap_val)
        ltp = float(ltp)
    except:
        ultra_write("N7", execution)
        return

    # ---------- BLOCK FILTER ----------
    if "BLOCKED" in sniper_block:
        ultra_write("N7", execution)
        return

    # =====================================================
    # TRUE SNIPER EXECUTION (FULL CONFIRMATION)
    # =====================================================

    if ("LONG READY" in sniper_ready
        and "UP" in gamma
        and "FLOW" in flow
        and relation == "ABOVE CPR"
        and ltp > vwap_val):

        execution = "🔥 AUTO EXECUTE CE"

    elif ("SHORT READY" in sniper_ready
          and "DOWN" in gamma
          and "FLOW" in flow
          and relation == "BELOW CPR"
          and ltp < vwap_val):

        execution = "🔻 AUTO EXECUTE PE"

    # =====================================================
    # TREND CONTINUATION ENGINE
    # =====================================================

    elif ("TREND CONTINUATION SHORT" in trend
          and relation == "BELOW CPR"
          and ltp < vwap_val):

        execution = "🔻 TREND AUTO PE"

    elif ("TREND CONTINUATION LONG" in trend
          and relation == "ABOVE CPR"
          and ltp > vwap_val):

        execution = "🔥 TREND AUTO CE"

    # =====================================================
    # DEALER TREND INTELLIGENCE
    # =====================================================

    elif ("DEALER TREND SHORT" in trend_regime
          and relation == "BELOW CPR"
          and ltp < vwap_val):

        execution = "🔻 DEALER TREND PE"

    elif ("DEALER TREND LONG" in trend_regime
          and relation == "ABOVE CPR"
          and ltp > vwap_val):

        execution = "🔥 DEALER TREND CE"

    # =====================================================
    # DARK POOL EARLY ENTRY
    # =====================================================

    elif ("DARK POOL SHORT BUILD" in dark
          and relation == "BELOW CPR"
          and ltp < vwap_val):

        execution = "🌑 EARLY DARK PE ENTRY"

    # ---------- FINAL WRITE ----------
    ultra_write("N7", execution)
# ================= ABSORPTION RADAR =================

def absorption_radar_engine(ltp, oc):

    relation = safe("H9")
    vwap_val = safe("Q3")

    signal = "NO ABSORPTION"

    try:
        vwap_val = float(vwap_val)
    except:
        ultra_write("N33", signal)
        return

    atm = round(ltp/50)*50
    key = f"{atm:.6f}"

    if key not in oc:
        ultra_write("N33", signal)
        return

    ce = oc[key].get("ce")
    pe = oc[key].get("pe")

    if not ce or not pe:
        ultra_write("N33", signal)
        return

    ce_prem = ce.get("last_price",0)
    pe_prem = pe.get("last_price",0)

    ce_oi = ce.get("oi",0)
    ce_prev = ce.get("previous_oi",0)

    pe_oi = pe.get("oi",0)
    pe_prev = pe.get("previous_oi",0)

    # ---- Downside absorption ----
    if (relation=="BELOW CPR"
        and pe_oi > pe_prev
        and pe_prem <= pe.get("previous_close",0)):

        signal="🟢 DOWNSIDE ABSORPTION"

    # ---- Upside absorption ----
    elif (relation=="ABOVE CPR"
          and ce_oi > ce_prev
          and ce_prem <= ce.get("previous_close",0)):

        signal="🔴 UPSIDE ABSORPTION"

    ultra_write("N33", signal)
# ================= LIQUIDITY VACUUM RADAR =================
# ================= LIQUIDITY VACUUM RADAR =================

LAST_LV_LTP = None

def liquidity_vacuum_radar(ltp, oc):

    global LAST_LV_LTP

    relation = safe("H9")
    vwap_val = safe("Q3")

    signal = "NO VACUUM"

    try:
        vwap_val = float(vwap_val)
    except:
        ultra_write("N35", signal)
        return

    atm = round(ltp/50)*50
    key = f"{atm:.6f}"

    if key not in oc:
        ultra_write("N35", signal)
        return

    ce = oc[key].get("ce")
    pe = oc[key].get("pe")

    if not ce or not pe:
        ultra_write("N35", signal)
        return

    # ---- PRICE ACCELERATION ----
    accel = 0
    if LAST_LV_LTP is not None:
        accel = abs(ltp - LAST_LV_LTP)

    LAST_LV_LTP = ltp

    # ---- DOWN VACUUM ----
    pe_prem_jump = pe.get("last_price",0) > pe.get("previous_close",0)*1.05
    pe_oi_flat = abs(pe.get("oi",0) - pe.get("previous_oi",0)) < 1500

    if (relation=="BELOW CPR"
        and ltp < vwap_val
        and accel > 25
        and pe_prem_jump
        and pe_oi_flat):

        signal="🔻 LIQUIDITY VACUUM DOWN"

    # ---- UP VACUUM ----
    ce_prem_jump = ce.get("last_price",0) > ce.get("previous_close",0)*1.05
    ce_oi_flat = abs(ce.get("oi",0) - ce.get("previous_oi",0)) < 1500

    if (relation=="ABOVE CPR"
        and ltp > vwap_val
        and accel > 25
        and ce_prem_jump
        and ce_oi_flat):

        signal="🚀 LIQUIDITY VACUUM UP"

    ultra_write("N35", signal)

# ================= INSTITUTIONAL BREAKOUT RADAR =================

LAST_BREAK_LTP = None
LAST_BREAK_TIME = None

def breakout_radar_engine(ltp):

    global LAST_BREAK_LTP, LAST_BREAK_TIME

    relation = safe("H9")
    vwap_val = safe("Q3")

    signal = "NO BREAKOUT"

    try:
        vwap_val = float(vwap_val)
    except:
        ultra_write("N11", signal)
        return

    now = time.time()
    velocity = 0

    if LAST_BREAK_LTP is not None and LAST_BREAK_TIME is not None:

        dt = now - LAST_BREAK_TIME

        if dt > 0:
            velocity = abs(ltp - LAST_BREAK_LTP) / dt

    LAST_BREAK_LTP = ltp
    LAST_BREAK_TIME = now

    # -------- INSTITUTIONAL BREAKOUT DETECTION --------

    if relation == "ABOVE CPR" and ltp > vwap_val:

        if velocity > 0.7:
            signal = "🔥 INSTITUTIONAL BREAKOUT UP"

    elif relation == "BELOW CPR" and ltp < vwap_val:

        if velocity > 0.7:
            signal = "🔻 INSTITUTIONAL BREAKOUT DOWN"

    ultra_write("N11", signal)
# ================= GAMMA ACCELERATION ENGINE =================

def gamma_acceleration_engine():

    gamma = safe("M17")
    flow = safe("N17")
    floating = safe("A17")
    sniper_ready = safe("N5")

    accel = "NO ACCELERATION"

    # acceleration conditions
    if (("STRONG" in floating or "SUPER STRONG" in floating) and
    "FLOW" in flow and
    "PREMIUM TRAP" not in gamma):


        if "LONG READY" in sniper_ready:
            accel = "🚀 GAMMA ACCELERATION UP"

        elif "SHORT READY" in sniper_ready:
            accel = "🔻 GAMMA ACCELERATION DOWN"

    ultra_write("N8", accel)
# ================= DEALER TREND INTELLIGENCE =================

TREND_REGIME = "NONE"

def dealer_trend_intelligence(ltp):

    global TREND_REGIME

    relation = safe("H9")
    vwap_val = safe("Q3")
    floating = safe("A17")
    gamma = safe("M17")

    try:
        vwap_val = float(vwap_val)
    except:
        ultra_write("N29","NONE")
        return

    regime = "NONE"

    # ---- CPR FLIP DETECTION ----
    prev_relation = get_state("prev_relation")

    if prev_relation:

        # ABOVE → BELOW flip
        if prev_relation == "ABOVE CPR" and relation == "BELOW CPR":
            regime = "🔥 DEALER TREND SHORT"

        # BELOW → ABOVE flip
        elif prev_relation == "BELOW CPR" and relation == "ABOVE CPR":
            regime = "🚀 DEALER TREND LONG"

    # ---- TREND CONTINUATION ----
    elif (relation=="BELOW CPR"
          and ltp < vwap_val
          and "STRONG" in floating
          and "PREMIUM TRAP" not in gamma):

        regime="🔻 TREND CONTINUATION"

    elif (relation=="ABOVE CPR"
          and ltp > vwap_val
          and "STRONG" in floating
          and "PREMIUM TRAP" not in gamma):

        regime="🚀 TREND CONTINUATION"

    TREND_REGIME = regime

    set_state("prev_relation", relation)

    ultra_write("N29", regime)
# ================= TREND CONTINUATION ENGINE =================

TREND_MEMORY = None

def trend_continuation_engine(ltp):

    global TREND_MEMORY

    relation = safe("H9")
    vwap_val = safe("Q3")
    floating = safe("A17")
    gamma = safe("M17")

    trend_signal = "NO TREND"

    try:
        vwap_val = float(vwap_val)
    except:
        ultra_write("N27", trend_signal)
        return

    # ---- Initialize memory ----
    if TREND_MEMORY is None:
        TREND_MEMORY = ltp
        ultra_write("N27", trend_signal)
        return

    # ---- TREND LOGIC ----

    # PE TREND CONTINUATION
    if (relation == "BELOW CPR"
        and ltp < vwap_val
        and ("STRONG" in floating or "SUPER STRONG" in floating)
        and "PREMIUM TRAP" not in gamma
        and ltp < TREND_MEMORY):

        trend_signal = "🔻 TREND CONTINUATION SHORT"

    # CE TREND CONTINUATION
    elif (relation == "ABOVE CPR"
          and ltp > vwap_val
          and ("STRONG" in floating or "SUPER STRONG" in floating)
          and "PREMIUM TRAP" not in gamma
          and ltp > TREND_MEMORY):

        trend_signal = "🚀 TREND CONTINUATION LONG"

    TREND_MEMORY = ltp

    ultra_write("N27", trend_signal)
# ================= DARK POOL ENTRY ENGINE =================

LAST_DARK_PREM = None

def dark_pool_entry_engine(ltp, oc):

    global LAST_DARK_PREM

    relation = safe("H9")
    vwap_val = safe("Q3")
    gamma = safe("M17")

    signal = "NO DARK SIGNAL"

    try:
        vwap_val = float(vwap_val)
    except:
        ultra_write("N31", signal)
        return

    atm = round(ltp/50)*50
    key = f"{atm:.6f}"

    if key not in oc:
        ultra_write("N31", signal)
        return

    ce = oc[key].get("ce")
    pe = oc[key].get("pe")

    if not ce or not pe:
        ultra_write("N31", signal)
        return

    pe_prem = pe.get("last_price",0)
    ce_oi = ce.get("oi",0)
    ce_prev = ce.get("previous_oi",0)
    pe_oi = pe.get("oi",0)
    pe_prev = pe.get("previous_oi",0)

    # ---- Memory ----
    if LAST_DARK_PREM is None:
        LAST_DARK_PREM = pe_prem
        ultra_write("N31", signal)
        return

    prem_rising = pe_prem > LAST_DARK_PREM
    ce_building = ce_oi >= ce_prev
    pe_not_exploding = abs(pe_oi - pe_prev) < 2000

    # ---- PE DARK ENTRY ----
    if (relation=="BELOW CPR"
        and ltp < vwap_val
        and prem_rising
        and ce_building
        and pe_not_exploding
        and "PREMIUM TRAP" not in gamma):

        signal="🌑 DARK POOL SHORT BUILD"

    LAST_DARK_PREM = pe_prem

    ultra_write("N31", signal)
# ================= FLOATING PIVOT PRO ENGINE =================

option_ranges = {}

def floating_pivot(high, low, ltp):

    p = (high + low + ltp)/3
    rng = high - low

    wp = (2*p) - high
    t1 = (2*p) - low
    t2 = p + rng
    t3 = high + 2*(p-low)

    if ltp > t1:
        status = "SUPER STRONG 🚀"
    elif wp < ltp <= t1:
        status = "STRONG"
    elif p < ltp <= wp:
        status = "WEAKENING"
    else:
        status = "INVALIDATED"

    return p, wp, t1, t2, t3, status



# ================= EMA CALCULATOR =================

def calculate_ema(values, period):

    ema = []

    k = 2/(period+1)

    for i,v in enumerate(values):

        if i == 0:
            ema.append(v)
        else:
            ema.append(v*k + ema[-1]*(1-k))

    return ema
# ================= GAMMA DEALER ENGINE =================

def gamma_filter(atm, decision, oc):

    try:

        strikes = list(oc.keys())
        strikes = sorted([float(s) for s in strikes])

        # find ATM index
        idx = strikes.index(float(f"{atm:.6f}"))

        nearby = strikes[max(0,idx-2): idx+3]

        oi_change_total = 0
        premium_speed = 0

        for s in nearby:

            data = oc.get(f"{s:.6f}",{}).get(decision.lower())

            if not data:
                continue

            oi = data.get("oi",0)
            prev_oi = data.get("previous_oi",0)
            ltp = data.get("last_price",0)
            prev_close = data.get("previous_close_price",0)

            oi_change_total += (oi - prev_oi)
            premium_speed += (ltp - prev_close)

        # ----- Decision Logic -----

        if premium_speed > 0 and oi_change_total < 0:
            return "GAMMA BLAST 🚀"

        elif premium_speed > 0 and oi_change_total > 0:
            return "DEALER TRAP ⚠️"

        else:
            return "NEUTRAL FLOW"

    except Exception as e:
        print("GAMMA ERROR:",e)

    return "UNKNOWN"


# ================= STRIKE SELECTOR =================

# ================= STRIKE SELECTOR =================

option_high_low = {}

# ---------- TRUE SESSION OHLC (FROM 9:15 AM) ----------
def get_session_range(security_id):

    try:

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        payload = {
            "securityId": str(security_id),
            "exchangeSegment": "NSE_FNO",
            "instrument": "OPTIDX",
            "interval": "1",
            "fromDate": f"{today} 09:15:00",
            "toDate": now
        }

        r = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=headers(),
            json=payload
        ).json()

        highs = r.get("high",[])
        lows  = r.get("low",[])

        if not highs or not lows:
            return None,None

        return max(highs),min(lows)

    except Exception as e:
        print("SESSION RANGE ERROR:",e)

    return None,None


# ---------- SMART RANGE ENGINE ----------
def update_range(key_name, session_high, session_low, ltp):

    if key_name not in option_high_low:

        option_high_low[key_name] = {
            "session_high": session_high if session_high else ltp,
            "session_low": session_low if session_low else ltp,
            "micro_high": ltp,
            "micro_low": ltp
        }

    r = option_high_low[key_name]

    r["micro_high"] = max(r["micro_high"], ltp)
    r["micro_low"]  = min(r["micro_low"], ltp)

    high = max(r["session_high"], r["micro_high"])
    low  = min(r["session_low"], r["micro_low"])

    return high, low

# ================= UNIVERSAL STRIKE ENGINE (AUTO + MANUAL) =================

def process_strike_floating(strike, side, oc, sheet_range):

    key = f"{strike:.6f}"

    opt = oc.get(key, {}).get(side.lower())

    if not opt:
        return

    ltp_opt = float(opt.get("last_price",0))
    security_id = opt.get("security_id")

    key_name = f"{strike}_{side}"

    session_high, session_low = get_session_range(security_id)

    high, low = update_range(key_name, session_high, session_low, ltp_opt)

    p, wp, t1, t2, t3, status = floating_pivot(high, low, ltp_opt)

    gamma_status = gamma_filter(strike, side, oc)

    ws.update(
        values=[[f"{strike} {side}", ltp_opt,
                 round(p,2), round(wp,2),
                 round(t1,2), round(t2,2),
                 round(t3,2), status + " | " + gamma_status]],
        range_name=sheet_range
    )

# ---------- AUTO STRIKE ----------
def auto_strike_floating(ltp, oc):

    relation = safe("H9")

    if relation == "ABOVE CPR":
        decision = "CE"
    elif relation == "BELOW CPR":
        decision = "PE"
    else:
        return

    atm = round(ltp/50)*50

    process_strike_floating(atm, decision, oc, "A17:H17")


# ---------- MANUAL STRIKE ----------
def manual_strike_floating(oc):

    manual = safe("A23")

    if not manual:
        return

    try:
        strike = int(manual.split()[0])
        side = manual.split()[1].upper()
    except:
        return

    process_strike_floating(strike, side, oc, "A23:H23")


# ================= EMA SCALP ENGINE =================

def process_strike_ema_scalp(strike, side, oc, sheet_range):

    key = f"{strike:.6f}"
    opt = oc.get(key, {}).get(side.lower())

    if not opt:
        return

    security_id = opt.get("security_id")

    # ---- DEFAULT EMA STATUS ----
    ema_status = "NO INTRADAY DATA"

    try:

        closes = fetch_intraday_closes(security_id)

        # Only calculate EMA if enough candles exist
        if len(closes) >= 30:

            ema9 = calculate_ema(closes,9)
            ema21 = calculate_ema(closes,21)

            if ema9[-1] > ema21[-1] and ema9[-2] <= ema21[-2]:
                ema_status = "EMA CROSS UP 🚀"

            elif ema9[-1] < ema21[-1] and ema9[-2] >= ema21[-2]:
                ema_status = "EMA CROSS DOWN 🔻"

            else:
                ema_status = "EMA NEUTRAL"

    except Exception as e:
        print("EMA SCALP ERROR:", e)


    # ---- ALWAYS CALCULATE FLOATING PIVOT ----
    ltp_opt = float(opt.get("last_price",0))

    high, low = ltp_opt, ltp_opt

    p, wp, t1, t2, t3, status = floating_pivot(high, low, ltp_opt)

    ws.update(
        values=[[f"{strike} {side}",
                 ltp_opt,
                 round(wp,2),
                 round(p,2),
                 round(t1,2),
                 round(t2,2),
                 round(t3,2),
                 status,
                 ema_status]],
        range_name=sheet_range
    )
# ================= EMA COMPRESSION DETECTOR =================

def process_strike_ema_compression(strike, side, oc, sheet_range):

    key = f"{strike:.6f}"
    opt = oc.get(key, {}).get(side.lower())

    if not opt:
        return

    security_id = opt.get("security_id")

    # ---- DEFAULT STATUS ----
    compression_status = "NO INTRADAY DATA"

    try:

        closes = fetch_intraday_closes(security_id)

        if len(closes) >= 40:

            ema9 = calculate_ema(closes,9)
            ema21 = calculate_ema(closes,21)

            dist_now = abs(ema9[-1] - ema21[-1])
            dist_prev = abs(ema9[-2] - ema21[-2])

            compression_status = "NO COMPRESSION"

            if dist_now < 0.5 and dist_now < dist_prev:
                compression_status = "⚡ EMA COMPRESSION BUILDING"

            elif dist_now < 0.3:
                compression_status = "🔥 TIGHT COMPRESSION"

    except Exception as e:
        print("EMA COMPRESSION ERROR:", e)


    # ---- ALWAYS CALCULATE FLOATING PIVOT ----
    ltp_opt = float(opt.get("last_price",0))

    high, low = ltp_opt, ltp_opt

    p, wp, t1, t2, t3, status = floating_pivot(high, low, ltp_opt)

    ws.update(
        values=[[f"{strike} {side}",
                 ltp_opt,
                 round(wp,2),
                 round(p,2),
                 round(t1,2),
                 round(t2,2),
                 round(t3,2),
                 status,
                 compression_status]],
        range_name=sheet_range
    )
# ================= DEALER GAMMA ENGINE v6 =================

def institutional_strike_selector(ltp, oc):

    atm = round(ltp / 50) * 50

    candidate_strikes = [atm-100, atm-50, atm, atm+50, atm+100]

    best_ce = None
    best_pe = None

    best_ce_score = -999999
    best_pe_score = -999999

    atm_key = f"{atm:.6f}"
    atm_iv = oc.get(atm_key, {}).get("ce", {}).get("iv", 0)

    for strike in candidate_strikes:

        key = f"{strike:.6f}"

        data = oc.get(key)
        if not data:
            continue

        for side in ["ce","pe"]:

            opt = data.get(side)
            if not opt:
                continue

            oi = opt.get("oi",0)
            oi_change = opt.get("oi_change",0)
            delta = abs(opt.get("delta",0))
            iv = opt.get("iv",0)

            gamma_proxy = oi_change * delta
            iv_edge = iv - atm_iv

            score = (oi*0.5)+(gamma_proxy*0.3)+(iv_edge*0.2)

            if side=="ce" and score>best_ce_score:
                best_ce_score=score
                best_ce=strike

            if side=="pe" and score>best_pe_score:
                best_pe_score=score
                best_pe=strike

    return best_ce,best_pe
def institutional_floating(strike, side, oc, sheet_row):

    key=f"{strike:.6f}"
    opt=oc.get(key,{}).get(side.lower())

    if not opt:
        return

    ltp_opt=float(opt.get("last_price",0))
    security_id=opt.get("security_id")

    key_name=f"{strike}_{side}_INST"

    if key_name not in option_high_low:

        high,low=get_session_range(security_id)

        if high and low:
            option_high_low[key_name]={"high":high,"low":low}
        else:
            option_high_low[key_name]={"high":ltp_opt,"low":ltp_opt}

    option_high_low[key_name]["high"]=max(option_high_low[key_name]["high"],ltp_opt)
    option_high_low[key_name]["low"]=min(option_high_low[key_name]["low"],ltp_opt)

    high=option_high_low[key_name]["high"]
    low=option_high_low[key_name]["low"]

    p,wp,t1,t2,t3,status=floating_pivot(high,low,ltp_opt)

    ws.update(
        values=[[f"{strike} {side}",ltp_opt,
                 round(p,2),round(wp,2),
                 round(t1,2),round(t2,2),
                 round(t3,2),status]],
        range_name=f"{sheet_row}:H{sheet_row[1:]}"
    )

# ================= 3 LAYER DECISION ENGINE =================

def decision_engine():

    auto = safe("A17")
    inst = safe("A19")
    gamma = safe("M17")

    decision = "WAIT"

    # --- detect CE or PE in strings ---
    auto_ce = "CE" in auto
    auto_pe = "PE" in auto

    inst_ce = "CE" in inst
    inst_pe = "PE" in inst

    # ---- BUY CE ----
    if auto_ce and inst_ce and "PREMIUM TRAP" not in gamma:
        decision = "🔥 CE BUY"

    # ---- BUY PE ----
    elif auto_pe and inst_pe and "PREMIUM TRAP" not in gamma:
        decision = "🔻 PE BUY"

    else:
        decision = "⚠️ WAIT / CONFLICT"

    ultra_write("C10", decision)
# ================= SCALP MODE V3 SMART + GAMMA ACCEL + LIQUIDITY VACUUM =================

def scalp_mode_v2(ltp, oc):

    global LAST_LTP, PREV_LTP, LAST_CE_PREM, LAST_PE_PREM

    relation = STATE.get("relation")
    vwap_val = STATE.get("vwap")
    bc = STATE.get("bc")
    tc = STATE.get("tc")

    signal = ""
    strike = ""

    # ---- SAFE CHECK ----
    try:
        vwap_val = float(vwap_val)
        bc = float(bc)
        tc = float(tc)
    except:
        ultra_write("C12","")
        ultra_write("C21","")
        return

    if LAST_LTP is None:
        LAST_LTP = ltp
        return

    atm = round(ltp/50)*50
    k = f"{atm:.6f}"

    if k in oc:

        ce = oc[k].get("ce")
        pe = oc[k].get("pe")

        gamma_accel_up = False
        gamma_accel_down = False

        liquidity_vacuum_up = False
        liquidity_vacuum_down = False

        # ---- Gamma Acceleration ----
        if ce and LAST_CE_PREM:
            if ce.get("last_price",0) > LAST_CE_PREM * 1.05:
                gamma_accel_up = True

        if pe and LAST_PE_PREM:
            if pe.get("last_price",0) > LAST_PE_PREM * 1.05:
                gamma_accel_down = True

        # ---- Liquidity Vacuum Detection ----
        if ce:
            ce_prem_jump = ce.get("last_price",0) > ce.get("previous_close",0) * 1.03
            ce_oi_flat = abs(ce.get("oi",0) - ce.get("previous_oi",0)) < 1000
            if ce_prem_jump and ce_oi_flat:
                liquidity_vacuum_up = True

        if pe:
            pe_prem_jump = pe.get("last_price",0) > pe.get("previous_close",0) * 1.03
            pe_oi_flat = abs(pe.get("oi",0) - pe.get("previous_oi",0)) < 1000
            if pe_prem_jump and pe_oi_flat:
                liquidity_vacuum_down = True

        # ---------- PE SMART SCALP ----------
        if relation == "BELOW CPR" and ltp < vwap_val and ltp < LAST_LTP:

            if pe and ce:

                pe_oi_up = pe.get("oi",0) > pe.get("previous_oi",0)
                ce_unwind = ce.get("oi",0) < ce.get("previous_oi",0)

                premium_expand = pe.get("last_price",0) > pe.get("previous_close",0)

                if (pe_oi_up or ce_unwind) and \
                   (premium_expand or gamma_accel_down or liquidity_vacuum_down):

                    signal = "🔻 PE SMART SCALP"
                    strike = f"{atm} PE"

        # ---------- CE SMART SCALP ----------
        elif relation == "ABOVE CPR" and ltp > vwap_val and ltp > LAST_LTP:

            if ce and pe:

                ce_oi_up = ce.get("oi",0) > ce.get("previous_oi",0)
                pe_unwind = pe.get("oi",0) < pe.get("previous_oi",0)

                premium_expand = ce.get("last_price",0) > ce.get("previous_close",0)

                if (ce_oi_up or pe_unwind) and \
                   (premium_expand or gamma_accel_up or liquidity_vacuum_up):

                    signal = "🔺 CE SMART SCALP"
                    strike = f"{atm} CE"

        # ---- Update premium memory ----
        if ce:
            LAST_CE_PREM = ce.get("last_price",0)

        if pe:
            LAST_PE_PREM = pe.get("last_price",0)

    # ====================================================
    # INSIDE CPR DEFENSE MODE
    # ====================================================

    if relation == "INSIDE CPR":

        if abs(ltp - bc) <= 20 and ltp > LAST_LTP:
            signal = "🔵 CE DEFENSE SCALP"
            strike = f"{atm} CE"

        elif abs(ltp - tc) <= 20 and ltp < LAST_LTP:
            signal = "🔴 PE DEFENSE SCALP"
            strike = f"{atm} PE"

    # ====================================================
    # WRITE OR AUTO CLEAR
    # ====================================================

    if signal:

        pivot_weak = round(ltp*0.75,2)
        entry = round(ltp*0.5,2)
        t1 = round(ltp*1.3,2)
        t2 = round(ltp*1.6,2)
        t3 = round(ltp*2.1,2)

        ultra_write("C12", signal)
        ultra_write("C21", strike)
        ultra_write("D21", ltp)
        ultra_write("E21", pivot_weak)
        ultra_write("F21", entry)
        ultra_write("G21", t1)
        ultra_write("H21", t2)
        ultra_write("I21", t3)
        ultra_write("J21", "SCALP ACTIVE")

    else:

        ultra_write("C12","")
        ultra_write("C21","")
        ultra_write("D21","")
        ultra_write("E21","")
        ultra_write("F21","")
        ultra_write("G21","")
        ultra_write("H21","")
        ultra_write("I21","")
        ultra_write("J21","")

    PREV_LTP = LAST_LTP
    LAST_LTP = ltp

# ================= TRADE LOGGER ENGINE =================

def trade_log_engine():

    decision = safe("C10")

    if "BUY" not in decision:
        return

    log_ws = gc.open("N50").worksheet("TRADE_LOG")

    # --- read active strike row (A17 auto row) ---
    row = ws.get("A17:I17")[0]

    strike = row[0]

    # --- prevent duplicate logging ---
    existing = log_ws.col_values(1)

    if strike in existing:
        return

    # append trade
    log_ws.append_row(row + ["ACTIVE"])
def trade_exit_engine():

    log_ws = gc.open("N50").worksheet("TRADE_LOG")

    data = log_ws.get_all_values()

    for i,row in enumerate(data[1:], start=2):

        try:

            ltp = float(ws.acell("C6").value)

            pivot_weak = float(row[2])
            t1 = float(row[4])
            t3 = float(row[6])

            exit_reason = ""

            if ltp >= t3:
                exit_reason = "EXIT DUE T3"

            elif ltp < pivot_weak:
                exit_reason = "EXIT PIVOT BREAK"

            elif ltp < t1:
                exit_reason = "EXIT BELOW T1"

            if exit_reason:

                ultra_write(f"J{i}", exit_reason)

        except:
            continue
# ================= PERFORMANCE ANALYTICS =================

def performance_analytics():

    log_ws = gc.open("N50").worksheet("TRADE_LOG")

    data = log_ws.get_all_values()

    if len(data) <= 1:
        return

    total = 0
    wins = 0
    losses = 0

    for row in data[1:]:

        if len(row) < 10:
            continue

        exit_reason = row[9]

        if exit_reason == "":
            continue

        total += 1

        if "T3" in exit_reason:
            wins += 1
        else:
            losses += 1

    win_rate = (wins/total)*100 if total>0 else 0

    ultra_write("L2", total)
    ultra_write("L3", wins)
    ultra_write("L4", losses)
    ultra_write("L5", f"{round(win_rate,2)}%")
# ================= TRADE STATE LOCK =================

CURRENT_TRADE = None
# ================= LOCKED TRADE ENTRY =================

def locked_trade_entry():

    global CURRENT_TRADE

    if CURRENT_TRADE is not None:
        return  # already in trade

    decision = safe("C10")

    if "BUY" not in decision:
        return

    log_ws = gc.open("N50").worksheet("TRADE_LOG")

    row = ws.get("A17:I17")[0]

    entry_time = datetime.datetime.now().strftime("%H:%M:%S")

    log_ws.append_row(row + ["", entry_time, "", "ACTIVE"])

    last_row = len(log_ws.col_values(1))

    CURRENT_TRADE = {
        "row": last_row,
        "strike": row[0]
    }
# ================= LOCKED TRADE EXIT =================

def locked_trade_exit():

    global CURRENT_TRADE

    if CURRENT_TRADE is None:
        return

    log_ws = gc.open("N50").worksheet("TRADE_LOG")

    r = CURRENT_TRADE["row"]

    try:

        ltp = float(ws.acell("C6").value)

        pivot_weak = float(log_ws.acell(f"C{r}").value)
        t1 = float(log_ws.acell(f"E{r}").value)
        t3 = float(log_ws.acell(f"G{r}").value)

        exit_reason = ""

        if ltp >= t3:
            exit_reason = "EXIT DUE T3"

        elif ltp < pivot_weak:
            exit_reason = "EXIT PIVOT BREAK"

        elif ltp < t1:
            exit_reason = "EXIT BELOW T1"

        if exit_reason:

            exit_time = datetime.datetime.now().strftime("%H:%M:%S")

            ultra_write(f"J{r}", exit_reason)
            ultra_write(f"L{r}", exit_time)

            if "T3" in exit_reason:
                ultra_write(f"M{r}", "WIN")
            else:
                ultra_write(f"M{r}", "LOSS")

            CURRENT_TRADE = None

    except:
        pass

NEWS_MODE = False
PREV_LTP = None

def news_mode_engine(ltp):

    global NEWS_MODE, PREV_LTP

    if PREV_LTP is None:
        PREV_LTP = ltp
        return

    move = abs(ltp - PREV_LTP)

    if move >= 80:
        NEWS_MODE = True
    else:
        NEWS_MODE = False

    PREV_LTP = ltp

# ================= SHARED INTRADAY CACHE =================
# ================= SHARED INTRADAY CACHE =================

EMA_CANDLE_CACHE = {}

def fetch_intraday_closes(security_id):

    global EMA_CANDLE_CACHE

    if security_id in EMA_CANDLE_CACHE:
        return EMA_CANDLE_CACHE[security_id]

    try:

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        payload = {
            "securityId": str(security_id),
            "exchangeSegment": "NSE_FNO",
            "instrument": "OPTIDX",
            "interval": "1",
            "fromDate": f"{today} 09:15:00",
            "toDate": now
        }

        r = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=headers(),
            json=payload
        ).json()

        closes = []

        data = r.get("data", {})

        if "candles" in data:
            for c in data["candles"]:
                closes.append(float(c[4]))

        elif "close" in data:
            closes = [float(x) for x in data.get("close",[])]

        EMA_CANDLE_CACHE[security_id] = closes

        return closes

    except:

        return []
# ================= LOOP =================

# ================= LOOP (OPTIMIZED INSTITUTIONAL ORDER) =================

while True:

    try:

        EMA_CANDLE_CACHE.clear()
        # ===== ULTRA READ CACHE =====

        cells = ws.batch_get([
            "B1","B2","B4",
            "H4","H5","H6","H9",
            "Q3","M17","N17","N9",
            "A17","A19","A23",
            "B31","B32","B33",
            "C6","N5","N6","N23"
        ])

        keys = [
            "B1","B2","B4",
            "H4","H5","H6","H9",
            "Q3","M17","N17","N9",
            "A17","A19","A23",
            "B31","B32","B33",
            "C6","N5","N6","N23"
        ]

        SHEET_CACHE.clear()
        SHEET_CACHE.update({
            k:(cells[i][0][0] if cells[i] else "")
            for i,k in enumerate(keys)
        })


        # ---------- MARKET ----------
        ltp, vix = market()

        if ltp is None:
            print("MARKET FAILED — SKIPPING LOOP")
            time.sleep(8)
            continue

        news_mode_engine(ltp)

        ultra_write("C6", ltp)
        ultra_write("C7", vix)


        # ---------- CPR ----------
        cpr_engine(ltp)


        # ---------- OPTIONCHAIN ----------
        oc = optionchain()

        if not oc:
            print("NO OC DATA — skipping loop")
            time.sleep(8)
            continue


        # ---------- STRUCTURE FIRST ----------
        vix_range_engine(ltp, vix)
        oi_levels_engine(ltp, oc)


        # ---------- VWAP ----------
        vwap(ltp, oc)


        # ---------- GAMMA CORE ----------
        gamma_engine(ltp, oc)
        predictive_gamma_engine(ltp, oc)
        god_mode_engine(ltp, oc)


        # ---------- FLOATING STRUCTURE ----------
        auto_strike_floating(ltp, oc)

        atm = round(ltp/50)*50

        # --- EMA SCALP PANEL ---
        process_strike_ema_scalp(atm,"CE",oc,"A38:I38")
        process_strike_ema_scalp(atm,"PE",oc,"A39:I39")

        # --- EMA COMPRESSION PANEL ---
        process_strike_ema_compression(atm,"CE",oc,"A41:I41")
        process_strike_ema_compression(atm,"PE",oc,"A42:I42")

        manual_strike_floating(oc)


        inst_ce, inst_pe = institutional_strike_selector(ltp, oc)

        if inst_ce:
            institutional_floating(inst_ce,"CE",oc,"A19")

        if inst_pe:
            institutional_floating(inst_pe,"PE",oc,"A25")


        # ---------- FLOW & TARGET ----------
        institutional_confirmation(ltp, oc)
        breakout_radar_engine(ltp)
        dealer_trap_engine(ltp, oc)
        inside_cpr_pro_engine(ltp, oc)
        liquidity_target_engine(ltp)
        dealer_trend_intelligence(ltp)
        trend_continuation_engine(ltp)
        dark_pool_entry_engine(ltp, oc)
        absorption_radar_engine(ltp, oc)
        liquidity_vacuum_radar(ltp, oc)


        # ---------- SNIPER EXECUTION ----------
        opening_sniper(ltp)
        true_sniper_mode()
        sniper_antitrap_filter()
        auto_sniper_execution()
        gamma_acceleration_engine()


        # ---------- FINAL DECISION ----------
        decision_engine()
        scalp_mode_v2(ltp, oc)


        # ---------- TRADE MANAGEMENT ----------
        locked_trade_entry()
        locked_trade_exit()
        trade_log_engine()
        trade_exit_engine()
        performance_analytics()


        ultra_write("M3", datetime.datetime.now().strftime("%H:%M:%S"))


        # ===== ULTRA WRITE FLUSH =====

        if WRITE_CACHE:

            batch_data = [
                {"range":k,"values":[[v]]}
                for k,v in WRITE_CACHE.items()
            ]

            ws.batch_update(batch_data)

            WRITE_CACHE.clear()


        print(">>> LOOP OK")

        time.sleep(8)


    except Exception as e:

        print("ERROR:", e)
        time.sleep(8)




