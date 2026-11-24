"""Lớp dịch vụ cho giao diện: đọc dữ liệu, trạng thái kết nối, tổng hợp streaming. Không phụ thuộc Streamlit
nên kiểm thử được bằng pytest."""
import json
import os
import sqlite3
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.analytics import market_health as mh
from src.analytics import panel as pn
from src.analytics import stock_score as ss
from src.finance import indicators as ind

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
SESSION_END = "14:40"   # tick cuối của một phiên đầy đủ phải muộn hơn mốc này (HOSE đóng cửa 14:45)


class DataUnavailable(Exception):
    """Dữ liệu chưa sẵn sàng (thiếu file DB, bảng rỗng...). Thông báo là chuỗi thân thiện với người dùng."""


def open_db(path: str) -> sqlite3.Connection:
    if not os.path.exists(path):
        raise DataUnavailable(f"Chưa có cơ sở dữ liệu tại '{path}'. Chạy: python -m src.data.collector --update")
    return sqlite3.connect(path, check_same_thread=False)


def db_signature(path: str) -> tuple:
    """Dấu vân tay file DB để làm khóa bộ nhớ đệm: đổi khi dữ liệu mới được ghi."""
    try:
        st = os.stat(path)
        return (st.st_mtime_ns // 1_000_000_000, st.st_size)
    except OSError:
        return (0, 0)


def freshness(con) -> dict:
    """Thời điểm cập nhật gần nhất của dữ liệu REST và ngày giao dịch mới nhất."""
    def one(sql):
        try:
            return con.execute(sql).fetchone()
        except sqlite3.Error:
            return (None, None)
    pf, pd_ = one("SELECT MAX(fetch_time), MAX(trade_date) FROM prices_daily")
    inf, ind_ = one("SELECT MAX(fetch_time), MAX(trade_date) FROM index_daily")
    errors = one("SELECT COUNT(*) FROM fetch_log WHERE http IS NULL OR http != 200")[0]
    calls = one("SELECT COUNT(*) FROM fetch_log")[0]
    if pf is None and inf is None:
        raise DataUnavailable("Cơ sở dữ liệu chưa có dữ liệu giá. Chạy: python -m src.data.collector --update")
    return {"prices_fetch": pf, "prices_last_date": pd_, "index_fetch": inf, "index_last_date": ind_,
            "api_calls": calls, "api_errors": errors}


def load_analysis(con, min_value: float) -> dict:
    """Tải bảng giá và tính điểm thị trường + chỉ báo cổ phiếu (nặng: nên được bộ nhớ đệm bởi giao diện)."""
    p = pn.load_panel(con, min_peak_value=min_value)
    if p.close.empty or p.index_close.dropna().empty:
        raise DataUnavailable("Thiếu dữ liệu giá cổ phiếu hoặc VN-Index.")
    el = pn.eligible_mask(p.value, min_value)
    health = mh.market_health(p.index_close, p.index_value, p.close, el)
    factors = ss.factor_table(p.close, p.value, p.index_close, el)
    ret = ind.daily_returns(p.close)
    return {"panel": p, "eligible": el, "health": health, "factors": factors, "ret": ret,
            "ret_m": ind.daily_returns(p.index_close.to_frame("m"))["m"]}


def symbol_meta(con) -> pd.DataFrame:
    try:
        return pd.read_sql_query("SELECT symbol, board, name_vi, icb_name FROM symbols", con).set_index("symbol")
    except Exception:
        return pd.DataFrame(columns=["board", "name_vi", "icb_name"])


def rank_table(a: dict, date, weights: dict, boards: list[str] | None, meta: pd.DataFrame) -> pd.DataFrame:
    t = ss.score_on(date, a["factors"], a["eligible"], weights)
    t = t.join(meta[["board", "name_vi", "icb_name"]], how="left")
    if boards:
        t = t[t["board"].isin(boards)]
    return t


# ---------- streaming ----------
def _has_stream(con) -> bool:
    cols = {r[1] for r in con.execute("PRAGMA table_info(stream_raw)")}
    return {"recv_time", "msg_type", "symbol", "payload"} <= cols


def stream_status(con, now: datetime | None = None) -> dict:
    """Trạng thái luồng WebSocket đã ghi: số dòng, số phiên, thời điểm nhận cuối, nhãn kết nối."""
    now = now or datetime.now(TZ)
    out = {"available": False, "rows": 0, "sessions": 0, "last": None, "age_s": None, "label": "Chưa có dữ liệu streaming",
           "by_day": pd.DataFrame(columns=["ngay", "dong", "tu", "den"])}
    if not _has_stream(con):
        return out
    df = pd.read_sql_query("SELECT substr(recv_time,1,10) AS ngay, COUNT(*) AS dong, MIN(recv_time) AS tu, "
                           "MAX(recv_time) AS den FROM stream_raw GROUP BY 1 ORDER BY 1", con)
    if df.empty:
        return out
    last = pd.to_datetime(df["den"].max())
    age = (now - last.to_pydatetime()).total_seconds()
    out.update(available=True, rows=int(df["dong"].sum()), sessions=len(df), last=last, age_s=age, by_day=df)
    out["label"] = ("Đang nhận dữ liệu (cách đây %d giây)" % age) if age < 120 else \
        f"Không có luồng mới (dòng cuối lúc {last:%d/%m/%Y %H:%M:%S})"
    return out


def stream_trades(con, day: str, limit: int = 400_000) -> pd.DataFrame:
    """TradeMessage của một ngày (YYYY-MM-DD) dưới dạng bảng: thời điểm, mã, giá, khối lượng, chiều, giá trị."""
    rows = con.execute("SELECT recv_time, symbol, payload FROM stream_raw WHERE msg_type='TradeMessage' "
                       "AND substr(recv_time,1,10)=? ORDER BY id LIMIT ?", (day, limit)).fetchall()
    recs = []
    for rt, sym, payload in rows:
        try:
            d = json.loads(payload)
            recs.append((rt, sym, float(d["price"]), float(d["quantity"]), str(d.get("side") or "").upper(),
                         float(d.get("total_volume") or np.nan), str(d.get("trading_time") or "")))
        except (ValueError, KeyError, TypeError):
            continue   # thông điệp hỏng bị bỏ qua, không làm hỏng giao diện
    df = pd.DataFrame(recs, columns=["recv_time", "symbol", "price", "qty", "side", "total_volume", "trading_time"])
    df["value"] = df["price"] * df["qty"]
    return df


def stream_pressure(trades: pd.DataFrame) -> pd.DataFrame:
    """Áp lực mua/bán chủ động theo mã: giá trị mua chủ động (B), bán chủ động (S), chênh lệch ròng và tỷ lệ."""
    if trades.empty:
        return pd.DataFrame(columns=["mua_chu_dong", "ban_chu_dong", "rong", "ty_le_mua", "so_lenh"])
    g = trades.pivot_table(index="symbol", columns="side", values="value", aggfunc="sum", fill_value=0.0)
    for c in ("B", "S"):
        if c not in g:
            g[c] = 0.0
    out = pd.DataFrame({"mua_chu_dong": g["B"], "ban_chu_dong": g["S"]})
    out["rong"] = out["mua_chu_dong"] - out["ban_chu_dong"]
    out["ty_le_mua"] = out["mua_chu_dong"] / (out["mua_chu_dong"] + out["ban_chu_dong"]).replace(0, np.nan)
    out["so_lenh"] = trades.groupby("symbol").size()
    return out.sort_values("rong", ascending=False)


def stream_foreign(con, day: str) -> pd.DataFrame:
    """Giá trị khối ngoại mua/bán lũy kế trong ngày (thông điệp mới nhất của từng mã)."""
    rows = con.execute("SELECT symbol, payload FROM stream_raw WHERE msg_type='ForeignRoomMessage' "
                       "AND substr(recv_time,1,10)=? ORDER BY id", (day,)).fetchall()
    last = {}
    for sym, payload in rows:
        try:
            last[sym] = json.loads(payload)
        except ValueError:
            continue
    recs = [{"symbol": s, "mua": float(d.get("buy_value") or 0), "ban": float(d.get("sell_value") or 0),
             "room_con_lai": d.get("current_room"), "room_tong": d.get("total_room")} for s, d in last.items()]
    df = pd.DataFrame(recs, columns=["symbol", "mua", "ban", "room_con_lai", "room_tong"])
    if df.empty:
        return df.set_index("symbol")
    df["rong"] = df["mua"] - df["ban"]
    return df.set_index("symbol").sort_values("rong", ascending=False)


def stream_crosscheck(con, day: str, trades: pd.DataFrame | None = None) -> pd.DataFrame:
    """Đối chiếu chéo: tổng khối lượng cuối phiên từ streaming so với totalMatch của REST cùng ngày.

    Chỉ so sánh khi tick cuối ≥ 14:40 (phiên được ghi đủ tới cuối); ngược lại ghi rõ 'chưa ghi hết phiên'."""
    t = trades if trades is not None else stream_trades(con, day)
    cols = ["symbol", "stream_vol", "rest_vol", "chenh_lech_%", "ket_luan"]
    if t.empty:
        return pd.DataFrame(columns=cols)
    last = t.dropna(subset=["total_volume"]).groupby("symbol").tail(1).set_index("symbol")
    rest = pd.read_sql_query("SELECT symbol, match_vol FROM prices_daily WHERE trade_date = ?", con, params=[day])
    rest = rest.set_index("symbol")["match_vol"]
    out = pd.DataFrame({"stream_vol": last["total_volume"], "rest_vol": rest.reindex(last.index)})
    hhmm = last["trading_time"].str.extract(r"(\d{2}:\d{2}):\d{2}")[0]
    out["day_du"] = hhmm >= SESSION_END
    out["chenh_lech_%"] = (out["stream_vol"] / out["rest_vol"] - 1) * 100
    def verdict(r):
        if pd.isna(r["rest_vol"]):
            return "chưa có dữ liệu REST của ngày này"
        if not r["day_du"]:
            return "chưa ghi hết phiên"
        return "khớp" if abs(r["chenh_lech_%"]) <= 1.0 else "lệch"
    out["ket_luan"] = out.apply(verdict, axis=1)
    return out.drop(columns="day_du").reset_index().rename(columns={"index": "symbol"})[cols]


# ---------- quan hệ cổ phiếu - thị trường (RQ3) ----------
def market_relation(a: dict, symbol: str, asof, window: int = 120) -> dict:
    """Beta, tương quan và độ lệch chuẩn tương đối của lợi suất ngày so với VN-Index, `window` phiên tới ngày asof."""
    p = a["panel"]
    r = ind.daily_returns(p.close[[symbol]])[symbol]
    rm = ind.daily_returns(p.index_close.to_frame("m"))["m"]
    d = pd.concat([r, rm], axis=1, keys=["s", "m"]).dropna()
    d = d[d.index <= asof].tail(window)
    if len(d) < int(window * 0.8) or d["m"].var() == 0:
        return {"beta": np.nan, "corr": np.nan, "n": len(d)}
    return {"beta": float(d["s"].cov(d["m"]) / d["m"].var()), "corr": float(d["s"].corr(d["m"])), "n": len(d)}


def stock_by_regime(a: dict, symbol: str, asof) -> pd.DataFrame:
    """Hành xử của một mã theo trạng thái thị trường: dùng trạng thái của phiên TRƯỚC để phân loại phiên hiện tại
    (không nhìn trước). Trả về số phiên, lợi suất TB ngày quy năm, của VN-Index, beta và % phiên tăng của mã."""
    p, h = a["panel"], a["health"]
    r = ind.daily_returns(p.close[[symbol]])[symbol]
    rm = ind.daily_returns(p.index_close.to_frame("m"))["m"]
    reg = h["regime"].shift(1)                       # trạng thái biết được vào cuối phiên trước
    d = pd.concat([r, rm, reg], axis=1, keys=["s", "m", "regime"]).dropna()
    d = d[(d.index <= asof) & (d["regime"] != "Chưa đủ dữ liệu")]
    rows = {}
    for name, g in d.groupby("regime"):
        beta = g["s"].cov(g["m"]) / g["m"].var() if len(g) > 20 and g["m"].var() > 0 else np.nan
        rows[name] = {"so_phien": len(g), "ln_mã_%/năm": g["s"].mean() * ind.TRADING_DAYS * 100,
                      "ln_VNIndex_%/năm": g["m"].mean() * ind.TRADING_DAYS * 100, "beta": beta,
                      "%_phiên_tăng": (g["s"] > 0).mean() * 100}
        rows[name]["chênh_lệch_pp"] = rows[name]["ln_mã_%/năm"] - rows[name]["ln_VNIndex_%/năm"]
    return pd.DataFrame(rows).T.reindex(["Yếu", "Trung tính", "Mạnh"]).dropna(how="all")


# ---------- độ mới của dữ liệu ----------
MARKET_CLOSE_TIME = time(15, 0)   # sau mốc này dữ liệu cuối ngày của phiên hôm nay đã có trên SSI


def expected_last_session(now: datetime | None = None) -> date:
    """Phiên gần nhất lẽ ra đã có dữ liệu cuối ngày (chỉ xét thứ Hai-thứ Sáu; chưa biết ngày nghỉ lễ)."""
    now = now or datetime.now(TZ)
    d = now.date()
    if not (d.weekday() < 5 and now.time() >= MARKET_CLOSE_TIME):
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def staleness(last_date: str | None, now: datetime | None = None) -> dict:
    """So ngày giao dịch mới nhất trong DB với phiên lẽ ra đã có. level: 'ok' | 'warn'."""
    exp = expected_last_session(now)
    if not last_date:
        return {"level": "warn", "behind": None, "expected": exp, "message": "Chưa có dữ liệu."}
    last = datetime.strptime(last_date[:10], "%Y-%m-%d").date()
    behind = int(np.busday_count(last + timedelta(days=1), exp + timedelta(days=1))) if exp > last else 0
    if behind <= 0:
        return {"level": "ok", "behind": 0, "expected": exp, "message": "Dữ liệu đã cập nhật tới phiên mới nhất."}
    return {"level": "warn", "behind": behind, "expected": exp,
            "message": (f"Dữ liệu đang cũ khoảng {behind} phiên: mới tới {last:%d/%m/%Y}, trong khi phiên gần nhất là "
                        f"{exp:%d/%m/%Y} (nếu giữa chừng có ngày nghỉ lễ thì số phiên có thể ít hơn). "
                        "Chạy `python scripts/daily_update.py` hoặc xem mục 'Cập nhật tự động' trong README.")}


# ---------- độ rộng chính thức và quan hệ cổ phiếu - thị trường ----------
def breadth_crosscheck(con, a: dict, index_code: str = "VNINDEX", last: int = 30) -> pd.DataFrame:
    """So số mã tăng/(tăng+giảm) CHÍNH THỨC (indexSummary đã lưu mỗi ngày) với số TỰ TÍNH từ giá của các mã cùng sàn."""
    try:
        off = pd.read_sql_query("SELECT trade_date, advances, declines FROM index_summary_daily WHERE index_code = ? "
                                "ORDER BY trade_date DESC LIMIT ?", con, params=[index_code, last])
    except Exception:
        return pd.DataFrame()
    if off.empty:
        return pd.DataFrame()
    board = {"VNINDEX": "HOSE", "VN30": "HOSE", "HNXINDEX": "HNX", "HNX30": "HNX", "UPCOMINDEX": "UPCOM"}.get(index_code, "HOSE")
    cols = [c for c in a["ret"].columns if a["panel"].board.get(c) == board]
    r = a["ret"][cols]
    up, dn = (r > 0).sum(axis=1), (r < 0).sum(axis=1)
    mine = (up / (up + dn).replace(0, np.nan)).rename("tu_tinh")
    off["trade_date"] = pd.to_datetime(off["trade_date"])
    off = off.set_index("trade_date").sort_index()
    off["chinh_thuc"] = off["advances"] / (off["advances"] + off["declines"]).replace(0, np.nan)
    out = off.join(mine, how="left").assign(chenh_lech=lambda x: (x["tu_tinh"] - x["chinh_thuc"]) * 100)
    out[["chinh_thuc", "tu_tinh"]] *= 100
    return out.rename(columns={"advances": "tang_chinh_thuc", "declines": "giam_chinh_thuc"})


def cross_beta(a: dict, asof, names, window: int = 120) -> pd.Series:
    """Beta 120 phiên của các mã `names` so với VN-Index tại ngày asof."""
    cols = [n for n in names if n in a["ret"].columns]
    return ind.beta_at(a["ret"][cols], a["ret_m"], asof, window)


# ---------- tổng hợp trang Thị trường ----------
PILLARS = (("trend", "Xu hướng"), ("breadth", "Độ rộng"), ("liquidity", "Thanh khoản"))


def market_summary(a: dict, asof) -> dict:
    """Số liệu để giải thích điểm thị trường tại ngày asof: thay đổi so với phiên trước, phân vị lịch sử, xu hướng điểm,
    đóng góp từng trụ cột (mỗi trụ cột 1/3 điểm tổng) và các khoảng cách của VN-Index tới SMA50/SMA200."""
    h = a["health"].dropna(subset=["score"])
    h = h[h.index <= asof]
    row = h.iloc[-1]
    prev = h.iloc[-2] if len(h) > 1 else None
    hist = h["score"]
    last5, prev5 = hist.tail(5).mean(), hist.iloc[-10:-5].mean() if len(hist) >= 10 else np.nan
    drift = last5 - prev5
    trend_label = "chưa đủ dữ liệu" if np.isnan(drift) else ("đang cải thiện" if drift > 2 else ("đang suy yếu" if drift < -2 else "đi ngang"))
    run = 1
    for rg in h["regime"].iloc[-2::-1]:
        if rg != row["regime"]:
            break
        run += 1
    pillars = {k: {"label": lab, "value": float(row[k]), "contribution": float(row[k]) / 3,
                   "delta": None if prev is None or np.isnan(prev[k]) else float(row[k] - prev[k])} for k, lab in PILLARS}
    ic = a["panel"].index_close.dropna()
    ic = ic[ic.index <= asof]
    sma50, sma200 = ic.rolling(50).mean(), ic.rolling(200).mean()
    iv = a["panel"].index_value.dropna()
    iv = iv[iv.index <= asof]
    liq_ratio = np.nan
    if len(iv) >= 200:
        liq_ratio = float(iv.tail(20).median() / iv.tail(250).median() - 1)
    return {
        "date": h.index[-1], "score": float(row["score"]), "regime": row["regime"],
        "delta": None if prev is None else float(row["score"] - prev["score"]),
        "percentile": float((hist <= row["score"]).mean() * 100), "drift5": None if np.isnan(drift) else float(drift),
        "trend_label": trend_label, "prev_regime": None if prev is None else prev["regime"], "regime_run": run,
        "pillars": pillars, "pct_above_50": float(row["pct_above_50"]), "n_eligible": int(row["n_eligible"]),
        "index_close": float(ic.iloc[-1]), "index_chg": float(ic.iloc[-1] / ic.iloc[-2] - 1) * 100 if len(ic) > 1 else np.nan,
        "dist_sma50": float(ic.iloc[-1] / sma50.iloc[-1] - 1) * 100 if sma50.notna().iloc[-1] else np.nan,
        "dist_sma200": float(ic.iloc[-1] / sma200.iloc[-1] - 1) * 100 if sma200.notna().iloc[-1] else np.nan,
        "sma50_slope": float(sma50.iloc[-1] / sma50.iloc[-21] - 1) * 100 if len(sma50.dropna()) > 21 else np.nan,
        "liq_vs_250": liq_ratio * 100 if not np.isnan(liq_ratio) else np.nan,
    }


def explain_market(m: dict) -> list[str]:
    """Diễn giải bằng lời, dựa hoàn toàn trên số liệu của market_summary. Chỉ mô tả, không khuyến nghị mua bán."""
    out = []
    pl = sorted(m["pillars"].values(), key=lambda x: x["value"])
    low, high = pl[0], pl[-1]
    out.append(f"Điểm {m['score']:.1f}/100 ở trạng thái **{m['regime']}**. Trụ cột thấp nhất là **{low['label']}** "
               f"({low['value']:.0f}/100), cao nhất là **{high['label']}** ({high['value']:.0f}/100); "
               f"mỗi trụ cột đóng góp tối đa 33,3 điểm vào điểm tổng.")
    b = m["pillars"]["breadth"]
    d = "" if b["delta"] is None else (f", {'cải thiện' if b['delta'] > 0 else 'suy giảm'} {abs(b['delta']):.0f} điểm so với phiên trước"
                                       if abs(b["delta"]) >= 1 else ", gần như không đổi so với phiên trước")
    out.append(f"Độ rộng: {m['pct_above_50']:.0f}% trong {m['n_eligible']} mã đủ thanh khoản có giá trên SMA50{d}.")
    if not np.isnan(m["liq_vs_250"]):
        out.append(f"Thanh khoản: giá trị khớp trung vị 20 phiên {'cao hơn' if m['liq_vs_250'] >= 0 else 'thấp hơn'} "
                   f"{abs(m['liq_vs_250']):.0f}% so với trung vị 250 phiên.")
    if not np.isnan(m["dist_sma50"]):
        out.append(f"VN-Index {m['index_close']:,.1f} đang {'trên' if m['dist_sma50'] >= 0 else 'dưới'} SMA50 {abs(m['dist_sma50']):.1f}%"
                   + (f" và {'trên' if m['dist_sma200'] >= 0 else 'dưới'} SMA200 {abs(m['dist_sma200']):.1f}%." if not np.isnan(m["dist_sma200"]) else "."))
    if m["prev_regime"] and m["prev_regime"] != m["regime"]:
        out.append(f"Trạng thái vừa chuyển từ {m['prev_regime']} sang {m['regime']} so với phiên trước.")
    else:
        out.append(f"Trạng thái {m['regime']} đã kéo dài {m['regime_run']} phiên liên tiếp; điểm 5 phiên gần nhất {m['trend_label']}.")
    out.append(f"Điểm hiện tại cao hơn {m['percentile']:.0f}% số phiên trong lịch sử của chính thị trường (phân vị lịch sử).")
    return out


def breadth_counts(a: dict) -> pd.DataFrame:
    """Số mã tăng / giảm / đứng giá mỗi ngày, trong các mã đủ thanh khoản. 'Đứng giá' gồm cả mã không khớp lệnh
    trong phiên (giá đóng cửa không đổi). Cột ad_line là đường tích lũy (tăng - giảm)."""
    r = a["ret"].where(a["eligible"])
    adv, dec = (r > 1e-12).sum(axis=1), (r < -1e-12).sum(axis=1)
    flat = (r.abs() <= 1e-12).sum(axis=1)
    out = pd.DataFrame({"tang": adv, "giam": dec, "dung_gia": flat})
    out["tong"] = out.sum(axis=1)
    out = out[out["tong"] > 0]
    out["ad_line"] = (out["tang"] - out["giam"]).cumsum()
    return out


# ---------- Realtime monitor ----------
MARKET_OPEN, MARKET_CLOSE = time(9, 0), time(14, 45)
START_CMD = 'python python/stream_recorder.py --db "data/market.db" --minutes 105'


def _in_trading_hours(now: datetime) -> bool:
    t = now.time()
    return now.weekday() < 5 and (time(9, 0) <= t <= time(11, 30) or time(13, 0) <= t <= MARKET_CLOSE)


def ws_state(status: dict, now: datetime | None = None) -> dict:
    """Trạng thái WebSocket suy ra từ dữ liệu đã lưu (bộ ghi chạy tiến trình riêng). level: ok | info | warn.
    Chỉ báo 'Mất kết nối' khi hôm nay ĐÃ nhận bản tin rồi dừng giữa giờ giao dịch; nếu hôm nay chưa có bản tin nào
    thì có thể bộ ghi chưa chạy hoặc sàn nghỉ (lễ) nên chỉ là thông tin, không cảnh báo."""
    now = now or datetime.now(TZ)
    if not status["available"]:
        return {"state": "Chưa khởi chạy", "level": "info",
                "detail": "Chưa có luồng nào được lưu. Khởi chạy bộ ghi trong giờ giao dịch (9:00-14:45)."}
    last, age = status["last"], status["age_s"]
    if age < 120:
        return {"state": "Đang nhận dữ liệu", "level": "ok", "detail": f"Bản tin gần nhất cách đây {int(age)} giây."}
    if _in_trading_hours(now):
        if last.date() == now.date():
            return {"state": "Mất kết nối", "level": "warn",
                    "detail": f"Đang trong giờ giao dịch nhưng không có bản tin mới từ {last:%H:%M:%S %d/%m} "
                              "(bộ ghi đã dừng, mạng/phiên đăng nhập hết hạn)."}
        return {"state": "Chưa nhận dữ liệu hôm nay", "level": "info",
                "detail": f"Hôm nay chưa có bản tin nào (lần cuối {last:%H:%M %d/%m}). Bộ ghi chưa chạy, "
                          "hoặc hôm nay sàn nghỉ giao dịch thì là bình thường."}
    return {"state": "Đã dừng", "level": "info",
            "detail": f"Lần nhận cuối lúc {last:%H:%M:%S %d/%m/%Y}; ngoài giờ giao dịch nên không có luồng mới là bình thường."}


def stream_counts(con, day: str) -> pd.DataFrame:
    """Số bản tin ĐÃ LƯU của một ngày theo loại (giao diện không biết số bản tin nhận nhưng không lưu, ví dụ quote)."""
    return pd.read_sql_query("SELECT msg_type AS loai, COUNT(*) AS so_ban_tin, MIN(recv_time) AS dau, MAX(recv_time) AS cuoi "
                             "FROM stream_raw WHERE substr(recv_time,1,10)=? GROUP BY msg_type ORDER BY 2 DESC", con, params=[day])


def stream_watchlist(trades: pd.DataFrame) -> pd.DataFrame:
    """Mỗi mã đã ghi: giá gần nhất, thay đổi so với giá đầu tiên ghi được (KHÔNG phải so với giá tham chiếu),
    khối lượng khớp lũy kế mới nhất, số lệnh và thời điểm cuối."""
    cols = ["gia_cuoi", "gia_dau_ghi", "doi_%", "kl_luy_ke", "so_lenh", "luc_cuoi"]
    if trades.empty:
        return pd.DataFrame(columns=cols)
    g = trades.sort_values("recv_time").groupby("symbol")
    out = pd.DataFrame({"gia_cuoi": g["price"].last(), "gia_dau_ghi": g["price"].first(),
                        "kl_luy_ke": g["total_volume"].last(), "so_lenh": g.size(), "luc_cuoi": g["recv_time"].last()})
    out["doi_%"] = (out["gia_cuoi"] / out["gia_dau_ghi"] - 1) * 100
    return out[cols].sort_values("so_lenh", ascending=False)


def top_pressure(press: pd.DataFrame, n: int = 12) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Mua ròng lớn nhất (chỉ mã có ròng > 0) và bán ròng lớn nhất (chỉ mã có ròng < 0)."""
    buy = press[press["rong"] > 0].sort_values("rong", ascending=False).head(n)
    sell = press[press["rong"] < 0].sort_values("rong").head(n)
    return buy, sell


# ---------- diễn giải thị trường (phân tích thay vì liệt kê) ----------
def pillar_note(key: str, m: dict) -> str:
    """Một dòng giải thích cho thẻ trụ cột."""
    if key == "trend":
        if np.isnan(m["dist_sma50"]):
            return "Chưa đủ dữ liệu SMA."
        s = f"VN-Index {'trên' if m['dist_sma50'] >= 0 else 'dưới'} SMA50 {abs(m['dist_sma50']):.1f}%"
        if not np.isnan(m["dist_sma200"]):
            s += f", {'trên' if m['dist_sma200'] >= 0 else 'dưới'} SMA200 {abs(m['dist_sma200']):.1f}%"
        return s + "."
    if key == "breadth":
        return f"{m['pct_above_50']:.0f}% mã đủ thanh khoản có giá trên SMA50."
    if np.isnan(m["liq_vs_250"]):
        return "Chưa đủ dữ liệu thanh khoản."
    return f"Thanh khoản 20 phiên {'cao' if m['liq_vs_250'] >= 0 else 'thấp'} hơn {abs(m['liq_vs_250']):.0f}% so với trung vị 250 phiên."


def interpret_market(m: dict) -> list[tuple[str, str]]:
    """Diễn giải có điều kiện: từ tổ hợp ba trụ cột suy ra bối cảnh có thể xảy ra. Chỉ mô tả dữ liệu quá khứ,
    dùng ngôn ngữ thận trọng, không đưa khuyến nghị mua/bán và không dự báo."""
    t, b, lq = (m["pillars"][k]["value"] for k in ("trend", "breadth", "liquidity"))
    out = [("Mức độ", f"Điểm {m['score']:.1f}/100 thuộc vùng **{m['regime']}**. Phân vị lịch sử {m['percentile']:.0f}% nghĩa là khoảng "
                      f"{m['percentile']:.0f}% số phiên từ khi có đủ dữ liệu có điểm thấp hơn hoặc bằng hôm nay.")]
    chg = "" if m["delta"] is None else f"So với phiên trước điểm {'tăng' if m['delta'] > 0 else 'giảm'} {abs(m['delta']):.1f}. "
    out.append(("Diễn biến", chg + f"Trung bình 5 phiên gần nhất {m['trend_label']}; trạng thái {m['regime']} đã kéo dài {m['regime_run']} phiên. "
                             "Điểm một phiên có nhiễu, nên đọc cùng xu hướng vài phiên."))
    if t < 33 and b < 33:
        cause = ("Yếu cả ở chỉ số lẫn độ rộng: không chỉ VN-Index mà đa số cổ phiếu đủ thanh khoản cũng nằm dưới đường trung bình. "
                 "Đây là mức yếu mang tính diện rộng, khác với trường hợp chỉ vài mã lớn kéo chỉ số xuống.")
    elif t >= 50 and b < 33:
        cause = ("Chỉ số còn giữ được xu hướng nhưng ít cổ phiếu đi cùng (độ rộng kém). Tăng điểm kiểu này thường dựa vào một nhóm nhỏ cổ phiếu; "
                 "đây là mô tả độ lan tỏa, không phải dự báo đảo chiều.")
    elif t < 33 and b >= 50:
        cause = "Nhiều cổ phiếu đang khá hơn chỉ số chung: độ rộng tốt hơn xu hướng của VN-Index, có thể do nhóm vốn hóa lớn đang kéo chỉ số."
    elif min(t, b, lq) == lq and lq < 33:
        cause = "Điểm thấp chủ yếu do thanh khoản; xu hướng và độ rộng chưa yếu bằng."
    else:
        cause = (f"Trụ cột thấp nhất là {min(m['pillars'].values(), key=lambda p: p['value'])['label']}; các trụ cột còn lại ở mức "
                 "khá hơn nên điểm không bị kéo xuống đồng đều.")
    out.append(("Nguyên nhân chính", cause))
    lv = m["liq_vs_250"]
    if not np.isnan(lv):
        if lv < -20:
            liq = (f"Thanh khoản thấp hơn bình thường khoảng {abs(lv):.0f}%: nhà đầu tư tham gia ít hơn hoặc thận trọng hơn. "
                   "Thanh khoản thấp một mình chưa đủ để kết luận áp lực bán đã kết thúc; cần thêm tín hiệu về xu hướng và độ rộng.")
        elif lv > 20 and m["regime"] == "Yếu":
            liq = (f"Thanh khoản cao hơn bình thường khoảng {lv:.0f}% trong lúc thị trường yếu. Số liệu này không phân biệt được "
                   "giữa bán mạnh và hoạt động mua vào ở vùng thấp.")
        elif lv > 20 and m["regime"] == "Mạnh":
            liq = f"Thanh khoản cao hơn bình thường khoảng {lv:.0f}%, phù hợp với trạng thái mạnh (đà tăng có sự tham gia của dòng tiền)."
        else:
            liq = f"Thanh khoản gần mức bình thường ({lv:+.0f}% so với trung vị 250 phiên) nên không phải yếu tố nổi bật."
        out.append(("Thanh khoản", liq))
    out.append(("Giới hạn", "Điểm mô tả trạng thái hiện tại so với lịch sử của chính thị trường. Khả năng của điểm trong việc dự báo lợi suất "
                            "được kiểm chứng ở tab Kiểm chứng; không dùng điểm này một mình để ra quyết định mua/bán."))
    return out


# ---------- tóm tắt một cổ phiếu ----------
IND_LABELS = {"trend_50": "Giá so với SMA50", "trend_200": "Giá so với SMA200", "mom_126_21": "Động lượng 126-21 phiên",
              "rs_63": "Sức mạnh tương đối 63 phiên", "liq_med": "Thanh khoản (giá trị khớp trung vị)",
              "vol_60": "Biến động 60 phiên (thấp là tốt)", "mdd_126": "Sụt giảm tối đa 126 phiên (nông là tốt)"}


def indicator_percentiles(a: dict, asof, sym: str) -> pd.Series:
    """Thứ hạng phần trăm (0-100, cao = tốt) của từng chỉ báo con của mã giữa các mã đủ điều kiện, cùng cách tính với điểm."""
    el = a["eligible"].loc[asof]
    out = {}
    for k, lab in IND_LABELS.items():
        row = a["factors"][k].loc[asof].where(el)
        r = row.rank(pct=True)
        out[lab] = float((1 - r[sym] if k == "vol_60" else r[sym]) * 100) if sym in r.index and pd.notna(r[sym]) else np.nan
    return pd.Series(out)


def stock_summary(a: dict, asof, sym: str, table: pd.DataFrame) -> dict:
    """Kết luận đầu trang chi tiết một mã: điểm và hạng, xu hướng, sức mạnh tương đối, mức rủi ro và 3 chỉ báo mạnh/yếu nhất."""
    row = table.loc[sym]
    rank = int(table["score"].rank(ascending=False, method="min")[sym])
    f = a["factors"]
    t50, t200 = (float(f[k].loc[asof, sym]) if sym in f[k].columns else np.nan for k in ("trend_50", "trend_200"))
    if np.isnan(t50) or np.isnan(t200):
        trend = "chưa đủ dữ liệu"
    elif t50 > 0 and t200 > 0:
        trend = "tăng: giá trên cả SMA50 và SMA200"
    elif t50 < 0 and t200 < 0:
        trend = "giảm: giá dưới cả SMA50 và SMA200"
    else:
        trend = "lẫn lộn: giá " + ("trên SMA50, dưới SMA200" if t50 > 0 else "dưới SMA50, trên SMA200")
    rs = float(row["rs_63_pp"])
    risk = row["risk"]
    risk_label = "thấp" if risk >= 67 else ("trung bình" if risk >= 33 else "cao")
    pct = indicator_percentiles(a, asof, sym).dropna().sort_values(ascending=False)
    return {"score": float(row["score"]), "rank": rank, "n": len(table), "trend": trend, "rs": rs,
            "rs_label": "vượt" if rs >= 0 else "kém hơn", "risk_label": risk_label, "risk_score": float(risk),
            "vol": float(row["vol_60_%"]), "mdd": float(row["mdd_126_%"]),
            "top_pos": list(pct.head(3).items()), "top_neg": list(pct.tail(3).sort_values().items())}


# ---------- điểm đáng chú ý trong phiên (streaming) ----------
def stream_highlights(trades: pd.DataFrame, spike_x: float = 3.0, bin_min: int = 5) -> pd.DataFrame:
    """Biến dữ liệu khớp lệnh thành vài sự kiện đọc được: (1) mã biến động giá nhiều nhất so với giá đầu tiên ghi được,
    kèm thời điểm đạt cực trị; (2) cửa sổ `bin_min` phút có khối lượng ≥ `spike_x` lần trung vị các cửa sổ của chính mã đó.
    Đây là mô tả thống kê đơn giản, không phải hệ thống cảnh báo."""
    cols = ["ma", "loai", "thoi_diem", "chi_tiet", "do_lon"]
    if trades.empty:
        return pd.DataFrame(columns=cols)
    t = trades.copy()
    t["t"] = pd.to_datetime(t["recv_time"], errors="coerce")
    t = t.dropna(subset=["t", "price"]).sort_values("t")
    rows = []
    for sym, g in t.groupby("symbol"):
        base = g["price"].iloc[0]
        if base > 0:
            dev = g["price"] / base - 1
            i = dev.abs().idxmax()
            rows.append((sym, "Biến động giá", g.loc[i, "t"], f"{dev[i] * 100:+.2f}% so với giá khớp đầu tiên ({base:,.2f})", abs(dev[i]) * 100))
        vol = g.set_index("t")["qty"].resample(f"{bin_min}min").sum()
        vol = vol[vol > 0]
        if len(vol) >= 6:
            med = vol.median()
            if med > 0 and vol.max() / med >= spike_x:
                rows.append((sym, "Khối lượng đột biến", vol.idxmax(), f"{vol.max() / med:.1f} lần trung vị {bin_min} phút của mã ({vol.max():,.0f} cp)",
                             vol.max() / med))
    out = pd.DataFrame(rows, columns=["ma", "loai", "thoi_diem", "chi_tiet", "do_lon"])
    if out.empty:
        return out
    top = pd.concat([out[out["loai"] == "Biến động giá"].nlargest(4, "do_lon"),
                     out[out["loai"] == "Khối lượng đột biến"].nlargest(4, "do_lon")])
    return top.sort_values("thoi_diem").reset_index(drop=True)
