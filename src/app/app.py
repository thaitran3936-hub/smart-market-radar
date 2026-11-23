"""SMART MARKET RADAR - giao diện Streamlit.

Chạy:  streamlit run src/app/app.py
Công cụ phân tích học thuật, không phải khuyến nghị đầu tư.

Bốn câu hỏi của sản phẩm và tab trả lời:
  1. Thị trường đang mạnh, trung tính hay yếu?                     -> tab Thị trường
  2. Cổ phiếu nào tốt về xu hướng, động lượng, thanh khoản, rủi ro? -> tab Xếp hạng cổ phiếu
  3. Cổ phiếu nào mạnh hơn VN-Index (RS, beta)?                    -> tab Xếp hạng cổ phiếu
  4. Phương pháp có hiệu quả khi kiểm chứng lịch sử không?          -> tab Kiểm chứng
Tab Realtime Monitor chứng minh tích hợp WebSocket SSI; tab Phương pháp ghi công thức và giới hạn.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # để chạy được từ thư mục gốc của dự án

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from src import config  # noqa: E402
from src.analytics import backtest as bt  # noqa: E402
from src.analytics import market_health as mh  # noqa: E402
from src.app import services as sv  # noqa: E402
from src.data import sync  # noqa: E402

st.set_page_config(page_title="Smart Market Radar", page_icon="📡", layout="wide")
DB = config.DB_PATH
REGIME_COLOR = {"Yếu": "#e5484d", "Trung tính": "#d9a441", "Mạnh": "#2fa36b", "Chưa đủ dữ liệu": "#888888"}
THRESHOLDS = {"500 triệu": 5e8, "1 tỷ (mặc định)": 1e9, "5 tỷ": 5e9}
GROUPS = (("trend", "Xu hướng"), ("momentum", "Động lượng"), ("liquidity", "Thanh khoản"), ("risk", "Rủi ro thấp"))


# ---------- đồng bộ dữ liệu từ máy chủ (GitHub Release) ----------
def get_secret(name: str) -> str:
    """Đọc cấu hình từ biến môi trường (.env) hoặc st.secrets (khi chạy trên Streamlit Cloud)."""
    v = os.getenv(name)
    if v:
        return v
    try:
        return str(st.secrets.get(name, "") or "")
    except Exception:   # không có tệp secrets
        return ""


@st.cache_resource(ttl=900, show_spinner="Đang kiểm tra dữ liệu mới trên máy chủ...")
def auto_sync(db_path: str, repo: str, _token: str):
    """Mỗi 15 phút (khi có người mở/làm mới trang) kiểm tra Release; có bản mới thì tải về và thay DB."""
    try:
        return sync.download_latest(repo, _token, db_path)
    except sync.SyncError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:   # vd. Windows không thay được tệp DB đang mở
        return {"status": "error", "message": f"{type(e).__name__}: {e}"}


# ---------- bộ nhớ đệm (chỉ giữ 1 bản để không tích tụ bộ nhớ khi dữ liệu đổi) ----------
@st.cache_resource(max_entries=1)
def get_con(path: str, _sig):
    return sv.open_db(path)


@st.cache_resource(max_entries=1, show_spinner="Đang tính chỉ báo trên toàn bộ dữ liệu (lần đầu mất vài chục giây)...")
def get_analysis(path: str, sig, min_value: float):
    a = sv.load_analysis(get_con(path, sig), min_value)
    a["breadth_counts"] = sv.breadth_counts(a)
    return a


@st.cache_resource(max_entries=2, show_spinner="Đang chạy kiểm chứng lịch sử (1-3 phút)...")
def get_backtest(path: str, sig, min_value: float, top_frac: float, cost_bps: float):
    a = get_analysis(path, sig, min_value)
    p = a["panel"]
    res = bt.stock_backtest(p.close, p.index_close, a["eligible"], a["factors"], a["health"],
                            top_frac=top_frac, cost_bps=cost_bps)
    ic = bt.information_coefficient(p.close, p.index_close, a["eligible"], a["factors"])
    rel = bt.summarize_relation(bt.relation_analysis(p.close, p.index_close, a["eligible"], a["factors"], a["health"]))
    return {"bt": res, "ic": ic, "regime5": bt.regime_forward_returns(a["health"], p.index_close, 5),
            "regime21": bt.regime_forward_returns(a["health"], p.index_close, 21), "relation": rel,
            "params": (min_value, top_frac, cost_bps)}


@st.cache_data(show_spinner=False)
def get_stream_day(path: str, sig, day: str):
    con = get_con(path, sig)
    trades = sv.stream_trades(con, day)
    return (trades, sv.stream_pressure(trades), sv.stream_foreign(con, day), sv.stream_crosscheck(con, day, trades),
            sv.stream_counts(con, day))


def fmt_time(ts) -> str:
    return "—" if not ts else str(ts).replace("T", " ")[:19]


def style(fig, title: str, height: int = 320, **kw):
    fig.update_layout(title=title, height=height, margin=dict(l=10, r=10, t=45, b=10), **kw)
    return fig


def show(fig):
    st.plotly_chart(fig, use_container_width=True)


def gauge(score: float, regime: str):
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=round(score, 1), number={"suffix": " / 100", "font": {"size": 40}},
        gauge={"axis": {"range": [0, 100], "tickvals": [0, mh.LOW, mh.HIGH, 100]},
               "bar": {"color": REGIME_COLOR.get(regime, "#888888"), "thickness": 0.3},
               "steps": [{"range": [0, mh.LOW], "color": "rgba(229,72,77,0.22)"},
                         {"range": [mh.LOW, mh.HIGH], "color": "rgba(217,164,65,0.22)"},
                         {"range": [mh.HIGH, 100], "color": "rgba(47,163,107,0.22)"}]}))
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=20, b=0))
    return fig


def sign(x, nd=1, suffix=""):
    return "—" if x is None or pd.isna(x) else f"{x:+.{nd}f}{suffix}"


# ---------- tiêu đề ----------
st.title("📡 Smart Market Radar")
st.caption("Ra-đa sức khỏe thị trường chứng khoán Việt Nam và sàng lọc cổ phiếu theo sức mạnh tương đối · "
           "Dữ liệu: SSI FastConnect v3 (REST + WebSocket) · Công cụ phân tích học thuật, **không phải khuyến nghị đầu tư**; "
           "không gửi lệnh giao dịch.")

GH_REPO, GH_TOKEN = get_secret("GITHUB_REPO"), get_secret("GITHUB_TOKEN")
sync_res = auto_sync(DB, GH_REPO, GH_TOKEN) if GH_REPO and GH_TOKEN else None

try:
    sig = sv.db_signature(DB)
    con = get_con(DB, sig)
    fresh = sv.freshness(con)
except sv.DataUnavailable as e:
    st.error(str(e))
    st.stop()
except Exception as e:  # lỗi không lường trước: báo rõ, không để trang trắng
    st.error(f"Không đọc được cơ sở dữ liệu: {type(e).__name__}: {e}")
    st.stop()

# ngưỡng thanh khoản đặt ở tab Xếp hạng nhưng ảnh hưởng toàn bộ phân tích: đọc từ session_state (lần đầu dùng mặc định)
th_label = st.session_state.get("th_label", "1 tỷ (mặc định)")
if th_label not in THRESHOLDS:
    th_label = "1 tỷ (mặc định)"
min_value = THRESHOLDS[th_label]

try:
    A = get_analysis(DB, sig, min_value)
except sv.DataUnavailable as e:
    st.error(str(e))
    st.stop()
health = A["health"].dropna(subset=["score"])
if health.empty:
    st.error("Chưa đủ dữ liệu để tính điểm thị trường (cần ≥ 250 phiên và có mã đủ thanh khoản).")
    st.stop()

with st.sidebar:
    st.header("Xem dữ liệu")
    last_day = health.index[-1].date()
    asof = pd.Timestamp(st.date_input("Ngày phân tích", value=last_day, min_value=health.index[0].date(), max_value=last_day))
    asof = health.index[health.index <= asof][-1]
    horizon = st.radio("Khoảng thời gian biểu đồ", ["1 năm", "3 năm", "5 năm", "Tất cả"], index=1, horizontal=True)
    st.caption("Bộ lọc sàn, ngành, thanh khoản và trọng số nằm ở tab Xếp hạng cổ phiếu.")
years = {"1 năm": 1, "3 năm": 3, "5 năm": 5}.get(horizon)
start = asof - pd.DateOffset(years=years) if years else health.index[0]

# ---------- thanh trạng thái dữ liệu ----------
ss_status = sv.stream_status(con)
ws = sv.ws_state(ss_status)
stale = sv.staleness(fresh["prices_last_date"])
rate = fresh["api_errors"] / fresh["api_calls"] * 100 if fresh["api_calls"] else 0.0
c1, c2, c3, c4 = st.columns(4)
with c1.container(border=True):
    st.metric("Phiên giao dịch mới nhất", fresh["prices_last_date"] or "—",
              help="Ngày của dữ liệu thị trường (khác với thời điểm chương trình tải dữ liệu).")
    st.caption(f"Tải về lúc {fmt_time(fresh['prices_fetch'])}")
with c2.container(border=True):
    st.metric("Lệnh gọi API lỗi / tổng", f"{fresh['api_errors']} / {fresh['api_calls']:,}",
              help="Tính cả các lần thử lại. Lỗi tạm thời (429/5xx/mạng) được tự thử lại; mã lỗi hẳn được ghi trong fetch_log.")
    st.caption(f"Tỷ lệ lỗi {rate:.2f}% · tự thử lại tối đa 5 lần")
with c3.container(border=True):
    st.metric("WebSocket", ws["state"], help=ws["detail"])
    st.caption(ws["detail"][:70] + ("…" if len(ws["detail"]) > 70 else ""))
with c4.container(border=True):
    st.metric("Dữ liệu realtime gần nhất", ss_status["last"].strftime("%H:%M:%S %d/%m") if ss_status["available"] else "—",
              help="Thời điểm bản tin streaming cuối cùng được lưu vào cơ sở dữ liệu.")
    st.caption(f"{ss_status['sessions']} phiên · {ss_status['rows']:,} bản tin đã lưu")
if sync_res and sync_res.get("status") == "error":
    st.warning("Chưa đồng bộ được dữ liệu mới từ máy chủ (đang dùng bản đã có): " + sync_res["message"])
if stale["level"] == "warn":
    st.warning("⏳ " + stale["message"])
if asof.strftime("%Y-%m-%d") != fresh["prices_last_date"]:
    st.info(f"Đang xem ngày {asof:%d/%m/%Y}; ngày giao dịch mới nhất trong dữ liệu là {fresh['prices_last_date']}.")

tabs = st.tabs(["Thị trường", "Xếp hạng cổ phiếu", "Realtime Monitor", "Kiểm chứng", "Phương pháp & giới hạn"])

# ===== Tab 1: sức khỏe thị trường =====
with tabs[0]:
    M = sv.market_summary(A, asof)
    st.subheader(f"Thị trường đang mạnh, trung tính hay yếu? — {asof:%d/%m/%Y}")
    g1, g2, g3 = st.columns([1.1, 1.3, 1.1])
    with g1.container(border=True):
        st.markdown("**Market Health Score**")
        show(gauge(M["score"], M["regime"]))
        st.markdown(f"<div style='text-align:center;font-size:1.3rem;font-weight:700;color:{REGIME_COLOR[M['regime']]}'>"
                    f"{M['regime']}</div>", unsafe_allow_html=True)
        st.caption(f"So với phiên trước: **{sign(M['delta'])} điểm** · Phân vị lịch sử: **{M['percentile']:.0f}%** · "
                   f"Điểm 5 phiên gần nhất: **{M['trend_label']}**")
    with g2.container(border=True):
        st.markdown("**Ba trụ cột và đóng góp vào điểm tổng** (mỗi trụ cột 1/3)")
        for k, lab in sv.PILLARS:
            pl = M["pillars"][k]
            a_, b_ = st.columns([3, 2])
            a_.metric(lab, f"{pl['value']:.0f} / 100", None if pl["delta"] is None else f"{pl['delta']:+.0f} so với phiên trước")
            b_.metric("Đóng góp", f"{pl['contribution']:.1f} / 33,3")
    with g3.container(border=True):
        st.markdown("**VN-Index**")
        st.metric("Điểm đóng cửa", f"{M['index_close']:,.1f}", sign(M["index_chg"], 2, "%"))
        st.caption(f"Cách SMA50: **{sign(M['dist_sma50'], 1, '%')}** · cách SMA200: **{sign(M['dist_sma200'], 1, '%')}**")
        st.caption(f"Độ dốc SMA50 (20 phiên): **{sign(M['sma50_slope'], 1, '%')}** · "
                   f"Thanh khoản 20 phiên so với trung vị 250 phiên: **{sign(M['liq_vs_250'], 0, '%')}**")
    with st.container(border=True):
        st.markdown("**Vì sao điểm như vậy?**")
        for line in sv.explain_market(M):
            st.markdown("- " + line)
        st.caption("Điểm là phân vị so với lịch sử của chính thị trường tính đến ngày đó; mô tả trạng thái, không phải dự báo "
                   "hay khuyến nghị mua bán.")

    h = health.loc[(health.index >= start) & (health.index <= asof)]
    full = A["panel"].index_close.dropna()
    ic_ = full[(full.index >= start) & (full.index <= asof)]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ic_.index, y=ic_, name="VN-Index", line=dict(width=2)))
    for n, dash in ((50, "dot"), (200, "dash")):
        s_ = full.rolling(n).mean()
        s_ = s_[(s_.index >= start) & (s_.index <= asof)]
        fig.add_trace(go.Scatter(x=s_.index, y=s_, name=f"SMA{n}", line=dict(width=1.2, dash=dash)))
    show(style(fig, "VN-Index, SMA50 và SMA200", 340))
    iv = A["panel"].index_value.dropna()
    iv = iv[(iv.index >= start) & (iv.index <= asof)]
    if len(iv):
        figv = go.Figure(go.Bar(x=iv.index, y=iv / 1e9, name="Giá trị khớp (tỷ đồng)"))
        figv.add_trace(go.Scatter(x=iv.index, y=iv.rolling(20).median() / 1e9, name="Trung vị 20 phiên", line=dict(width=2)))
        show(style(figv, "Giá trị giao dịch của thị trường (tỷ đồng/phiên)", 220))

    st.markdown("**Ba trụ cột theo thời gian**")
    pc = st.columns(3)
    for col, (k, lab) in zip(pc, sv.PILLARS):
        f_ = go.Figure()
        f_.add_hrect(y0=0, y1=mh.LOW, fillcolor=REGIME_COLOR["Yếu"], opacity=0.10, line_width=0)
        f_.add_hrect(y0=mh.HIGH, y1=100, fillcolor=REGIME_COLOR["Mạnh"], opacity=0.10, line_width=0)
        f_.add_trace(go.Scatter(x=h.index, y=h[k], name=lab, line=dict(width=1.6)))
        f_.update_layout(showlegend=False, yaxis=dict(range=[0, 100]))
        with col:
            show(style(f_, f"{lab} (0-100)", 240))
    with st.expander("Biểu đồ tổng hợp (bật/tắt từng đường)"):
        lines = {"Điểm tổng": "score", "Xu hướng": "trend", "Độ rộng": "breadth", "Thanh khoản": "liquidity"}
        chosen = st.multiselect("Đường hiển thị", list(lines), default=["Điểm tổng"])
        f2 = go.Figure()
        f2.add_hrect(y0=0, y1=mh.LOW, fillcolor=REGIME_COLOR["Yếu"], opacity=0.08, line_width=0)
        f2.add_hrect(y0=mh.HIGH, y1=100, fillcolor=REGIME_COLOR["Mạnh"], opacity=0.08, line_width=0)
        for name in chosen or []:
            f2.add_trace(go.Scatter(x=h.index, y=h[lines[name]], name=name, line=dict(width=3 if name == "Điểm tổng" else 1.2)))
        show(style(f2, f"Điểm theo thời gian (đỏ < {mh.LOW:.0f}: Yếu, xanh > {mh.HIGH:.0f}: Mạnh)", 320, yaxis=dict(range=[0, 100])))

    st.markdown("**Độ rộng thị trường**")
    bc = A["breadth_counts"]
    bnow = bc.loc[:asof].iloc[-1]
    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Số mã tăng", f"{int(bnow['tang'])}")
    b2.metric("Số mã giảm", f"{int(bnow['giam'])}")
    b3.metric("Số mã đứng giá", f"{int(bnow['dung_gia'])}",
              help="Giá đóng cửa không đổi, gồm cả mã không khớp lệnh trong phiên.")
    b4.metric("% mã trên SMA50", f"{M['pct_above_50']:.0f}%")
    bw = bc[(bc.index >= start) & (bc.index <= asof)]
    d1, d2 = st.columns(2)
    if len(bw):
        fa = go.Figure(go.Scatter(x=bw.index, y=bw["ad_line"] - bw["ad_line"].iloc[0], name="A/D tích lũy", line=dict(width=2)))
        with d1:
            show(style(fa, "Đường Advance/Decline (tích lũy tăng − giảm, gốc 0 tại đầu kỳ)", 280))
        fb = go.Figure(go.Scatter(x=h.index, y=h["pct_above_50"], name="% trên SMA50", fill="tozeroy"))
        with d2:
            show(style(fb, "% mã đủ thanh khoản có giá trên SMA50", 280, yaxis=dict(range=[0, 100])))
    st.caption(f"Phạm vi: các mã đủ thanh khoản (trung vị giá trị khớp 60 phiên ≥ {th_label}, giao dịch ≥ 80% số phiên) — "
               f"{int(bnow['tong'])} mã hôm nay. Mã tăng: đóng cửa > phiên trước; giảm: <; đứng giá: bằng nhau. "
               "Mã thiếu giá hoặc bị gắn cờ lỗi giá không được tính. Để tiết kiệm bộ nhớ, chỉ nạp các mã có thể đủ thanh khoản ở ngưỡng đang chọn.")

    chk = sv.breadth_crosscheck(con, A)
    with st.expander("Đối chiếu độ rộng tự tính với số liệu chính thức của sàn (indexSummary lưu mỗi ngày)"):
        if chk.empty:
            st.info("Chưa có ảnh chụp indexSummary. Chạy `python -m src.data.collector --snapshot` hoặc cập nhật hằng ngày; "
                    "SSI không lưu lịch sử nên số liệu này chỉ tích lũy từ ngày bắt đầu lưu.")
        else:
            st.dataframe(chk.round(1).rename(columns={"tang_chinh_thuc": "Tăng (chính thức)", "giam_chinh_thuc": "Giảm (chính thức)",
                                                      "chinh_thuc": "% tăng chính thức", "tu_tinh": "% tăng tự tính",
                                                      "chenh_lech": "Chênh lệch (điểm %)"})[
                ["Tăng (chính thức)", "Giảm (chính thức)", "% tăng chính thức", "% tăng tự tính", "Chênh lệch (điểm %)"]],
                use_container_width=True)
            st.caption("Tự tính chỉ dùng các mã cùng sàn đủ thanh khoản ở ngưỡng đang chọn, còn số chính thức tính mọi mã; chênh lệch nhỏ là bình thường.")

# ===== Tab 2: xếp hạng cổ phiếu =====
with tabs[1]:
    st.subheader("Cổ phiếu nào tốt, và cổ phiếu nào mạnh hơn VN-Index?")
    meta = sv.symbol_meta(con)
    f1, f2_, f3, f4 = st.columns([2, 2, 3, 2])
    query = f1.text_input("Tìm mã hoặc tên", "", placeholder="VD: HPG, thép...").strip().lower()
    boards = f2_.multiselect("Sàn", ["HOSE", "HNX", "UPCOM"], default=["HOSE", "HNX", "UPCOM"])
    sectors_all = sorted(meta["icb_name"].dropna().unique()) if "icb_name" in meta else []
    sectors = f3.multiselect("Ngành (để trống = tất cả)", sectors_all)
    f4.selectbox("Thanh khoản tối thiểu (trung vị giá trị khớp 60 phiên/ngày)", list(THRESHOLDS), index=1, key="th_label",
                 help="Chỉ xếp hạng các mã có giao dịch ≥ 80% số phiên và đủ ngưỡng này. Đổi ngưỡng sẽ tính lại toàn bộ phân tích.")
    with st.expander("Trọng số điểm (mặc định bằng nhau, không tối ưu trên dữ liệu)"):
        wc = st.columns(4)
        w = {k: wc[i].slider(lab, 0, 100, 25, 5, key=f"w_{k}") for i, (k, lab) in enumerate(GROUPS)}
    n_total = A["panel"].close.shape[1]
    n_el = int(A["eligible"].loc[asof].sum())
    st.caption(f"Universe: {n_total:,} mã HOSE/HNX/UPCoM có trong dữ liệu → {n_el} mã đủ điều kiện tại {asof:%d/%m/%Y} "
               f"(trung vị giá trị khớp 60 phiên ≥ {th_label}/phiên và có giao dịch ≥ 80% số phiên). "
               "Điểm = trung bình có trọng số của 4 nhóm; mỗi nhóm là thứ hạng phần trăm giữa các mã đủ điều kiện.")

    if sum(w.values()) == 0:
        st.warning("Tổng trọng số bằng 0: hãy đặt ít nhất một nhóm lớn hơn 0.")
    elif not boards:
        st.warning("Hãy chọn ít nhất một sàn.")
    else:
        table = sv.rank_table(A, asof, w, boards, meta)
        if sectors:
            table = table[table["icb_name"].isin(sectors)]
        if query:
            hay = table.index.to_series().str.lower() + " " + table["name_vi"].fillna("").str.lower()
            table = table[hay.str.contains(query, regex=False)]
        if table.empty:
            st.info("Không có mã nào thỏa bộ lọc ở ngày này. Thử bỏ bớt bộ lọc hoặc hạ ngưỡng thanh khoản.")
        else:
            names = {"trend": "Xu hướng", "momentum": "Động lượng", "liquidity": "Thanh khoản", "risk": "Rủi ro thấp (cao = tốt)",
                     "score": "Điểm", "vol_60_%": "Biến động 60p (%/năm)", "mdd_126_%": "Sụt giảm tối đa 126p (%)",
                     "mom_126_21_%": "Động lượng 126-21p (%)", "rs_63_pp": "RS 63p so với VN-Index (điểm %)",
                     "liq_median_ty": "GT khớp TV (tỷ/phiên)", "board": "Sàn", "name_vi": "Tên", "icb_name": "Ngành"}
            sc1, sc2, sc3 = st.columns([2, 1, 2])
            core = ["Sàn", "Ngành", "Điểm", "Xu hướng", "Động lượng", "Thanh khoản", "Rủi ro thấp (cao = tốt)"]
            more = sc3.checkbox("Hiện thêm chỉ báo chi tiết", False)
            extra = ["Tên", "Biến động 60p (%/năm)", "Sụt giảm tối đa 126p (%)", "Động lượng 126-21p (%)",
                     "RS 63p so với VN-Index (điểm %)", "GT khớp TV (tỷ/phiên)"]
            shown_cols = core + (extra if more else [])
            sort_by = sc1.selectbox("Sắp xếp theo", [c for c in shown_cols if c not in ("Sàn", "Ngành", "Tên")], index=0)
            top_n = sc2.slider("Số dòng", 10, 100, 20, 5)
            ren = table.rename(columns=names).rename_axis("Mã")
            ren = ren.sort_values(sort_by, ascending=False)
            view = ren.head(top_n)[shown_cols].reset_index()
            cfg = {"Điểm": st.column_config.ProgressColumn("Điểm", min_value=0, max_value=100, format="%.1f")}
            for c in shown_cols:
                if c not in ("Sàn", "Ngành", "Tên", "Điểm"):
                    cfg[c] = st.column_config.NumberColumn(c, format="%.1f")
            st.caption(f"{len(table)} mã thỏa bộ lọc; hiển thị {len(view)}. Bấm vào một dòng để xem chi tiết bên dưới. "
                       "**Điểm Rủi ro thấp: cao = rủi ro THẤP (tốt)**; mọi điểm nhóm đều theo hướng 'cao = tốt'.")
            sel_sym = None
            try:
                ev = st.dataframe(view, use_container_width=True, hide_index=True, column_config=cfg,
                                  on_select="rerun", selection_mode="single-row", key="rank_table")
                rows_sel = list(ev.selection.rows) if ev is not None else []
                if rows_sel:
                    sel_sym = view.iloc[rows_sel[0]]["Mã"]
            except TypeError:   # phiên bản Streamlit cũ không hỗ trợ chọn dòng: dùng ô chọn bên dưới
                st.dataframe(view, use_container_width=True, hide_index=True, column_config=cfg)
            st.download_button("Tải bảng xếp hạng (CSV)", ren.to_csv().encode("utf-8-sig"),
                               file_name=f"xep_hang_{asof:%Y%m%d}.csv", mime="text/csv")

            # ---- đóng góp của các nhóm vào điểm ----
            wn = {k: v / sum(w.values()) for k, v in w.items()}
            top15 = table.sort_values("score", ascending=False).head(15)
            fig = go.Figure()
            for k, lab in GROUPS:
                fig.add_trace(go.Bar(x=top15.index, y=top15[k] * wn[k], name=lab, text=(top15[k] * wn[k]).round(1),
                                     texttemplate="%{text}", textposition="inside"))
            show(style(fig, "Đóng góp có trọng số của từng nhóm vào điểm tổng (15 mã đứng đầu)", 360, barmode="stack"))
            st.caption("Đóng góp của một nhóm = điểm nhóm (0-100) × trọng số đã chuẩn hóa. Tổng bốn đóng góp bằng đúng điểm tổng; "
                       "khi trọng số bằng nhau, mỗi nhóm đóng góp tối đa 25 điểm.")

            # ---- Beta - RS ----
            bq = sv.cross_beta(A, asof, list(table.index)).dropna()
            sel_ready = list(view["Mã"])
            sym = st.selectbox("Mã cần xem chi tiết (hoặc bấm vào dòng trong bảng)", sel_ready,
                               index=sel_ready.index(sel_sym) if sel_sym in sel_ready else 0)
            if len(bq) > 5:
                min_sc = st.slider("Chỉ vẽ các mã có điểm tổng từ", 0, 100, 0, 5)
                q = table.loc[bq.index]
                q = q[q["score"] >= min_sc]
                bqf = bq.loc[q.index]
                figq = go.Figure()
                for pos, sym_name, label in ((True, "circle", "RS ≥ 0 (mạnh hơn VN-Index)"), (False, "diamond", "RS < 0 (yếu hơn VN-Index)")):
                    m_ = (q["rs_63_pp"] >= 0) if pos else (q["rs_63_pp"] < 0)
                    if m_.any():
                        figq.add_trace(go.Scatter(
                            x=bqf[m_], y=q.loc[m_, "rs_63_pp"], mode="markers", name=label, text=q.index[m_],
                            customdata=np.c_[q.loc[m_, "score"], q.loc[m_, "icb_name"].fillna("—")],
                            hovertemplate="<b>%{text}</b><br>Beta %{x:.2f}<br>RS %{y:.1f} điểm %<br>Điểm %{customdata[0]:.1f}"
                                          "<br>Ngành %{customdata[1]}<extra></extra>",
                            marker=dict(size=8, symbol=sym_name, color=q.loc[m_, "score"], colorscale="Viridis", cmin=0, cmax=100,
                                        showscale=pos, colorbar=dict(title="Điểm") if pos else None)))
                if sym in q.index and sym in bqf.index:
                    figq.add_trace(go.Scatter(x=[bqf[sym]], y=[q.loc[sym, "rs_63_pp"]], mode="markers+text", text=[sym],
                                              textposition="top center", name=f"Đang chọn: {sym}",
                                              marker=dict(size=16, symbol="circle-open", line=dict(width=3))))
                figq.add_hline(y=0, line_dash="dot")
                figq.add_vline(x=1, line_dash="dot")
                for (xr, yr, txt) in ((0.99, 0.98, "Beta > 1, RS > 0<br>Vượt trội, nhạy hơn thị trường"),
                                      (0.01, 0.98, "Beta < 1, RS > 0<br>Vượt trội, ít nhạy hơn"),
                                      (0.99, 0.02, "Beta > 1, RS < 0<br>Kém hơn, nhạy hơn"),
                                      (0.01, 0.02, "Beta < 1, RS < 0<br>Kém hơn, ít nhạy hơn")):
                    figq.add_annotation(xref="paper", yref="paper", x=xr, y=yr, text=txt, showarrow=False, opacity=0.7,
                                        xanchor="right" if xr > 0.5 else "left", yanchor="top" if yr > 0.5 else "bottom",
                                        font=dict(size=11))
                show(style(figq, "Beta (120 phiên) và sức mạnh tương đối 63 phiên so với VN-Index", 480,
                           xaxis_title="Beta so với VN-Index", yaxis_title="Sức mạnh tương đối (điểm %)"))
                st.caption("Hình dạng điểm (tròn/thoi) và nhãn bốn vùng cho biết RS dương/âm, không chỉ dựa vào màu. "
                           "Beta thấp không đồng nghĩa an toàn; beta cao không mặc định là xấu.")

            # ---- chi tiết một mã ----
            st.markdown(f"### Chi tiết {sym}")
            row_ = table.loc[sym]
            info_ = [row_.get(k) for k in ("name_vi", "board", "icb_name")]
            st.caption(" · ".join(str(v) for v in info_ if isinstance(v, str) and v))
            cc = pd.DataFrame({"Điểm nhóm (0-100)": [row_[k] for k, _ in GROUPS], "Trọng số": [wn[k] for k, _ in GROUPS],
                               "Đóng góp vào điểm tổng": [row_[k] * wn[k] for k, _ in GROUPS]},
                              index=[lab for _, lab in GROUPS])
            d_a, d_b = st.columns([2, 3])
            d_a.metric("Điểm tổng", f"{row_['score']:.1f}")
            d_a.dataframe(cc.round(2), use_container_width=True)
            px_ = A["panel"].close[sym].dropna()
            px_ = px_[(px_.index >= start) & (px_.index <= asof)]
            base_ix = A["panel"].index_close.reindex(px_.index).ffill()
            if len(px_) > 5:
                rs = (px_ / px_.iloc[0]) / (base_ix / base_ix.iloc[0]) * 100
                figd = go.Figure()
                figd.add_trace(go.Scatter(x=px_.index, y=px_ / px_.iloc[0] * 100, name=f"{sym} (gốc 100)"))
                figd.add_trace(go.Scatter(x=base_ix.index, y=base_ix / base_ix.iloc[0] * 100, name="VN-Index (gốc 100)"))
                figd.add_trace(go.Scatter(x=rs.index, y=rs, name="Đường RS = giá mã / VN-Index (gốc 100)", line=dict(dash="dot")))
                with d_b:
                    show(style(figd, f"{sym} so với VN-Index, chuẩn hóa về 100 tại {px_.index[0]:%d/%m/%Y}", 340))
                st.caption("Đường RS đi lên: cổ phiếu đang vượt trội VN-Index; đi xuống: kém hơn VN-Index. "
                           f"RS = (giá mã / giá mã ngày đầu) ÷ (VN-Index / VN-Index ngày đầu) × 100, bắt đầu từ {px_.index[0]:%d/%m/%Y}.")
                rel = sv.market_relation(A, sym, asof)
                r1, r2, r3 = st.columns(3)
                r1.metric("Beta (120 phiên)", "—" if pd.isna(rel["beta"]) else f"{rel['beta']:.2f}",
                          help="Hệ số hồi quy lợi suất ngày của mã theo lợi suất ngày VN-Index trong 120 phiên gần nhất. "
                               "> 1: biến động mạnh hơn thị trường; < 1: nhẹ hơn.")
                r2.metric("Tương quan với VN-Index", "—" if pd.isna(rel["corr"]) else f"{rel['corr']:.2f}",
                          help="Hệ số tương quan Pearson của lợi suất ngày, 120 phiên. Gần 1: đi cùng chiều thị trường; gần 0: ít liên quan.")
                r3.metric("RS 63 phiên (điểm %)", f"{row_['rs_63_pp']:.1f}",
                          help="Lợi suất của mã trừ lợi suất VN-Index trong 63 phiên gần nhất, tính bằng điểm phần trăm.")
                st.markdown(f"**{sym} hành xử thế nào theo trạng thái thị trường?** (phiên được phân loại theo trạng thái của "
                            "phiên TRƯỚC, nên không dùng thông tin chưa biết)")
                reg_tab = sv.stock_by_regime(A, sym, asof)
                if reg_tab.empty:
                    st.info("Chưa đủ dữ liệu để phân tích theo trạng thái thị trường.")
                else:
                    st.dataframe(reg_tab.rename(columns={
                        "so_phien": "Số phiên quan sát", "ln_mã_%/năm": f"LN {sym} (%/năm)", "ln_VNIndex_%/năm": "LN VN-Index (%/năm)",
                        "chênh_lệch_pp": "Chênh lệch (điểm %/năm)", "beta": "Beta", "%_phiên_tăng": f"% phiên {sym} tăng"}).round(2),
                        use_container_width=True)
                    st.caption("Lợi suất bình quân ngày quy năm. Số phiên giữa các trạng thái khác nhau nhiều và các phiên liền kề "
                               "không độc lập, nên kết quả chỉ nên đọc như mô tả, không phải dự báo.")
            else:
                st.info("Không đủ dữ liệu giá của mã này trong khoảng thời gian đã chọn.")

# ===== Tab 3: Realtime Monitor =====
with tabs[2]:
    st.subheader("Realtime Monitor — dữ liệu WebSocket SSI đã lưu")
    box = {"ok": st.success, "info": st.info, "warn": st.warning}[ws["level"]]
    box(f"**{ws['state']}.** {ws['detail']}")
    st.caption("Bộ ghi streaming chạy như một tiến trình riêng và lưu vào bảng `stream_raw`; trang này đọc dữ liệu đã lưu. "
               "Số bản tin nhận nhưng không lưu (vd. bảng giá bid/ask) chỉ hiện trong cửa sổ lệnh của bộ ghi.")
    with st.expander("Cách khởi chạy bộ ghi", expanded=not ss_status["available"]):
        st.code(sv.START_CMD, language="bash")
        st.caption("Chạy trong thư mục repo mẫu của SSI (có `python/stream_recorder.py`), giờ giao dịch 9:00-11:30 và 13:00-14:45. "
                   "Có thể cần duyệt Smart OTP trên điện thoại. Chỉ SUBSCRIBE dữ liệu thị trường, không đặt lệnh.")
    if ss_status["available"]:
        st.markdown(f"**Đã ghi {ss_status['sessions']} phiên, {ss_status['rows']:,} bản tin.**")
        st.dataframe(ss_status["by_day"].rename(columns={"ngay": "Ngày", "dong": "Số bản tin", "tu": "Bản tin đầu", "den": "Bản tin cuối"}),
                     use_container_width=True, hide_index=True)
        day = st.selectbox("Chọn phiên", list(ss_status["by_day"]["ngay"])[::-1])
        trades, press, foreign, cross, counts = get_stream_day(DB, sig, day)
        k1, k2, k3 = st.columns(3)
        k1.metric("Bản tin đã lưu trong phiên", f"{int(counts['so_ban_tin'].sum()):,}")
        k2.metric("Bản tin đầu (thời điểm bắt đầu ghi)", counts["dau"].min()[11:19] if len(counts) else "—")
        k3.metric("Bản tin cuối", counts["cuoi"].max()[11:19] if len(counts) else "—")
        st.dataframe(counts.rename(columns={"loai": "Loại bản tin", "so_ban_tin": "Số bản tin đã lưu", "dau": "Đầu", "cuoi": "Cuối"}),
                     use_container_width=True, hide_index=True)
        if trades.empty:
            st.info("Phiên này không có bản tin khớp lệnh.")
        else:
            wl = sv.stream_watchlist(trades)
            st.markdown(f"**Danh sách mã đang theo dõi ({len(wl)} mã):** {', '.join(wl.index)}")
            st.dataframe(wl.rename(columns={"gia_cuoi": "Giá khớp gần nhất", "gia_dau_ghi": "Giá khớp đầu tiên ghi được",
                                            "doi_%": "Đổi so với giá đầu (%)", "kl_luy_ke": "KL khớp lũy kế",
                                            "so_lenh": "Số bản tin khớp", "luc_cuoi": "Lúc cuối"}).round(2),
                         use_container_width=True,
                         column_config={"Đổi so với giá đầu (%)": st.column_config.NumberColumn(format="%.2f"),
                                        "KL khớp lũy kế": st.column_config.NumberColumn(format="%d")})
            st.caption("“Đổi so với giá đầu” tính từ lần khớp đầu tiên bộ ghi nhận được, không phải so với giá tham chiếu của sàn.")
            pick = st.selectbox("Diễn biến giá khớp trong phiên của mã", list(wl.index))
            tp = trades[trades["symbol"] == pick].copy()
            tp["t"] = pd.to_datetime(tp["recv_time"], errors="coerce")
            show(style(go.Figure(go.Scatter(x=tp["t"], y=tp["price"], mode="lines+markers", name=pick)),
                       f"Giá khớp {pick} theo thời gian nhận bản tin", 300))

            st.markdown("**Áp lực mua/bán chủ động** (giá trị khớp theo chiều lệnh chủ động; ròng = mua chủ động − bán chủ động)")
            buy, sell = sv.top_pressure(press)
            cc1, cc2 = st.columns(2)
            if len(buy):
                f1_ = go.Figure(go.Bar(x=buy["rong"] / 1e6, y=buy.index, orientation="h", marker_color=REGIME_COLOR["Mạnh"]))
                with cc1:
                    show(style(f1_, "Mua ròng chủ động lớn nhất (triệu đồng)", 340, yaxis=dict(autorange="reversed")))
            else:
                cc1.info("Không có mã nào mua ròng chủ động trong dữ liệu đã ghi.")
            if len(sell):
                f2s = go.Figure(go.Bar(x=sell["rong"] / 1e6, y=sell.index, orientation="h", marker_color=REGIME_COLOR["Yếu"]))
                with cc2:
                    show(style(f2s, "Bán ròng chủ động lớn nhất (triệu đồng)", 340, yaxis=dict(autorange="reversed")))
            else:
                cc2.info("Không có mã nào bán ròng chủ động trong dữ liệu đã ghi.")
            st.caption("Chiều lệnh (mua/bán chủ động) lấy từ trường `side` của bản tin khớp lệnh SSI; chỉ phản ánh các mã và "
                       "khoảng thời gian bộ ghi chạy, không phải toàn thị trường.")
            if not foreign.empty:
                st.markdown("**Khối ngoại trong phiên** (lũy kế mua/bán theo bản tin room mới nhất của từng mã)")
                fx = foreign.assign(mua=lambda x: x["mua"] / 1e6, ban=lambda x: x["ban"] / 1e6, rong=lambda x: x["rong"] / 1e6)
                st.dataframe(fx.rename(columns={"mua": "Mua (triệu đồng)", "ban": "Bán (triệu đồng)", "rong": "Ròng (triệu đồng)",
                                                "room_con_lai": "Room còn lại (cp)", "room_tong": "Room tổng (cp)"}).head(30).round(1),
                             use_container_width=True,
                             column_config={"Room còn lại (cp)": st.column_config.NumberColumn(format="%d"),
                                            "Room tổng (cp)": st.column_config.NumberColumn(format="%d")})
            st.markdown("**Đối chiếu chéo với dữ liệu REST** (khối lượng khớp cuối phiên từ streaming so với `totalMatch`)")
            if cross.empty:
                st.info("Chưa có dữ liệu để đối chiếu.")
            else:
                st.write(cross["ket_luan"].value_counts().to_dict())
                st.dataframe(cross.round(2), use_container_width=True, hide_index=True)
                st.caption("Chỉ so sánh khi bộ ghi chạy tới cuối phiên (bản tin cuối ≥ 14:40); ngược lại ghi “chưa ghi hết phiên”.")

# ===== Tab 4: kiểm chứng =====
with tabs[3]:
    st.subheader("Phương pháp có hiệu quả khi kiểm chứng bằng dữ liệu lịch sử không?")
    st.caption("Tín hiệu tính tới hết phiên t, vào lệnh ở giá đóng cửa phiên t+1, tái cân bằng hằng tháng, có chi phí giao dịch. "
               "Không tham số nào được tối ưu trên dữ liệu (trọng số bằng nhau, ngưỡng 1/3 – 2/3), nên không có tập huấn luyện để "
               "“lọt” sang tập kiểm tra; độ ổn định được kiểm tra bằng cách chia hai giai đoạn liên tiếp.")
    cc1, cc2 = st.columns(2)
    frac = cc1.selectbox("Tỷ lệ mã được chọn mỗi tháng", [0.05, 0.10, 0.20], index=1, format_func=lambda x: f"Top {x:.0%}")
    cost = cc2.selectbox("Chi phí mỗi chiều (bps) — dùng 60 để mô phỏng thêm trượt giá", [15, 30, 60], index=1)
    if st.button("Chạy kiểm chứng"):
        st.session_state["bt_result"] = get_backtest(DB, sig, min_value, frac, float(cost))
    R = st.session_state.get("bt_result")
    if R is None:
        st.info("Bấm 'Chạy kiểm chứng' để tính (lần đầu mất 1-3 phút, các lần sau dùng bộ nhớ đệm). Mặc định Top 10%, 30 bps — "
                "cấu hình dùng trong báo cáo.")
    elif R["params"][0] != min_value:
        st.info("Ngưỡng thanh khoản đã đổi so với lần chạy trước: bấm 'Chạy kiểm chứng' để tính lại.")
    else:
        res = R["bt"]
        st.caption(f"Kết quả đang hiển thị: Top {R['params'][1]:.0%}, phí {R['params'][2]:.0f} bps/chiều.")
        if len(res) < bt.MIN_MONTHS:
            st.warning(f"Không đủ dữ liệu để kiểm chứng (cần ≥ {bt.MIN_MONTHS} tháng, hiện có {len(res)}).")
        else:
            t1, t2, t3, t4 = st.tabs(["1. Điểm thị trường", "2. Bộ sàng lọc cổ phiếu", "3. Danh mục", "4. Theo trạng thái thị trường"])
            with t1:
                for hz, key in ((5, "regime5"), (21, "regime21")):
                    reg = R[key]
                    st.markdown(f"**Lợi suất VN-Index {hz} phiên tới theo trạng thái** (mẫu không chồng lấn mỗi {hz} phiên"
                                + (", ≈ 1 tháng" if hz == 21 else "") + ")")
                    tab = (reg["table"] * [1, 100, 100, 100]).rename(
                        columns={"n": "Số kỳ", "mean": "TB (%)", "median": "Trung vị (%)", "pos": "% kỳ dương"}).round(2)
                    st.dataframe(tab, use_container_width=True)
                    b = reg["regression"]
                    st.caption(f"Hồi quy lợi suất {hz} phiên tới theo điểm: hệ số {b['beta']:.5f}/điểm, t (Newey-West, {hz} độ trễ) = "
                               f"{b['t']:.2f}, n = {b['n']} ngày. "
                               + ("**Có ý nghĩa ở mức 5%** (|t| ≥ 1,96)." if abs(b["t"]) >= 1.96 else "**Chưa có ý nghĩa thống kê ở mức 5%** (|t| < 1,96)."))
                st.caption("Lợi suất tương lai nhiều phiên chồng lấn nhau nên các quan sát không độc lập; vì vậy bảng dùng mẫu không "
                           "chồng lấn và hồi quy dùng sai số chuẩn Newey-West.")
            with t2:
                ic = R["ic"]
                if ic.empty:
                    st.info("Chưa đủ dữ liệu để tính IC.")
                else:
                    t_ic = ic["ic"].mean() / ic["ic"].std() * (len(ic) ** 0.5)
                    i1, i2, i3 = st.columns(3)
                    i1.metric("IC (Spearman) trung bình", f"{ic['ic'].mean():.3f}")
                    i2.metric("% tháng IC dương", f"{(ic['ic'] > 0).mean():.0%}")
                    i3.metric("t-stat của IC", f"{t_ic:.2f}", help="|t| ≥ 1,96 mới có ý nghĩa thống kê ở mức 5%.")
                    figi = go.Figure(go.Bar(x=ic.index, y=ic["ic"], marker_color=np.where(ic["ic"] >= 0, REGIME_COLOR["Mạnh"], REGIME_COLOR["Yếu"])))
                    show(style(figi, f"IC theo tháng (n = {len(ic)} tháng)", 260))
                    st.caption("IC = tương quan hạng giữa điểm cuối tháng và lợi suất tháng kế tiếp. IC nhỏ (vài phần trăm) là bình thường "
                               "của tín hiệu giá; điều quan trọng là mức ổn định và ý nghĩa thống kê.")
                curve = pd.DataFrame({"Top (đã trừ phí)": (1 + res["net"]).cumprod(), "Mọi mã đủ điều kiện (cân bằng)": (1 + res["universe"]).cumprod(),
                                      "VN-Index": (1 + res["index"]).cumprod()})
                curve.index = res["exit"]
                show(style(go.Figure([go.Scatter(x=curve.index, y=curve[c], name=c) for c in curve.columns]),
                           f"Top {R['params'][1]:.0%} so với VN-Index và toàn bộ universe — giá trị 1 đồng (log)", 340, yaxis_type="log"))
            with t3:
                stats = bt.summarize(res)
                st.dataframe((stats[["ann_return", "ann_vol", "max_drawdown", "hit_rate"]] * 100).round(1).join(stats["sharpe"].round(2)).rename(
                    columns={"ann_return": "LN năm (%)", "ann_vol": "Biến động năm (%)", "max_drawdown": "Sụt giảm tối đa (%)",
                             "hit_rate": "% tháng dương", "sharpe": "Sharpe"}), use_container_width=True)
                cs = bt.cost_summary(res)
                p1, p2, p3, p4 = st.columns(4)
                p1.metric("Số lần tái cân bằng", cs["so_lan_tai_can_bang"])
                p2.metric("Vòng quay TB/tháng", f"{cs['vong_quay_tb']:.0%}")
                p3.metric("Phí TB/tháng", f"{cs['phi_tb_thang'] * 100:.2f}%")
                p4.metric("Số mã giữ TB", f"{cs['so_ma_giu_tb']:.0f} / {cs['so_ma_xep_hang_tb']:.0f}")
                dd = (1 + res["net"]).cumprod()
                dd = dd / dd.cummax() - 1
                show(style(go.Figure(go.Scatter(x=res["exit"], y=dd * 100, fill="tozeroy", name="Sụt giảm")),
                           "Sụt giảm của danh mục top (đã trừ phí), %", 240))
                st.markdown("**Hai giai đoạn liên tiếp (kiểm tra độ ổn định)**")
                for name, sm in bt.split_periods(res).items():
                    if sm is None:
                        st.caption(f"{name}: không đủ {bt.MIN_MONTHS} tháng để thống kê.")
                    else:
                        st.caption(name)
                        st.dataframe((sm[["ann_return", "ann_vol", "max_drawdown", "hit_rate"]] * 100).round(1).join(sm["sharpe"].round(2)).rename(
                            columns={"ann_return": "LN năm (%)", "ann_vol": "Biến động năm (%)", "max_drawdown": "Sụt giảm tối đa (%)",
                                     "hit_rate": "% tháng dương", "sharpe": "Sharpe"}), use_container_width=True)
                st.caption("Chi phí giả định theo từng chiều giao dịch (30 bps mặc định); chọn 60 bps để mô phỏng thêm trượt giá. "
                           "Trượt giá thực tế và khả năng mua đủ khối lượng chưa được mô hình hóa.")
            with t4:
                ex = bt.excess_by_regime(res).reindex(["Yếu", "Trung tính", "Mạnh"]).dropna(how="all")
                st.markdown("**Danh mục top vượt “mọi mã đủ điều kiện” bao nhiêu (điểm %/tháng) theo trạng thái thị trường lúc ra tín hiệu**")
                st.dataframe(ex.round(2).rename(columns={"so_thang": "Số tháng", "vuot_tb_pp": "Vượt trội TB (điểm %/tháng)"}),
                             use_container_width=True)
                rel = R.get("relation") or {}
                if rel:
                    st.markdown("**Quan hệ thị trường – cổ phiếu (RQ3):** nhóm beta cao có hưởng lợi khi thị trường tăng, chịu thiệt khi giảm không?")
                    t3_ = (rel["by_regime"] * [1, 100, 100, 100]).round(2).rename(columns={
                        "so_thang": "Số tháng", "thi_truong_tb": "VN-Index TB (%/tháng)",
                        "chenh_lech_beta_tb": "Beta cao − thấp (%/tháng)", "chenh_lech_RS_tb": "RS cao − thấp (%/tháng)"})
                    st.dataframe(t3_, use_container_width=True)
                    bs = rel["beta_slope"]
                    st.caption(f"Hồi quy (lợi suất nhóm beta cao − thấp) theo lợi suất VN-Index cùng tháng: hệ số {bs['beta']:.2f}, "
                               f"t = {bs['t']:.2f} (n = {bs['n']} tháng). Hệ số dương nghĩa là nhóm beta cao khuếch đại biến động của thị trường "
                               f"(kết quả này gần như hiển nhiên theo định nghĩa beta). Sức mạnh tương đối duy trì sang tháng sau "
                               f"(RS cao − thấp): {rel['rs_mean'] * 100:.2f}%/tháng, t = {rel['rs_t']:.2f}.")
                st.caption("Mỗi trạng thái chỉ có vài chục tháng; chưa kiểm định riêng từng nhóm, nên đọc như gợi ý.")
            st.warning("Kết quả cho thấy tín hiệu yếu và không chắc chắn; không nên diễn giải là lợi thế đầu tư. "
                       "Có thiên lệch người sống sót, giá điều chỉnh ngược và chưa tính trượt giá thực tế.")

# ===== Tab 5: phương pháp =====
with tabs[4]:
    st.subheader("Phương pháp, giả định và giới hạn")
    st.markdown("**Bốn câu hỏi sản phẩm trả lời:** (1) Thị trường đang mạnh, trung tính hay yếu? (2) Cổ phiếu nào tốt về xu hướng, "
                "động lượng, thanh khoản, rủi ro? (3) Cổ phiếu nào mạnh hơn VN-Index? (4) Phương pháp có hiệu quả khi kiểm chứng lịch sử không? "
                "**Người dùng mục tiêu:** nhà đầu tư cá nhân, người học phân tích chứng khoán, chuyên viên phân tích.")
    with st.expander("Nguồn dữ liệu và universe", expanded=True):
        st.markdown(f"""
- **SSI FastConnect v3**: REST (`securitiesByBoard`, `securitiesSummary`, `indexSummary`) cho giá và khối lượng cuối ngày từ 2018; WebSocket
  (khớp lệnh, room khối ngoại, tick chỉ số) lưu vào `stream_raw`. Giờ lưu theo Asia/Ho_Chi_Minh (UTC+7).
- **Universe**: toàn bộ mã HOSE, HNX, UPCoM có dữ liệu. Một mã được **xếp hạng** nếu trung vị giá trị khớp 60 phiên ≥ ngưỡng thanh khoản
  (mặc định 1 tỷ đồng/phiên) và có giao dịch ≥ 80% số phiên; vì UPCoM có tỷ lệ phiên không giao dịch rất cao nên bộ lọc này loại phần lớn mã kém thanh khoản.
- **Dữ liệu thiếu/lỗi**: dòng nghi lỗi được **gắn cờ** (`data_status`: ok / no_trade / suspect / missing, cùng `flags`), không bị xóa.
  Giá lỗi (`bad_price`) bị coi là thiếu khi tính chỉ báo; phiên không giao dịch không tính vào thanh khoản. Giá đóng cửa đã điều chỉnh ngược.
""")
    with st.expander("Công thức các chỉ báo"):
        st.markdown(f"""
- **Xu hướng** = giá/SMA50 − 1 và giá/SMA200 − 1. **Động lượng** = lợi suất 126 phiên bỏ 21 phiên gần nhất, và RS 63 phiên.
- **Sức mạnh tương đối (RS) 63 phiên** = lợi suất 63 phiên của mã − lợi suất 63 phiên của VN-Index (điểm %).
- **Beta (120 phiên)** = Cov(r mã, r VN-Index) / Var(r VN-Index) trên lợi suất ngày; **tương quan** = hệ số Pearson cùng cửa sổ.
- **Thanh khoản** = trung vị giá trị khớp 60 phiên. **Rủi ro** = độ lệch chuẩn lợi suất ngày 60 phiên (quy năm) và sụt giảm tối đa 126 phiên;
  điểm Rủi ro được đảo chiều nên **cao = rủi ro thấp = tốt**.
""")
    with st.expander("Market Health Score và Stock Score"):
        st.markdown(f"""
- **Market Health Score** = trung bình của 3 trụ cột (trọng số 1/3): *xu hướng* (VN-Index so với SMA50, SMA200; SMA50 so với SMA200), *độ rộng*
  (% mã trên SMA50; tỷ lệ mã tăng trong 10 phiên, trên mã đủ thanh khoản) và *thanh khoản* (giá trị khớp trung vị 20 phiên so với 250 phiên).
  Mỗi chỉ báo được quy về **phân vị mở rộng chỉ dùng quá khứ** (cần ≥ 250 phiên), nên không nhìn trước.
- **Ngưỡng phân loại**: < {mh.LOW:.0f} Yếu · {mh.LOW:.0f}–{mh.HIGH:.0f} Trung tính · > {mh.HIGH:.0f} Mạnh (ba phần bằng nhau của thang phân vị; không tối ưu).
- **Stock Score** = trung bình có trọng số của 4 nhóm (mặc định 25% mỗi nhóm, theo lập luận 1/N); mỗi chỉ báo được xếp hạng phần trăm giữa các mã đủ điều kiện.
  Đóng góp của nhóm = điểm nhóm × trọng số chuẩn hóa; tổng đóng góp = điểm tổng.
""")
    with st.expander("Cách kiểm chứng"):
        st.markdown("""
- **Điểm thị trường**: lợi suất VN-Index 5 và 21 phiên tới theo trạng thái (mẫu không chồng lấn); hồi quy theo điểm với sai số Newey-West.
- **Điểm cổ phiếu**: IC (Spearman) theo tháng; danh mục top mỗi tháng, tỷ trọng bằng nhau; so sánh với VN-Index và toàn bộ universe.
- **Danh mục**: tín hiệu cuối tháng t, vào lệnh giá đóng cửa t+1, phí mỗi chiều 30 bps (độ nhạy 15/60 bps và ngưỡng thanh khoản 0,5/1/5 tỷ).
- **Theo trạng thái**: phân loại theo trạng thái tại ngày tín hiệu; đối với lợi suất ngày của một mã, dùng trạng thái của phiên trước.
- Không tham số nào được tối ưu trên dữ liệu; hai giai đoạn liên tiếp (trước/sau 2023) dùng để xem độ ổn định, không để chọn tham số.
""")
    with st.expander("Giả định và hạn chế", expanded=True):
        st.markdown("""
Điểm số là công cụ định lượng hỗ trợ phân tích, **không phải dự báo chắc chắn lợi suất tương lai** và không phải khuyến nghị đầu tư.
Thiên lệch người sống sót (mã đã hủy niêm yết không có trong dữ liệu); giá điều chỉnh ngược; chưa mô hình hóa trượt giá và khả năng mua đủ khối lượng;
độ rộng lịch sử tự tính từ giá chứ không phải số liệu chính thức của sàn; mã ít giao dịch bị loại; dữ liệu streaming mới ghi một số phiên và
chỉ gồm các mã đã đăng ký theo dõi. Kiểm chứng cho thấy tín hiệu yếu (xem tab Kiểm chứng). Không dùng kết quả để tự động đặt lệnh.
""")
    with st.expander("Cơ sở tham khảo (tự kiểm tra lại trước khi trích dẫn)"):
        st.markdown("Jegadeesh & Titman (1993); Levy (1967); Faber (2007); Ang, Hodrick, Xing & Zhang (2006); Amihud (2002); "
                    "DeMiguel, Garlappi & Uppal (2009); Newey & West (1987).")
