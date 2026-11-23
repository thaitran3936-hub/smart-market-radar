"""Kiểm chứng lịch sử (backtest) cho điểm thị trường và điểm cổ phiếu.

Nguyên tắc chống sai lệch nhìn trước:
- Tín hiệu tính từ dữ liệu tới hết phiên t; vào lệnh ở giá đóng cửa phiên t+1 (không dùng giá phiên t).
- Tái cân bằng cuối mỗi tháng; giữ tới giá đóng cửa phiên sau ngày tín hiệu của kỳ kế tiếp.
- Chi phí giao dịch tính trên giá trị mua + bán (cost_bps mỗi chiều).
- Mô hình KHÔNG có tham số nào được tối ưu trên dữ liệu (trọng số bằng nhau, ngưỡng 33/67 cố định) nên
  không có "mẫu huấn luyện" để quá khớp; kết quả được báo cáo theo 2 giai đoạn để xem độ ổn định.
- Lợi suất tương lai chồng lấn (21 phiên) làm sai t-stat thông thường: dùng Newey-West và lấy mẫu không chồng lấn.
Hạn chế phải nêu: thiên lệch người sống sót (mã đã hủy niêm yết không có trong dữ liệu), giá điều chỉnh ngược,
chưa mô hình hóa trượt giá/tác động thị trường, chưa kiểm tra khả năng mua đủ khối lượng.
"""
import argparse

import numpy as np
import pandas as pd

from src.analytics import stock_score as ss
from src.finance import indicators as ind


MIN_MONTHS = 12   # số tháng tối thiểu để báo cáo chỉ số hiệu suất của một giai đoạn


# ---------- thống kê ----------
def newey_west_slope(y: np.ndarray, x: np.ndarray, lags: int) -> dict:
    """Hồi quy y = a + b·x, sai số chuẩn Newey-West (HAC). Trả về b, t, n."""
    m = ~(np.isnan(y) | np.isnan(x))
    y, x = y[m], x[m]
    n = len(y)
    if n < lags + 5:
        return {"beta": np.nan, "t": np.nan, "n": n}
    X = np.column_stack([np.ones(n), x])
    xtx_inv = np.linalg.inv(X.T @ X)
    beta = xtx_inv @ X.T @ y
    e = y - X @ beta
    u = X * e[:, None]
    S = u.T @ u
    for l in range(1, lags + 1):
        w = 1 - l / (lags + 1)
        G = u[l:].T @ u[:-l]
        S += w * (G + G.T)
    V = xtx_inv @ S @ xtx_inv
    return {"beta": float(beta[1]), "t": float(beta[1] / np.sqrt(V[1, 1])), "n": n}


def perf_stats(r: pd.Series, periods_per_year: int = 12, rf_annual: float = 0.0) -> dict:
    """Chỉ số hiệu suất từ chuỗi lợi suất theo kỳ."""
    r = r.dropna()
    if len(r) < 3:
        return {"n": len(r)}
    cum = (1 + r).cumprod()
    years = len(r) / periods_per_year
    vol = r.std() * np.sqrt(periods_per_year)
    ann = cum.iloc[-1] ** (1 / years) - 1
    return {"n": len(r), "ann_return": ann, "ann_vol": vol,
            "sharpe": (r.mean() - rf_annual / periods_per_year) / r.std() * np.sqrt(periods_per_year),
            "max_drawdown": float((cum / cum.cummax() - 1).min()), "hit_rate": float((r > 0).mean())}


# ---------- kiểm chứng điểm thị trường ----------
def regime_forward_returns(health: pd.DataFrame, index_close: pd.Series, horizon: int = 21) -> dict:
    """Lợi suất VN-Index `horizon` phiên TỚI theo trạng thái thị trường tại ngày tín hiệu.

    Trả về: bảng theo trạng thái (lấy mẫu không chồng lấn mỗi `horizon` phiên) và hồi quy
    lợi suất tương lai theo điểm (Newey-West, lags = horizon)."""
    fwd = index_close.shift(-horizon) / index_close - 1
    d = health.join(fwd.rename("fwd")).dropna(subset=["score", "fwd"])
    nonover = d.iloc[::horizon]
    tab = nonover.groupby("regime")["fwd"].agg(n="count", mean="mean", median="median",
                                               pos=lambda s: (s > 0).mean())
    reg = newey_west_slope(d["fwd"].to_numpy(), d["score"].to_numpy(), lags=horizon)
    return {"table": tab.reindex(["Yếu", "Trung tính", "Mạnh"]), "regression": reg,
            "unconditional_mean": float(nonover["fwd"].mean()), "n_days": len(d)}


# ---------- backtest điểm cổ phiếu ----------
def month_end_signal_dates(index: pd.DatetimeIndex, start: str) -> list:
    s = pd.Series(index, index=index)
    last = s.groupby([index.year, index.month]).last()
    return [d for d in last.tolist() if d >= pd.Timestamp(start)]


def stock_backtest(close, index_close, eligible, factors, health, weights=None, top_frac=0.10,
                   min_names=10, cost_bps=30.0, start="2019-06-01") -> pd.DataFrame:
    """Mỗi tháng chọn top `top_frac` theo điểm (tối thiểu `min_names` mã), tỷ trọng bằng nhau."""
    idx = close.index
    sig = month_end_signal_dates(idx, start)
    prev_w: pd.Series = pd.Series(dtype=float)
    rows = []
    for k in range(len(sig) - 1):
        d, d_next = sig[k], sig[k + 1]
        i, j = idx.get_loc(d), idx.get_loc(d_next)
        if i + 1 >= len(idx) or j + 1 >= len(idx):
            break
        entry, exit_ = idx[i + 1], idx[j + 1]
        table = ss.score_on(d, factors, eligible, weights)
        if len(table) < min_names:
            continue
        n_sel = max(min_names, int(round(len(table) * top_frac)))
        sel = list(table.index[:n_sel])
        r_all = (close.loc[exit_, table.index] / close.loc[entry, table.index] - 1).dropna()
        r_sel = r_all.reindex(sel).dropna()
        if r_sel.empty:
            continue
        w = pd.Series(1.0 / len(r_sel), index=r_sel.index)
        # giá trị giao dịch = tổng |tỷ trọng mới - tỷ trọng cũ| (mua + bán); chưa tính trôi tỷ trọng giữa kỳ
        traded = float(w.sub(prev_w, fill_value=0).abs().sum())  # phép trừ đã gộp cả mã chỉ có ở kỳ trước
        cost = traded * cost_bps / 1e4
        reg = health["regime"].asof(d) if "regime" in health else None
        rows.append({"signal": d, "entry": entry, "exit": exit_, "n_ranked": len(table), "n_held": len(r_sel),
                     "gross": float(r_sel.mean()), "cost": cost, "net": float(r_sel.mean()) - cost,
                     "universe": float(r_all.mean()),
                     "index": float(index_close.loc[exit_] / index_close.loc[entry] - 1),
                     "turnover": traded / 2, "regime": reg})
        prev_w = w
    return pd.DataFrame(rows).set_index("signal") if rows else pd.DataFrame()


def information_coefficient(close, index_close, eligible, factors, start="2019-06-01", weights=None) -> pd.DataFrame:
    """Tương quan hạng (Spearman) giữa điểm tại cuối tháng và lợi suất kỳ kế tiếp, từng tháng."""
    idx = close.index
    sig = month_end_signal_dates(idx, start)
    out = []
    for k in range(len(sig) - 1):
        d, d_next = sig[k], sig[k + 1]
        i, j = idx.get_loc(d), idx.get_loc(d_next)
        if j + 1 >= len(idx):
            break
        t = ss.score_on(d, factors, eligible, weights)
        fwd = (close.iloc[j + 1][t.index] / close.iloc[i + 1][t.index] - 1)
        m = pd.concat([t["score"], fwd.rename("fwd")], axis=1).dropna()
        if len(m) >= 20:
            out.append({"signal": d, "ic": m["score"].corr(m["fwd"], method="spearman"), "n": len(m)})
    return pd.DataFrame(out).set_index("signal") if out else pd.DataFrame()


def extreme_monthly_returns(close, eligible, start="2019-06-01", top: int = 10) -> tuple[pd.DataFrame, dict]:
    """Kiểm tra giá trị cực đoan: lợi suất kỳ (như backtest) lớn nhất theo trị tuyệt đối, và độ nhạy của
    danh mục 'mọi mã' khi giới hạn lợi suất mỗi mã trong ±50% (winsorize)."""
    idx = close.index
    sig = month_end_signal_dates(idx, start)
    recs, avg, avg_clip = [], [], []
    for k in range(len(sig) - 1):
        d, d_next = sig[k], sig[k + 1]
        i, j = idx.get_loc(d), idx.get_loc(d_next)
        if j + 1 >= len(idx):
            break
        names = eligible.loc[d][eligible.loc[d]].index
        r = (close.iloc[j + 1][names] / close.iloc[i + 1][names] - 1).dropna()
        if r.empty:
            continue
        avg.append(r.mean())
        avg_clip.append(r.clip(-0.5, 0.5).mean())
        for sym, v in r.items():
            recs.append((sym, idx[i + 1], idx[j + 1], close.iloc[i + 1][sym], close.iloc[j + 1][sym], v))
    df = pd.DataFrame(recs, columns=["symbol", "entry", "exit", "close_entry", "close_exit", "ret"])
    worst = df.reindex(df["ret"].abs().sort_values(ascending=False).index).head(top)
    cmp = {"vol_goc": perf_stats(pd.Series(avg))["ann_vol"], "vol_cat_±50%": perf_stats(pd.Series(avg_clip))["ann_vol"],
           "ret_goc": perf_stats(pd.Series(avg))["ann_return"], "ret_cat_±50%": perf_stats(pd.Series(avg_clip))["ann_return"],
           "n_quan_sat": len(df), "n_|r|>50%": int((df["ret"].abs() > 0.5).sum()), "n_|r|>100%": int((df["ret"].abs() > 1.0).sum())}
    return worst, cmp


def relation_analysis(close, index_close, eligible, factors, health, start="2019-06-01", window=120,
                      min_names=30) -> pd.DataFrame:
    """RQ3 - quan hệ thị trường và cổ phiếu, theo tháng.

    Cuối mỗi tháng: chia các mã đủ điều kiện thành 3 nhóm theo beta (120 phiên) và 3 nhóm theo sức mạnh tương đối
    63 phiên; đo lợi suất kỳ tới (vào lệnh ở phiên t+1) của từng nhóm, so với lợi suất VN-Index cùng kỳ.
    Câu hỏi: nhóm beta cao có thực sự hưởng lợi khi thị trường tăng và chịu thiệt khi thị trường giảm? Sức mạnh tương
    đối hôm nay có duy trì sang tháng sau không?"""
    idx = close.index
    ret = ind.daily_returns(close)
    rm = ind.daily_returns(index_close.reindex(idx).to_frame("m"))["m"]
    sig = month_end_signal_dates(idx, start)
    rows = []
    for k in range(len(sig) - 1):
        d, d_next = sig[k], sig[k + 1]
        i, j = idx.get_loc(d), idx.get_loc(d_next)
        if j + 1 >= len(idx):
            break
        names = eligible.loc[d][eligible.loc[d]].index
        beta = ind.beta_at(ret[names], rm, d, window).dropna()
        rs = factors["rs_63"].loc[d].reindex(beta.index)
        fwd = (close.iloc[j + 1][beta.index] / close.iloc[i + 1][beta.index] - 1)
        m = pd.concat([beta.rename("beta"), rs.rename("rs"), fwd.rename("fwd")], axis=1).dropna()
        if len(m) < min_names:
            continue
        tb = pd.qcut(m["beta"], 3, labels=["Thấp", "Vừa", "Cao"])
        tr = pd.qcut(m["rs"], 3, labels=["Thấp", "Vừa", "Cao"])
        gb, gr = m.groupby(tb, observed=True)["fwd"].mean(), m.groupby(tr, observed=True)["fwd"].mean()
        rows.append({"signal": d, "regime": health["regime"].asof(d), "n": len(m),
                     "market": float(index_close.loc[idx[j + 1]] / index_close.loc[idx[i + 1]] - 1),
                     "beta_low": gb.get("Thấp"), "beta_high": gb.get("Cao"),
                     "rs_low": gr.get("Thấp"), "rs_high": gr.get("Cao"), "median_beta": float(m["beta"].median())})
    return pd.DataFrame(rows).set_index("signal") if rows else pd.DataFrame()


def summarize_relation(rel: pd.DataFrame) -> dict:
    """Tổng hợp RQ3: (1) 'chênh lệch beta' (beta cao - beta thấp) theo trạng thái thị trường và hồi quy theo lợi suất
    thị trường; (2) sức mạnh tương đối có duy trì không (RS cao - RS thấp)."""
    if rel.empty:
        return {}
    r = rel.assign(beta_spread=rel["beta_high"] - rel["beta_low"], rs_spread=rel["rs_high"] - rel["rs_low"])
    by = r.groupby("regime").agg(so_thang=("market", "count"), thi_truong_tb=("market", "mean"),
                                 chenh_lech_beta_tb=("beta_spread", "mean"), chenh_lech_RS_tb=("rs_spread", "mean"))
    by = by.reindex(["Yếu", "Trung tính", "Mạnh"]).dropna(how="all")
    reg = newey_west_slope(r["beta_spread"].to_numpy(), r["market"].to_numpy(), lags=1)
    rs = r["rs_spread"].dropna()
    t_rs = rs.mean() / rs.std() * np.sqrt(len(rs)) if len(rs) > 2 and rs.std() > 0 else np.nan
    return {"by_regime": by, "beta_slope": reg, "rs_mean": float(rs.mean()), "rs_t": float(t_rs), "n": len(r)}


def summarize(bt: pd.DataFrame) -> pd.DataFrame:
    cols = {"Danh mục top (đã trừ phí)": "net", "Danh mục top (chưa phí)": "gross",
            "Mọi mã đủ điều kiện (cân bằng)": "universe", "VN-Index": "index"}
    return pd.DataFrame({k: perf_stats(bt[v]) for k, v in cols.items()}).T


def split_periods(bt: pd.DataFrame, cut: str = "2023-01-01") -> dict:
    """Thống kê theo hai giai đoạn liền nhau (trước/sau `cut`). Mô hình không có tham số nào được học từ dữ liệu nên
    không có 'tập huấn luyện'; chia giai đoạn chỉ để kiểm tra độ ổn định theo thời gian. Giai đoạn < MIN_MONTHS -> None."""
    out = {}
    for name, sub in ((f"Trước {cut[:4]}", bt[bt.index < cut]), (f"Từ {cut[:4]}", bt[bt.index >= cut])):
        out[name] = summarize(sub) if len(sub) >= MIN_MONTHS else None
    return out


def excess_by_regime(bt: pd.DataFrame) -> pd.DataFrame:
    """Lợi suất vượt 'mọi mã đủ điều kiện' của danh mục top (điểm %/tháng) theo trạng thái thị trường tại ngày tín hiệu."""
    x = bt.assign(excess=bt["net"] - bt["universe"])
    return x.groupby("regime")["excess"].agg(so_thang="count", vuot_tb_pp=lambda s: s.mean() * 100)


def cost_summary(bt: pd.DataFrame) -> dict:
    return {"so_lan_tai_can_bang": int(len(bt)), "vong_quay_tb": float(bt["turnover"].mean()), "phi_tb_thang": float(bt["cost"].mean()),
            "so_ma_giu_tb": float(bt["n_held"].mean()), "so_ma_xep_hang_tb": float(bt["n_ranked"].mean())}


def main(argv=None) -> int:
    from src import config
    from src.analytics import market_health as mh
    from src.analytics import panel as pn
    from src.data import db
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--top-frac", type=float, default=0.10)
    ap.add_argument("--cost-bps", type=float, default=30.0)
    ap.add_argument("--start", default="2019-06-01")
    a = ap.parse_args(argv)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    p = pn.load_panel(db.connect(config.DB_PATH))
    el = pn.eligible_mask(p.value, a.min_value)
    health = mh.market_health(p.index_close, p.index_value, p.close, el)
    f = ss.factor_table(p.close, p.value, p.index_close, el)

    print("=== 1. ĐIỂM THỊ TRƯỜNG: lợi suất VN-Index 21 phiên tới theo trạng thái (mẫu không chồng lấn) ===")
    r = regime_forward_returns(health, p.index_close, 21)
    print((r["table"] * [1, 100, 100, 100]).rename(columns={"mean": "TB %", "median": "Trung vị %", "pos": "% kỳ dương"}).round(2))
    b = r["regression"]
    print(f"Hồi quy lợi suất tương lai theo điểm: hệ số {b['beta']:.5f}/điểm, t(Newey-West) = {b['t']:.2f}, n = {b['n']} ngày")
    print(f"Lợi suất TB không điều kiện: {r['unconditional_mean'] * 100:.2f}%/21 phiên")

    print("\n=== 2. ĐIỂM CỔ PHIẾU: backtest top mỗi tháng ===")
    bt = stock_backtest(p.close, p.index_close, el, f, health, top_frac=a.top_frac, cost_bps=a.cost_bps, start=a.start)
    if bt.empty:
        print("Không đủ dữ liệu để backtest.")
        return 1
    for name, sub in (("Toàn kỳ", bt), ("2019-2022", bt[bt.index < "2023-01-01"]), ("2023-nay", bt[bt.index >= "2023-01-01"])):
        print(f"\n--- {name}: {len(sub)} tháng ---")
        if len(sub) < MIN_MONTHS:
            print(f"Không đủ dữ liệu (cần ≥ {MIN_MONTHS} tháng) để thống kê giai đoạn này.")
            continue
        s = summarize(sub)
        print((s[["ann_return", "ann_vol", "max_drawdown", "hit_rate"]] * 100).round(1).join(s["sharpe"].round(2)))
    print(f"\nVòng quay TB/tháng: {bt['turnover'].mean() * 100:.0f}% | phí TB/tháng: {bt['cost'].mean() * 100:.2f}% "
          f"| số mã giữ TB: {bt['n_held'].mean():.0f} / {bt['n_ranked'].mean():.0f} mã được xếp hạng")

    ic = information_coefficient(p.close, p.index_close, el, f, a.start)
    if not ic.empty:
        t = ic["ic"].mean() / ic["ic"].std() * np.sqrt(len(ic))
        print(f"\nIC (Spearman) TB = {ic['ic'].mean():.3f}, t = {t:.2f}, % tháng IC>0 = {(ic['ic'] > 0).mean() * 100:.0f}% (n = {len(ic)})")

    print("\n--- Theo trạng thái thị trường tại ngày tín hiệu: lợi suất vượt 'mọi mã' của danh mục top (điểm %/tháng) ---")
    bt = bt.assign(excess=bt["net"] - bt["universe"])
    print(bt.groupby("regime")["excess"].agg(thang="count", tb_pp=lambda s: s.mean() * 100).round(2))

    worst, cmp = extreme_monthly_returns(p.close, el, a.start)
    print("\n--- Kiểm tra giá trị cực đoan (lợi suất tháng của từng mã đủ điều kiện) ---")
    print(worst.assign(entry=worst["entry"].dt.date, exit=worst["exit"].dt.date, ret=(worst["ret"] * 100).round(1))
          .to_string(index=False))
    print({k: (round(v, 3) if isinstance(v, float) else v) for k, v in cmp.items()})

    rel = relation_analysis(p.close, p.index_close, el, f, health, a.start)
    sr = summarize_relation(rel)
    if sr:
        print("\n=== 3. QUAN HỆ THỊ TRƯỜNG - CỔ PHIẾU (RQ3) ===")
        print("Chênh lệch lợi suất tháng tới: nhóm beta CAO trừ nhóm beta THẤP, theo trạng thái thị trường tại ngày tín hiệu")
        print((sr["by_regime"] * [1, 100, 100, 100]).round(2).rename(columns={
            "so_thang": "số tháng", "thi_truong_tb": "VN-Index TB %", "chenh_lech_beta_tb": "beta cao - thấp %",
            "chenh_lech_RS_tb": "RS cao - thấp %"}))
        b = sr["beta_slope"]
        print(f"Hồi quy (beta cao - thấp) theo lợi suất VN-Index tháng đó: hệ số {b['beta']:.2f}, t = {b['t']:.2f} (n = {b['n']})")
        print(f"Sức mạnh tương đối duy trì? (RS cao - thấp) TB = {sr['rs_mean'] * 100:.2f}%/tháng, t = {sr['rs_t']:.2f}")

    print("\n--- Độ nhạy: top 10% theo ngưỡng thanh khoản và phí ---")
    rows = []
    for mv in (5e8, 1e9, 5e9):
        el2 = pn.eligible_mask(p.value, mv)
        f2 = ss.factor_table(p.close, p.value, p.index_close, el2)
        for cb in (a.cost_bps, 2 * a.cost_bps):
            b2 = stock_backtest(p.close, p.index_close, el2, f2, health, top_frac=a.top_frac, cost_bps=cb, start=a.start)
            if not b2.empty:
                s2 = perf_stats(b2["net"])
                rows.append({"ngưỡng GT (đồng)": f"{mv:,.0f}", "phí/chiều (bps)": cb, "LN năm %": s2["ann_return"] * 100,
                             "Sharpe": s2["sharpe"], "MDD %": s2["max_drawdown"] * 100})
    print(pd.DataFrame(rows).round(2).to_string(index=False))
    print("\nLưu ý: công cụ phân tích học thuật, không phải khuyến nghị đầu tư. Có thiên lệch người sống sót "
          "(mã đã hủy niêm yết không có trong dữ liệu) và chưa mô hình hóa trượt giá.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
