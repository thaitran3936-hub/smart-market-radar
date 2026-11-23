"""Kiểm thử pipeline dữ liệu bằng phản hồi SSI giả (không gọi mạng)."""
import sqlite3
from datetime import date

import pandas as pd
import pytest

from src.data import cleaning, collector, db
from src.ssi_api.client import SSIClient, SSIError, split_range


class FakeResp:
    def __init__(self, status=200, body=None, headers=None, text=""):
        self.status_code, self._body, self.headers, self.text = status, body, headers or {}, text

    def json(self):
        return self._body


class FakeSession:
    """Trả lần lượt các phản hồi đã định sẵn; ghi lại các lệnh GET."""
    def __init__(self, responses):
        self.responses, self.gets = list(responses), []

    def post(self, url, **kw):
        return FakeResp(200, {"accessToken": "t", "expiresAt": 9e12})

    def get(self, url, **kw):
        self.gets.append((url, kw.get("params")))
        return self.responses.pop(0)


def row(d, close, vol=1000, val=None, pct=None):
    return {"tradingDate": d, "open": close, "high": close, "low": close, "close": close,
            "totalMatch": vol, "totalMatchValue": val if val is not None else vol * close,
            "priceChangePercentage": pct}


def client(responses):
    return SSIClient(api_key="k", api_secret="s", base="http://x", min_interval=0,
                     session=FakeSession(responses), sleep=lambda s: None)


def test_split_range_khong_chong_lan_va_du_ngay():
    parts = split_range(date(2018, 1, 1), date(2026, 11, 17))
    assert parts[0][0] == date(2018, 1, 1) and parts[-1][1] == date(2026, 11, 17)
    for (_, e), (s, _) in zip(parts, parts[1:]):
        assert (s - e).days == 1
    assert all((e - s).days < 1095 for s, e in parts)


def test_normalize_sap_xep_va_bo_trung():
    raw = pd.DataFrame([row("2020/01/03", 12), row("2020/01/02", 10),
                        row("2020/01/02", 10, vol=5, val=50)])
    df = cleaning.normalize_summary(raw)
    assert list(df["trade_date"].dt.strftime("%Y-%m-%d")) == ["2020-01-02", "2020-01-03"]
    assert df.loc[0, "match_vol"] == 1000  # giữ dòng có giá trị khớp lớn hơn


def test_flag_khoi_luong_0_nhung_co_gia_tri_la_suspect_khong_xoa():
    raw = pd.DataFrame([row("2020/01/02", 10), row("2020/01/03", 10, vol=0, val=3.5e11),
                        row("2020/01/06", 10, vol=0, val=0), row("2020/01/07", 10)])
    out = cleaning.add_quality_flags(cleaning.normalize_summary(raw))
    assert len(out) == 4  # không xóa dòng nào
    st = dict(zip(out["trade_date"].dt.day, out["data_status"]))
    assert st[3] == "suspect" and st[6] == "no_trade" and st[2] == "ok"


def test_summary_chia_doi_khoang_khi_itemsCount_lon_hon_so_dong():
    c = client([
        FakeResp(200, {"data": [row("2020/01/02", 10)], "itemsCount": 2}),   # thiếu -> chia đôi
        FakeResp(200, {"data": [row("2020/01/02", 10)], "itemsCount": 1}),
        FakeResp(200, {"data": [row("2020/09/01", 11)], "itemsCount": 1}),
    ])
    df = c.summary(symbol="SSI", start=date(2020, 1, 1), end=date(2020, 12, 31))
    assert len(df) == 2 and len(c.session.gets) == 3


def test_429_cho_roi_thu_lai_va_loi_4xx_khac_bao_loi():
    ok = FakeResp(200, {"data": [], "itemsCount": 0})
    c = client([FakeResp(429, headers={"X-RATELIMIT-RESET": "2"}, text="slow"), ok])
    assert c.get("/p") == {"data": [], "itemsCount": 0}
    assert [x["http"] for x in c.calls] == [429, 200]
    with pytest.raises(SSIError):
        client([FakeResp(400, text="bad")]).get("/p")


def test_401_lay_lai_token():
    c = client([FakeResp(401, text="expired"), FakeResp(200, {"data": []})])
    assert c.get("/p") == {"data": []}


def test_ghi_db_thay_toan_bo_va_dung_cot_trang_thai(tmp_path):
    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)
    raw = cleaning.normalize_summary(pd.DataFrame([row("2020/01/02", 10), row("2020/01/03", 11)]))
    flagged = cleaning.add_quality_flags(raw)
    assert db.replace_prices(con, "SSI", flagged, "2026-11-18T10:00:00+07:00") == 2
    db.replace_prices(con, "SSI", flagged.iloc[:1], "2026-11-18T11:00:00+07:00")
    assert con.execute("SELECT COUNT(*) FROM prices_daily").fetchone()[0] == 1
    out = db.load_prices(con, ["SSI"])
    assert out["close"].iloc[0] == 10 and out["data_status"].iloc[0] == "ok"


def test_phat_hien_dieu_chinh_nguoc():
    old = pd.DataFrame({"trade_date": pd.to_datetime(["2020-01-02"]), "close": [10.0]})
    same = old.copy()
    changed = pd.DataFrame({"trade_date": pd.to_datetime(["2020-01-02"]), "close": [8.0]})
    assert not collector._adjusted(old, same) and collector._adjusted(old, changed)


def test_effective_end_truoc_va_sau_15h():
    from datetime import datetime
    assert collector.effective_end(datetime(2026, 11, 18, 10, 0)) == date(2026, 11, 17)
    assert collector.effective_end(datetime(2026, 11, 18, 15, 5)) == date(2026, 11, 18)


def test_bang_stream_raw_cu_duoc_doi_ten_khong_mat_du_lieu(tmp_path):
    p = str(tmp_path / "m.db")
    raw = sqlite3.connect(p)
    raw.execute("CREATE TABLE stream_raw(id INTEGER PRIMARY KEY, recv_time TEXT, channel TEXT, payload TEXT)")
    raw.execute("INSERT INTO stream_raw(recv_time, channel, payload) VALUES ('t','c','{}')")
    raw.commit()
    raw.close()
    con = db.connect(p)
    db.init_db(con)
    names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert any(n.startswith("stream_raw_legacy_") for n in names)
    assert {r[1] for r in con.execute("PRAGMA table_info(stream_raw)")} >= {"msg_type", "symbol"}


def test_run_update_nhieu_san_gop_ma_va_chi_so(tmp_path):
    import pandas as pd
    body = {"data": [row("2020/01/02", 10)], "itemsCount": 1}

    class C:
        calls: list = []

        def securities_by_board(self, b):
            return pd.DataFrame({"symbol": [b + "1", b + "W"], "stockType": ["Stock", "CW"]})

        def summary(self, **kw):
            return pd.DataFrame([row("2020/01/02", 10)])

        def drain_calls(self):
            return []

    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)
    r = collector.run_update(con, C(), boards=["HOSE", "HNX", "UPCOM"], end=date(2020, 1, 3))
    got = {x[0] for x in con.execute("SELECT symbol FROM prices_daily")}
    idx = {x[0] for x in con.execute("SELECT index_code FROM index_daily")}
    assert got == {"HOSE1", "HNX1", "UPCOM1"}  # bỏ chứng quyền (CW)
    assert idx == {"VNINDEX", "VN30", "HNXINDEX", "HNX30", "UPCOMINDEX"} and not r["failed"]


def test_lay_token_thu_lai_khi_rot_mang():
    import requests

    class S(FakeSession):
        n = 0

        def post(self, url, **kw):
            S.n += 1
            if S.n < 3:
                raise requests.ConnectionError("reset")
            return FakeResp(200, {"accessToken": "t", "expiresAt": 9e12})

    c = SSIClient(api_key="k", api_secret="s", base="http://x", min_interval=0,
                  session=S([FakeResp(200, {"data": []})]), sleep=lambda s: None)
    assert c.get("/p") == {"data": []} and S.n == 3


def test_ret_mismatch_nho_chi_la_canh_bao_lon_moi_la_nghi_loi():
    # close tăng 3% nhưng price_change_pct = 0 -> lệch 3 điểm: cảnh báo mềm, vẫn "ok"
    raw = pd.DataFrame([row("2020/01/02", 100, pct=0.0), row("2020/01/03", 103, pct=0.0),
                        row("2020/01/06", 120, pct=0.0)])   # 103 -> 120: lệch ~16,5 điểm
    out = cleaning.add_quality_flags(cleaning.normalize_summary(raw))
    st = dict(zip(out["trade_date"].dt.day, out["data_status"]))
    assert st[3] == "ok" and "ret_mismatch" in out.loc[1, "flags"]
    assert st[6] == "suspect" and "ret_mismatch_large" in out.loc[2, "flags"]


def test_chi_so_bo_qua_kiem_tra_loi_suat_vi_price_change_pct_la_phan_so():
    raw = pd.DataFrame([row("2020/01/02", 100, pct=0.0), row("2020/01/03", 90, pct=-0.1)])
    out = cleaning.add_quality_flags(cleaning.normalize_summary(raw), check_vwap=False, check_ret=False)
    assert set(out["data_status"]) == {"ok"}


def test_reflag_ap_dung_quy_tac_moi_len_du_lieu_da_luu(tmp_path):
    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)
    raw = cleaning.normalize_summary(pd.DataFrame([row("2020/01/02", 100, pct=0.0),
                                                   row("2020/01/03", 90, pct=-0.1)]))
    old = cleaning.add_quality_flags(raw, check_ret=True)  # cách cũ: đánh dấu sai chỉ số
    db.replace_index(con, "VNINDEX", old, "t")
    assert con.execute("SELECT COUNT(*) FROM index_daily WHERE data_status='suspect'").fetchone()[0] == 1
    collector.reflag(con)
    assert con.execute("SELECT COUNT(*) FROM index_daily WHERE data_status='suspect'").fetchone()[0] == 0
