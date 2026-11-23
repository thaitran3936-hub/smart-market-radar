import numpy as np
import pandas as pd

from src.analytics import panel as pn
from src.data import cleaning, db


def test_loc_ma_kem_thanh_khoan_khong_doi_ket_qua_ma_du_dieu_kien(tmp_path):
    con = db.connect(str(tmp_path / "m.db"))
    db.init_db(con)
    idx = pd.bdate_range("2019-01-02", periods=300)
    rng = np.random.default_rng(0)

    def put(sym, level):
        c = 100 * np.cumprod(1 + rng.normal(0, 0.01, len(idx)))
        vol = level / c
        df = pd.DataFrame({"tradingDate": idx.strftime("%Y/%m/%d"), "open": c, "high": c, "low": c, "close": c,
                           "totalMatch": vol, "totalMatchValue": vol * c, "priceChangePercentage": 0.0})
        db.replace_prices(con, sym, cleaning.add_quality_flags(cleaning.normalize_summary(df), check_vwap=False, check_ret=False), "t")
    put("LIQ", 5e9)
    put("ILQ", 1e6)       # không bao giờ đạt ngưỡng 1 tỷ
    ix = pd.DataFrame({"tradingDate": idx.strftime("%Y/%m/%d"), "open": 1, "high": 1, "low": 1, "close": 1000.0,
                       "totalMatch": 1e8, "totalMatchValue": 1e12, "priceChangePercentage": 0.0})
    db.replace_index(con, "VNINDEX", cleaning.add_quality_flags(cleaning.normalize_summary(ix), check_vwap=False, check_ret=False), "t")
    con.commit()
    full = pn.load_panel(con)
    slim = pn.load_panel(con, min_peak_value=1e9)
    assert set(full.close.columns) == {"LIQ", "ILQ"} and set(slim.close.columns) == {"LIQ"}
    e_full = pn.eligible_mask(full.value, 1e9)
    e_slim = pn.eligible_mask(slim.value, 1e9)
    assert not e_full["ILQ"].any()                       # mã bị loại thật sự không bao giờ đủ điều kiện
    pd.testing.assert_series_equal(e_full["LIQ"], e_slim["LIQ"])
