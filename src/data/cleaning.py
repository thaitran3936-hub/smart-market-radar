"""Chuẩn hóa và gắn cờ chất lượng dữ liệu securitiesSummary.

Nguyên tắc: KHÔNG xóa dòng chỉ vì nghi ngờ. Mỗi dòng có `data_status` và `flags`,
lớp tính toán phía sau quyết định dùng hay bỏ.

Phát hiện từ khảo sát (xem docs/api_survey.md):
- open/high/low/close/average là giá ĐÃ ĐIỀU CHỈNH ngược; match_val, match_vol, deal_* là số gốc.
  Vì vậy close / (match_val / match_vol) không bằng 1 mà là hệ số điều chỉnh (đổi theo thời gian).
- price_change_pct tính trên giá gốc (làm tròn 1 chữ số); price_change có thể trống ở dữ liệu cũ.
- Thứ tự dòng trả về không theo ngày, có thể có ngày trùng.

Phân loại giá trị 0 (không tự xóa):
- khối lượng = 0 và giá trị = 0      -> no_trade (không giao dịch)
- khối lượng = 0 nhưng giá trị > 0   -> suspect (mâu thuẫn, lỗi dữ liệu)
- giá thiếu                          -> missing
Lưu ý: vwap_outlier so với trung vị của 21 phiên quanh ngày đó (cửa sổ căn giữa). Đây là kiểm toán chất
lượng dữ liệu, không dùng lợi suất hay kết quả đầu tư; cần nêu rõ trong báo cáo.
"""
import numpy as np
import pandas as pd

COLUMN_MAP = {
    "tradingDate": "trade_date", "open": "open", "high": "high", "low": "low", "close": "close",
    "average": "average", "priceChange": "price_change", "priceChangePercentage": "price_change_pct",
    "totalMatch": "match_vol", "totalMatchValue": "match_val",
    "totalDeal": "deal_vol", "totalDealValue": "deal_val",
    "totalForeignBuy": "foreign_buy_vol", "totalForeignBuyValue": "foreign_buy_val",
    "totalForeignSell": "foreign_sell_vol", "totalForeignSellValue": "foreign_sell_val",
    "remainForeignRoom": "foreign_room_remain", "totalForeignRoom": "foreign_room_total",
}
VALUE_COLS = [c for c in COLUMN_MAP.values() if c != "trade_date"]
FLAG_ORDER = ["missing_price", "bad_price", "zero_volume", "vol_val_conflict", "vwap_outlier",
              "ret_mismatch", "ret_mismatch_large", "jump", "dup_resolved"]


def normalize_summary(raw: pd.DataFrame) -> pd.DataFrame:
    """Đổi tên cột, ép kiểu số, đổi ngày yyyy/mm/dd, sắp xếp tăng dần, xử lý ngày trùng."""
    cols = ["trade_date"] + VALUE_COLS
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols + ["_dup"])
    df = raw.rename(columns=COLUMN_MAP)
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    df = df[cols].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y/%m/%d", errors="coerce")
    df = df.dropna(subset=["trade_date"])
    for c in VALUE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    df["_dup"] = df.duplicated("trade_date", keep=False)
    # Ngày trùng: giữ dòng có giá trị khớp lớn nhất
    df = df.sort_values(["trade_date", "match_val"], na_position="first")
    df = df.drop_duplicates("trade_date", keep="last")
    return df.sort_values("trade_date").reset_index(drop=True)


def add_quality_flags(df: pd.DataFrame, jump_pct: float = 15.0, mismatch_pp: float = 1.0,
                      large_mismatch_pp: float = 5.0, vwap_factor: float = 5.0,
                      check_vwap: bool = True, check_ret: bool = True) -> pd.DataFrame:
    """Thêm cột flags (chuỗi nối bằng |) và data_status: ok | no_trade | suspect | missing.

    Kết quả chẩn đoán trên dữ liệu thật (18/11/2026) cho thấy:
    - Chỉ số: price_change_pct có lúc là phân số (-0,010 thay vì -1,0 %), không so được với lợi suất
      từ close, nên chỉ số bỏ qua kiểm tra này (check_ret=False).
    - Cổ phiếu: lệch 1-5 điểm % rất phổ biến (làm tròn, điều chỉnh ngược) nên chỉ là cảnh báo mềm
      (ret_mismatch). Chỉ lệch từ large_mismatch_pp (5 điểm %) trở lên mới tính là nghi lỗi.
      Ngưỡng 5 là quy ước kiểm toán, báo cáo cần nêu và thử độ nhạy."""
    df = df.copy()
    if df.empty:
        df["flags"] = pd.Series(dtype=str)
        df["data_status"] = pd.Series(dtype=str)
        return df
    close = df["close"]
    vol = df["match_vol"]
    ret = (close / close.shift(1) - 1.0) * 100.0  # lợi suất từ chính chuỗi close (đã điều chỉnh)

    vwap = df["match_val"] / vol.where(vol > 0)
    ratio = close / vwap  # ≈ hệ số điều chỉnh; so với trung vị cục bộ để bắt giá trị lệch hẳn
    med = ratio.rolling(21, min_periods=5, center=True).median()

    masks = {
        "missing_price": close.isna(),
        "bad_price": (close <= 0) | (df["high"] < df["low"]),
        "zero_volume": vol == 0,
        # Khối lượng = 0 nhưng giá trị khớp > 0 là mâu thuẫn (gặp thật: SSI 23/01/2018, 350 tỷ đồng)
        # nên là lỗi dữ liệu, không phải "không giao dịch".
        "vol_val_conflict": (vol == 0) & (df["match_val"] > 0),
        # Với chỉ số, giá trị/khối lượng không phải giá nên bỏ qua kiểm tra này (check_vwap=False)
        "vwap_outlier": ((ratio > vwap_factor * med) | (ratio < med / vwap_factor)) if check_vwap
        else pd.Series(False, index=df.index),
        "ret_mismatch": ((ret - df["price_change_pct"]).abs() > mismatch_pp) if check_ret
        else pd.Series(False, index=df.index),
        "ret_mismatch_large": ((ret - df["price_change_pct"]).abs() > large_mismatch_pp) if check_ret
        else pd.Series(False, index=df.index),
        "jump": ret.abs() > jump_pct,  # chỉ là cảnh báo, không loại
        "dup_resolved": df["_dup"].fillna(False).astype(bool) if "_dup" in df else pd.Series(False, index=df.index),
    }
    masks = {k: v.fillna(False).astype(bool) for k, v in masks.items()}

    suspect = (masks["bad_price"] | masks["vol_val_conflict"] | masks["vwap_outlier"]
               | masks["ret_mismatch_large"])
    status = np.where(masks["missing_price"], "missing",
                      np.where(suspect, "suspect",
                               np.where(masks["zero_volume"], "no_trade", "ok")))
    df["data_status"] = status
    # Nối tên cờ bằng phép toán theo cột (nhanh hơn nhiều so với duyệt từng dòng)
    flags = pd.Series("", index=df.index)
    for k in FLAG_ORDER:
        flags = flags + np.where(masks[k].to_numpy(), k + "|", "")
    df["flags"] = flags.str.rstrip("|")
    return df
