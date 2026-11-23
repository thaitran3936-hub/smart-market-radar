# Smart Market Radar

Ra-đa sức khỏe thị trường chứng khoán Việt Nam và sàng lọc cổ phiếu, xây dựng trên **SSI FastConnect v3**
(đồ án cuối kỳ, học phần Gói phần mềm ứng dụng cho tài chính 1).

> **Công cụ phân tích học thuật, không phải khuyến nghị đầu tư.** Sản phẩm chỉ dùng dữ liệu thị trường,
> **không gửi lệnh giao dịch** và không đọc dữ liệu tài khoản.

*Ghi chú về nguồn dữ liệu:* đề nêu NEO API của VPBankS; đồ án dùng SSI FastConnect theo sự đồng ý của giảng viên
(điền ngày và hình thức xác nhận vào báo cáo).

## 1. Vấn đề và người dùng
Nhà đầu tư cá nhân thường chọn cổ phiếu mà không biết thị trường chung đang khỏe hay yếu. Sản phẩm trả lời hai câu hỏi:
(1) thị trường đang ở trạng thái nào (xu hướng, độ rộng, thanh khoản)? (2) trong bối cảnh đó, mã nào mạnh về xu hướng,
động lượng, thanh khoản và rủi ro? Phương pháp được kiểm chứng trên dữ liệu lịch sử 2018-2026 và báo cáo trung thực các hạn chế.

## 2. Kiến trúc
```
SSI REST (securitiesByBoard, securitiesSummary)  --> src/ssi_api/client.py  (giới hạn tần suất, thử lại, làm mới token)
                                                 --> src/data/collector.py + cleaning.py (chuẩn hóa, gắn cờ chất lượng)
SSI WebSocket (khớp lệnh, room khối ngoại)       --> scripts/stream_recorder.py ----+
                                                                                    v
                                                          SQLite  data/market.db (prices_daily, index_daily, symbols,
                                                                                  fetch_log, stream_raw)
                                                                                    v
        src/finance/indicators.py --> src/analytics/{panel,market_health,stock_score,backtest}.py
                                                                                    v
                                                   src/app/services.py --> src/app/app.py (Streamlit)
```

## 3. Cài đặt
Yêu cầu Python 3.11+.
```
python -m venv .venv
.venv\Scripts\activate            # Windows (Linux/macOS: source .venv/bin/activate)
python -m pip install -r requirements.txt   # giao diện + phân tích (gọn, chạy được trên Streamlit Cloud)
# pip install -r requirements-dev.txt     # thêm pytest
# pip install -r requirements-stream.txt  # thêm ssi-sdk cho bộ ghi streaming
copy .env.example .env             # rồi điền SSI_API_KEY và SSI_API_SECRET (lấy ở developers.ssi.com.vn)
```
**Không bao giờ commit `.env`, `config.json`, `token_cache.json` hoặc thư mục `data/`** (đã nằm trong `.gitignore`).
Key chỉ cần quyền dữ liệu thị trường; token không kèm OTP nên không có quyền giao dịch.

## 4. Chạy
| Việc | Lệnh |
|---|---|
| Tải 20 mã thử | `python -m src.data.collector --update --limit 20` |
| Tải toàn thị trường (HOSE, HNX, UPCoM; ~1 giờ) | `python -m src.data.collector --update --boards HOSE,HNX,UPCOM` |
| Cập nhật hằng ngày | `python -m src.data.collector --update --boards HOSE,HNX,UPCOM` (tăng dần; tự tải lại mã bị điều chỉnh giá) |
| **Cập nhật hằng ngày** (giá + indexSummary; ~1 giờ cho toàn thị trường) | `python scripts/daily_update.py` |
| Chỉ lưu ảnh chụp indexSummary hôm nay | `python -m src.data.collector --snapshot` |
| Gắn lại cờ chất lượng (không gọi API) | `python -m src.data.collector --reflag` |
| Báo cáo chất lượng dữ liệu | `python -m src.data.collector --report` |
| Điểm thị trường và top cổ phiếu (dòng lệnh) | `python -m src.analytics.run` |
| Kiểm chứng lịch sử | `python -m src.analytics.backtest` |
| **Giao diện** | `streamlit run src/app/app.py` |
| Kiểm thử | `python -m pytest -q` |

### Cập nhật tự động (dữ liệu luôn mới khi người dùng xem sau)
Giao diện chỉ **đọc** cơ sở dữ liệu; dữ liệu mới có khi script cập nhật chạy. Giao diện tự tính lại khi tệp DB đổi và
**cảnh báo nếu dữ liệu cũ hơn phiên giao dịch gần nhất** (chưa biết ngày nghỉ lễ nên có thể báo dư một vài phiên).
Chạy tự động mỗi ngày làm việc lúc 15:30 bằng Windows Task Scheduler (máy phải bật):
```
schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 15:30 /TN "SmartMarketRadar-Update" /TR "\"<đường dẫn dự án>\scripts\daily_update.bat\""
```
Nếu đường dẫn có dấu tiếng Việt khiến lệnh lỗi, tạo bằng giao diện: Task Scheduler > Create Basic Task > Daily > Start a program >
chọn `scripts\daily_update.bat`. Nhật ký ở `data/daily_update.log`; script thoát mã 1 nếu > 5% mã lỗi, mã 2 nếu lỗi chung (mất mạng, sai key).
`indexSummary` (số mã tăng/giảm/trần/sàn chính thức) SSI **không lưu lịch sử**, nên phải được lưu mỗi ngày từ lúc bắt đầu chạy.
Hướng phát triển: chạy bộ cập nhật trên máy chủ/dịch vụ lập lịch thay vì máy cá nhân.

### Ghi streaming (WebSocket)
`scripts/stream_recorder.py` dùng SDK chính thức `ssi-sdk` và các tệp `config.py`, `auth_helper.py` của repo mẫu
`ssi-fastconnect-v3-tutorials`. Đặt tệp cạnh `sample_10_websocket_data.py` trong thư mục `python/` của repo đó, điền
`config.json` theo repo đó, rồi chạy trong giờ giao dịch (9:00-14:45):
```
python python/stream_recorder.py --db "<đường dẫn>/data/market.db" --minutes 60
```
Lần đầu mỗi ngày cần duyệt Smart OTP trên ứng dụng SSI (chỉ để kết nối; bộ ghi chỉ **đăng ký nhận** dữ liệu thị trường).

## 5. Dữ liệu
Mọi dấu thời gian theo `Asia/Ho_Chi_Minh` (UTC+7). Từ điển dữ liệu chi tiết: `docs/api_survey.md`.

| Bảng | Nội dung |
|---|---|
| `prices_daily`, `index_daily` | giá mở/cao/thấp/đóng (điều chỉnh ngược), khối lượng/giá trị khớp, khối ngoại; `data_status` (ok, no_trade, suspect, missing) và `flags` |
| `symbols` | mã, sàn, loại, ngành |
| `index_summary_daily` | ảnh chụp indexSummary mỗi ngày: số mã tăng/giảm/đứng/trần/sàn, giá trị khớp, khối ngoại ròng |
| `fetch_log` | nhật ký mọi lệnh gọi API (không chứa key/token) |
| `stream_raw` | thông điệp WebSocket (`recv_time`, `msg_type`, `symbol`, `payload` JSON) |

`data_sample/` chứa mẫu dữ liệu giá một mã (2018-2021) để xem định dạng; dữ liệu đầy đủ do collector tải về.

## 6. Phương pháp (tóm tắt)
Chi tiết công thức, giả định và cơ sở tham khảo: tab "Phương pháp & giới hạn" của giao diện và báo cáo.
- **Điểm thị trường (0-100):** trung bình 3 trụ cột (xu hướng, độ rộng, thanh khoản), mỗi chỉ báo quy về phân vị chỉ dùng quá khứ.
- **Điểm cổ phiếu:** 4 nhóm (xu hướng, động lượng + sức mạnh tương đối, thanh khoản, rủi ro), trọng số mặc định bằng nhau.
- **Quan hệ thị trường - cổ phiếu (RQ3):** beta và tương quan 120 phiên so với VN-Index; sức mạnh tương đối; cổ phiếu hành xử thế nào
  khi thị trường Yếu/Trung tính/Mạnh (phân loại theo trạng thái phiên trước); kiểm chứng "nhóm beta cao − thấp" theo lợi suất thị trường.
- **Kiểm chứng:** tín hiệu tới hết phiên t, vào lệnh ở giá phiên t+1, tái cân bằng hằng tháng, có phí; Newey-West cho lợi suất chồng lấn.

## 7. Giới hạn đã biết
- Kết quả kiểm chứng cho tín hiệu **yếu**, chưa có ý nghĩa thống kê ở mức 5%; không phải khuyến nghị đầu tư.
- Thiên lệch người sống sót (mã đã hủy niêm yết không có trong dữ liệu); giá điều chỉnh ngược nên chứa thông tin về sau.
- Chưa mô hình hóa trượt giá, tác động thị trường và khả năng mua đủ khối lượng.
- Độ rộng lịch sử tự tính từ giá (SSI không có lịch sử số mã tăng/giảm chính thức); `indexSummary` chỉ trả phiên gần nhất.
- Dữ liệu SSI có lỗi cục bộ (sai đơn vị giá trị ở vài ngày, giá trùng): được gắn cờ chứ không xóa.
- Dữ liệu streaming chỉ ghi một số phiên; mã thanh khoản thấp bị loại khỏi xếp hạng.
- Bộ ghi streaming phụ thuộc repo mẫu của SSI và cần Smart OTP mỗi ngày.

## 8. Cấu trúc thư mục
```
src/ssi_api     client REST        src/data       collector, làm sạch, SQLite
src/finance     chỉ báo thuần túy  src/analytics  điểm thị trường, điểm cổ phiếu, kiểm chứng
src/app         dịch vụ + Streamlit   scripts     khảo sát API, ghi streaming, chẩn đoán dữ liệu
tests           kiểm thử tự động   docs           khảo sát API, mô tả bài toán
data_sample     mẫu dữ liệu        report         báo cáo
```

## Cập nhật tự động không cần bật máy (tùy chọn, hướng phát triển)

Cơ chế: GitHub Actions chạy `scripts/daily_update.py` lúc 15:30 (giờ VN) các ngày làm việc trên máy chủ của GitHub,
rồi đăng `market.db.gz` lên Release `data-latest`. Máy người xem chạy `python scripts/sync_db.py` để lấy bản mới nhất
(streaming/indexSummary đã ghi cục bộ được giữ lại).

Thiết lập một lần:
1. Tạo kho GitHub **PRIVATE**, đẩy mã nguồn lên (không đẩy `.env`, `config.json`, `token_cache.json`, `data/`).
2. Settings → Secrets and variables → Actions: thêm `SSI_API_KEY`, `SSI_API_SECRET`.
3. Tab Actions → daily-update → Run workflow (lần đầu tick "full"; ~1–2 giờ). Lần đầu chưa có DB thì tải lại từ 2018.
   Tốt nhất tải sẵn `data/market.db` đã có: `gzip -c data/market.db > market.db.gz` rồi `gh release create data-latest market.db.gz`.
4. Trên máy xem: thêm `GITHUB_REPO=chu/ten-kho` và `GITHUB_TOKEN` (fine-grained, chỉ đọc Contents) vào `.env`.

Lưu ý trung thực: chưa kiểm chứng SSI có cho phép IP của GitHub runner; hạn mức phút Actions của kho private miễn phí
(2.000 phút/tháng) – thứ 2–5 chỉ cập nhật mã còn giao dịch (`--active-only`), thứ 6 cập nhật toàn bộ.

### Dung lượng dữ liệu
Không xóa lịch sử giá: backtest và chỉ báo cần chuỗi dài. ~180 byte/dòng, thêm ~1.500 dòng/ngày (~0,06%/ngày).
Mỗi lần cập nhật ghi lại chuỗi của từng mã (vì giá điều chỉnh) nên tệp không phình thêm. Bảng tăng nhanh nhất là
`stream_raw`; dọn sau khi nộp bài: `python scripts/maintain_db.py --prune-stream-days 60 --vacuum`.


## Giao diện (Streamlit)
Năm tab, mỗi tab trả lời một câu hỏi của sản phẩm: **Thị trường** (mạnh/trung tính/yếu?), **Xếp hạng cổ phiếu** (cổ phiếu nào tốt, mạnh hơn VN-Index?),
**Realtime Monitor** (dữ liệu WebSocket đã lưu), **Kiểm chứng** (phương pháp có hiệu quả không?), **Phương pháp & giới hạn**.
Giao diện tối mặc định (`.streamlit/config.toml`); đổi sang sáng ở menu ⋮ → Settings. Điểm Rủi ro thấp: cao = rủi ro thấp (tốt).


## Đưa giao diện lên web (Streamlit Community Cloud) — người xem chỉ cần mở link
1. share.streamlit.io → đăng nhập bằng GitHub → New app → chọn kho `smart-market-radar`, nhánh `main`, tệp chính `src/app/app.py`.
2. Advanced settings → Secrets, dán (giá trị thật của bạn):
```
GITHUB_REPO = "chu-so-huu/smart-market-radar"
GITHUB_TOKEN = "token-chi-doc-contents"
```
3. Lần mở đầu ứng dụng tự tải `market.db.gz` từ Release `data-latest`. Sau đó, mỗi lần có người mở hoặc làm mới trang (tối đa mỗi 15 phút
   một lần kiểm tra), nếu Release có bản mới hơn thì tải về và tính lại. GitHub Actions cập nhật Release lúc 15:30 các ngày làm việc,
   nên người xem không phải chạy lệnh nào.
Giới hạn đã biết: Streamlit Cloud miễn phí cho tối đa khoảng 2,7 GB RAM (theo tài liệu tháng 2/2024, có thể đổi) và ứng dụng ngủ sau 12 giờ không có người
truy cập (mở lại là chạy tiếp). Ứng dụng chỉ nạp các mã có thể đủ thanh khoản để giảm bộ nhớ (đo trên dữ liệu mô phỏng: 400 mã ≈ 0,5 GB).
Dữ liệu hiển thị ở link công khai là chỉ báo tính từ dữ liệu SSI; hãy kiểm tra điều khoản của SSI trước khi công khai.
