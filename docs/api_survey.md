# Khảo sát API SSI FastConnect v3

Ngày khảo sát: 17/11/2026 (REST), nguồn: developers.ssi.com.vn và 3 lần chạy `scripts/api_survey.py`, `probe_dates.py`, `probe_history.py`.
Xác thực: `POST https://api.ssi.com.vn/api/v3/auth/token` (apiKey + apiSecret, không OTP, chỉ có quyền dữ liệu thị trường).

## 1. Bảng đối chiếu endpoint (nhóm, dữ liệu, chức năng trong dự án)

| Endpoint (GET, tiền tố /api/v3/data) | Nhóm | Trường dùng | Chức năng trong dự án | Tần suất gọi |
|---|---|---|---|---|
| securitiesByBoard?board=HOSE | Danh mục/tham chiếu | symbol, stockType, icbName, listedShare, firstTradingDate | Universe mã (lọc `stockType == "Stock"`), ngành | 1 lần/ngày |
| indexList | Danh mục/tham chiếu | index, indexName, board | Xác định mã chỉ số | 1 lần |
| masterdata | Danh mục/tham chiếu | ceiling, floor, refPrice (ngày hiện tại) | Tham chiếu biên độ, kiểm tra ngày hiện tại | 1 lần/ngày |
| securitiesSummary (symbol= hoặc index=) | Dữ liệu thị trường (EOD) | open, high, low, close (đã điều chỉnh), totalMatch, totalMatchValue, priceChangePercentage, totalForeignBuy/SellValue | Chuỗi giá cổ phiếu và VN-Index, thanh khoản, RS, backtest | Tải lại toàn bộ lịch sử mỗi mã khi cập nhật |
| indexSummary?index=VNINDEX | Dữ liệu thị trường (EOD) | indexValue, totalAdvance/Decline/NoChange/Ceiling/FloorStock, totalMatchValue, netPurchasesForeignValue | Độ rộng chính thức của phiên gần nhất, đối chiếu độ rộng tự tính | 1 lần/ngày, lưu lại mỗi ngày |
| ohlc?timeFrame=1d | Dữ liệu thị trường (nến) | open, high, low, close, volume, value | Nguồn đối chiếu chéo cho securitiesSummary | Khi cần |
| WebSocket wss://stream.ssi.com.vn/ws/v3 (ssi-sdk) | Streaming | TradeMessage (price, quantity, side, total_volume, open/high/low/avg), ForeignRoomMessage (room, mua/bán khối ngoại), IndexTickMessage | Bộ ghi nội ngày, lưu `stream_raw`; khung diễn biến trong phiên | Suốt phiên |

## 2. Cách đáp ứng điều kiện tối thiểu của đề

- [ ] Đường A: ≥ 2 nhóm API. Có nhóm Danh mục/tham chiếu và nhóm Dữ liệu thị trường, nhưng mọi endpoint v3 đều nằm dưới `/data/`, nên cách phân nhóm có thể bị coi là một. Chưa chắc chắn.
- [x] Đường B: ≥ 1 luồng WebSocket có lưu DB. **Đã đạt (18/11/2026, 10:28 đến 10:33):** 1.714 dòng vào `stream_raw` (TradeMessage 1.502, ForeignRoomMessage 144, IndexTickMessage 60, Control 8), 19 chủ đề subscribe thành công. Bằng chứng: ảnh chụp khối "BẰNG CHỨNG STREAMING ĐÃ LƯU DB" do `scripts/stream_recorder.py` in ra.

## 3. Kết quả khảo sát thực tế

| Mục | Kết quả |
|---|---|
| Rate limit | Header `X-RATELIMIT-LIMIT = 20`, `REMAINING = 19` sau mỗi lệnh; chu kỳ chưa rõ (header RESET không có). 40 lệnh liên tiếp (mỗi lệnh 0,6 đến 2 giây) không bị 429. Collector giữ giãn cách ≥ 0,5 giây và tự lùi khi gặp 429 |
| Hạn token | Chưa đo. Collector tự lấy lại token khi gặp 401 |
| Định dạng ngày | `securitiesSummary`: `yyyy/mm/dd`. `ohlc`: `yyyy/mm/dd HH:MM:SS`. `indexSummary`: bỏ qua from/to |
| securitiesByBoard HOSE | 926 dòng gồm cổ phiếu, chứng quyền (CW) và loại khác. Phải lọc `stockType == "Stock"`. HNX 421 dòng gồm cả trái phiếu |
| Số chỉ số / mã VN-Index | indexList có 36 chỉ số. `index=VNINDEX` dùng được với securitiesSummary |
| pageSize tối đa | 1000 được chấp nhận. Phản hồi có `itemsCount`, **không có `pageCount`** |
| Lịch sử | Có từ ít nhất 02/01/2018: SSI 2213 phiên, VNINDEX 2210 phiên (tính đến 17/11/2026). Chưa thử xa hơn 2018 |
| Thứ tự và phân trang | Dữ liệu trả về **không theo thứ tự ngày**. Trang 1 (1000 dòng) của VNINDEX trải từ 2018 đến 04/2026, nên không thể dựa vào thứ tự trang. Cách xử lý: chia cửa sổ ≤ 3 năm để mỗi lần gọi 1 trang, kiểm `len(data) == itemsCount`, tự sắp xếp và bỏ trùng theo `tradingDate` |
| Một lệnh lấy cả thị trường? | Không. `index=` chỉ trả chính chỉ số, `board=` bị từ chối, `symbol` và `index` không đi cùng nhau. Tải theo từng mã |
| Giá điều chỉnh | **Có.** `close/open/high/low` được điều chỉnh ngược. Tỷ lệ `close / (totalMatchValue / totalMatch)` có trung vị 0,245 (2018), 0,252 (2019), 0,308 (2020), 0,331 (2021), khoảng 0,80 (05/2026), 1,00 (10/2026). `totalMatch` và `totalMatchValue` **không** điều chỉnh |
| Đơn vị | Giá tính bằng đồng (không phải nghìn đồng); `totalMatchValue` tính bằng đồng |
| Tính nhất quán biên độ | 1000 phiên SSI 2018 đến 2021: không có phiên nào lợi suất từ `close` vượt ±7,1% (đúng biên độ HOSE), tức chuỗi điều chỉnh liền mạch |
| Trường rỗng | `average`, `totalDeal`, `totalDealValue`, `openInterest`, `settlementPrice` rỗng toàn bộ; `priceChange` rỗng 505/1000 dòng cũ. Không dùng các trường này. Dùng `priceChangePercentage` hoặc lợi suất từ `close` |
| Lỗi dữ liệu đã gặp | (1) 13/09/2018 và 25/04/2019: `totalMatchValue` nhỏ hơn thực tế khoảng 1 triệu lần (sai đơn vị, giá trị thật tính bằng triệu đồng). (2) 23/01/2018: `totalMatch = 0` nhưng `totalMatchValue` = 349,9 tỷ. (3) 20/06/2019: `close` và `totalMatch` trùng hệt ngày 19/06/2019 (bản sao cũ). (4) VNINDEX có 1 ngày trùng và 2 phiên khối lượng 0. Collector gắn cờ `data_status`, không xóa; chỉ báo thanh khoản dùng trung vị để không bị ảnh hưởng |
| indexSummary | Luôn trả đúng 1 dòng (phiên gần nhất) dù xin ngày nào. Tham số phải là `index=`. Có đủ số mã tăng/giảm/đứng/trần/sàn. `totalBuyForeignValue`/`totalSellForeignValue` bằng 0, chỉ dùng `netPurchasesForeignValue`. **Không có lịch sử**, nên phải lưu lại mỗi ngày từ 17/11/2026; độ rộng lịch sử phải tự tính từ giá |
| ohlc | Chạy với ngày dạng `yyyy/mm/dd HH:MM:SS`. Trường: open, high, low, close, volume, value, symbol, tradingDate. Chưa kiểm tra có điều chỉnh không |
| masterdata | Chỉ trả ceiling/floor/refPrice của ngày hiện tại, 2120 mã, 10 dòng/trang mặc định |
| Streaming | Kết nối bằng `ssi-sdk`, cần duyệt Smart OTP trên app trong 30 giây (token lưu `token_cache.json`, gitignore). Chỉ subscribe dữ liệu thị trường. `QuoteMessage` (bid/ask) chiếm khoảng 70% thông điệp, bỏ qua mặc định. `IndexTickMessage.value` (≈141,7 triệu) **không phải điểm chỉ số** (giống tổng khối lượng), `change`/`advances`/`declines` rỗng: VN-Index và độ rộng lấy từ REST, không lấy từ tick chỉ số |
