# Khảo sát API SSI FastConnect Data (điền sau khi chạy scripts/api_survey.py)

Ngày khảo sát: 17/11/2026

## 1. Bảng đối chiếu endpoint (nhóm → dữ liệu → chức năng trong dự án)

| Endpoint | Nhóm | Trường dùng | Chức năng trong dự án | Tần suất gọi |
|---|---|---|---|---|
| IndexList | Danh mục/tham chiếu | IndexCode, IndexName | Xác định mã chỉ số (VN-Index) | 1 lần |
| Securities | Danh mục/tham chiếu | Symbol, Market | Universe mã HOSE | 1 lần/ngày |
| IndexComponents | Danh mục/tham chiếu | StockSymbol | Tham chiếu VN30 (không dùng suy ngược quá khứ) | 1 lần |
| DailyStockPrice | Dữ liệu thị trường (EOD) | Close, ClosePriceAdjusted, Ref/Ceiling/Floor, TotalMatchVal/Vol, Foreign* | Chỉ báo cổ phiếu, RS, backtest | Cập nhật tăng dần |
| DailyIndex | Dữ liệu thị trường (EOD) | IndexValue, Advances, Declines, Ceilings, Floors, TotalMatchVal | Market Health Score, đối chiếu độ rộng | Cập nhật tăng dần |
| Streaming (MI, X-QUOTE, X-TRADE) | WebSocket | theo kênh | Bộ ghi nội ngày, lưu `stream_raw` | Suốt phiên |

## 2. Cách đáp ứng điều kiện tối thiểu của đề

- [ ] Đường A: ≥ 2 nhóm API (Danh mục/tham chiếu + Dữ liệu thị trường)
- [ ] Đường B: ≥ 1 luồng WebSocket có lưu DB (bằng chứng: ảnh chụp kết quả `stream_probe.py` + bảng `stream_raw`)

## 3. Kết quả khảo sát thực tế (điền)

| Mục | Kết quả |
|---|---|
| Token hợp lệ trong bao lâu | |
| Giới hạn tần suất (thông báo quota) | |
| Số mã HOSE (Securities) | |
| Mã chỉ số VN-Index đúng | |
| DailyStockPrice: lịch sử xa nhất lấy được | |
| DailyStockPrice: khoảng ngày tối đa/lần gọi | |
| DailyStockPrice không truyền symbol: có trả toàn sàn? | |
| close vs closepriceadjusted: khác nhau thế nào | |
| Trường rỗng/0 bất thường | |
| Streaming: kênh dùng được, số thông điệp/phút | |
