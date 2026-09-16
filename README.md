# Đảm bảo thứ tự sự kiện với Kafka Partition Key

## Giới thiệu bài tập
Bài tập này giải quyết vấn đề "Tính nhất quán cuối" trong kiến trúc hướng sự kiện (Event-Driven Architecture) khi sử dụng Apache Kafka. Cụ thể là đảm bảo các sự kiện của cùng một đơn hàng (`CREATED`, `PAID`, `SHIPPED`) luôn được xử lý đúng thứ tự dù topic có nhiều partition.

## Chức năng đã làm
1. **Phân tích vấn đề**: Giải thích lý do mất thứ tự khi dùng nhiều partition.
2. **Giải pháp Partition Key**: Sử dụng `orderId` làm khóa phân vùng để ép các sự kiện cùng đơn hàng vào chung 1 partition.
3. **Mã nguồn Producer (Python)**: Gửi sự kiện kèm theo khóa phân vùng sử dụng thư viện `kafka-python`.
4. **Mã nguồn Consumer (Python)**: Đọc sự kiện từ partition và xử lý theo thứ tự.
5. **Giải thích tối ưu**: Số lượng Consumer tối đa hiệu quả bằng số lượng partition (5).

## Hướng dẫn chạy chương trình
1. Đảm bảo Kafka broker đang chạy tại `localhost:9092`.
2. Cài đặt thư viện: `pip install kafka-python`
3. Chạy producer: `python producer.py`
4. Chạy consumer: `python consumer.py`