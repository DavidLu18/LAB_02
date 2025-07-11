#!/bin/bash

echo "🚀 Khởi động DNSE Pipeline đơn giản"
echo "=================================="

# Kiểm tra file cần thiết
if [ ! -f "messages.json" ]; then
    echo "❌ Không tìm thấy messages.json"
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "❌ Không tìm thấy .env"
    exit 1
fi

echo "✅ Files OK"

# Khởi động Pipeline 3 (Sink) trước
echo "🔄 Khởi động Pipeline 3 (Sink to PostgreSQL)..."
python3 sink_snapshot.py &
SINK_PID=$!
echo "✅ Sink started (PID: $SINK_PID)"
sleep 3

# Khởi động Pipeline 2 (Transform)
echo "🔄 Khởi động Pipeline 2 (Transform)..."
python3 dnse_filter.py &
FILTER_PID=$!
echo "✅ Transform started (PID: $FILTER_PID)"
sleep 3

# Khởi động Pipeline 1 (Source)
echo "🔄 Khởi động Pipeline 1 (Source)..."
python3 transform_dnse.py &
SOURCE_PID=$!
echo "✅ Source started (PID: $SOURCE_PID)"

echo ""
echo "🎉 Tất cả pipeline đã khởi động!"
echo "📊 Kiểm tra Redpanda UI: http://localhost:8080"
echo "🔍 Xem logs:"
echo "   - tail -f sink_snapshot.log"
echo "   - tail -f dnse_filter.log"
echo "   - tail -f transform_dnse.log"
echo ""
echo "🛑 Dừng pipeline: pkill -f 'python3.*dnse'"

# Lưu PID để dừng sau
echo "$SINK_PID $FILTER_PID $SOURCE_PID" > pipeline.pids

echo "⏳ Pipeline đang chạy... (Ctrl+C để dừng)"
wait
