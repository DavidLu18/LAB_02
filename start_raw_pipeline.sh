#!/bin/bash

echo "🚀 Khởi động RAW Sink Pipeline (Write-Only)"
echo "============================================="

# Kiểm tra files
if [ ! -f ".env" ]; then
    echo "❌ Không tìm thấy .env"
    exit 1
fi

echo "✅ Files OK"

# Khởi động RAW Sink Pipeline
echo "🔄 Khởi động RAW Sink Pipeline (data_raw schema)..."
python3 sink_snapshot_raw.py &
RAW_SINK_PID=$!
echo "✅ RAW Sink started (PID: $RAW_SINK_PID)"

echo ""
echo "🎉 RAW Pipeline đã khởi động!"
echo "📝 Mode: WRITE-ONLY (no duplicates handling)"
echo "📊 Schema: data_raw"
echo "🔍 Xem logs: tail -f sink_snapshot_raw.log"
echo ""
echo "🛑 Dừng pipeline: kill $RAW_SINK_PID"

# Lưu PID
echo "$RAW_SINK_PID" > raw_pipeline.pid

echo "⏳ RAW Pipeline đang chạy... (Ctrl+C để dừng)"
wait
