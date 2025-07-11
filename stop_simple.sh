#!/bin/bash

echo "🛑 Dừng DNSE Pipeline"
echo "===================="

# Dừng tất cả Python processes liên quan
pkill -f "transform_dnse.py"
pkill -f "dnse_filter.py" 
pkill -f "sink_snapshot.py"

# Xóa file PID
rm -f pipeline.pids

echo "✅ Pipeline đã dừng"
