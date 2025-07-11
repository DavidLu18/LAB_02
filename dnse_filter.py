import json
import os
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from kafka import KafkaConsumer, KafkaProducer
import time
from dotenv import load_dotenv

load_dotenv()

STOCK_OHLC_PATTERN = "plaintext/quotes/krx/mdds/v2/ohlc"
STOCK_TOP_PRICE_PATTERN = "plaintext/quotes/krx/mdds/topprice"
STOCK_TICK_PATTERN = "plaintext/quotes/krx/mdds/tick"
STOCK_INFO_PATTERN = "plaintext/quotes/krx/mdds/stockinfo"
STOCK_MARKET_INDEX_PATTERN = "plaintext/quotes/krx/mdds/index"
STOCK_BOARD_EVENT_PATTERN = "plaintext/quotes/krx/mdds/boardevent"


def calculate_message_checksum(message: Dict[str, Any], algorithm: str = "sha256") -> str:
    try:
        message_copy = {k: v for k, v in message.items() if k != 'checksum'}
        message_str = json.dumps(message_copy, sort_keys=True, default=str)
        
        if algorithm.lower() == "md5":
            hash_obj = hashlib.md5()
        elif algorithm.lower() == "sha1":
            hash_obj = hashlib.sha1()
        else:
            hash_obj = hashlib.sha256()
        
        hash_obj.update(message_str.encode('utf-8'))
        return hash_obj.hexdigest()
        
    except Exception as e:
        print(f"Error calculating checksum: {e}")
        return ""


def verify_message_checksum(message: Dict[str, Any], algorithm: str = "sha256") -> bool:
    stored_checksum = message.get('checksum', '')
    if not stored_checksum:
        return False
    
    calculated_checksum = calculate_message_checksum(message, algorithm)
    return stored_checksum == calculated_checksum


def get_dnse_time_i_to_i(t) -> int:
    return int(t)


def get_dnse_time_s_to_i(t: str) -> int:
    return int(datetime.fromisoformat(t.replace('Z', '+00:00')).timestamp())


def transform_ohlcv(message_h: Dict[str, Any]) -> List[Dict[str, Any]]:
    resolution = message_h.get('resolution', 'MIN')
    if resolution is None:
        resolution = 'MIN'
    
    return [{
        "time": get_dnse_time_i_to_i(message_h['time']),
        "symbol": str(message_h["symbol"]),
        "resolution": str(resolution),
        "open": float(message_h["open"]),
        "high": float(message_h["high"]),
        "low": float(message_h["low"]),
        "close": float(message_h["close"]),
        "volume": float(message_h["volume"]),
        "updated": get_dnse_time_i_to_i(message_h["lastUpdated"]),
        "data_type": "OH",
        "source": "dnse"
    }]


def transform_top_price(message_h: Dict[str, Any]) -> List[Dict[str, Any]]:
    symbol = str(message_h["symbol"])
    bid_ask_size = 10 if symbol.startswith("VN30F") else 3
    
    if message_h.get("bid") is None:
        bp = []
        bq = []
    else:
        bp = []
        bq = []
        for i in range(bid_ask_size):
            try:
                if i < len(message_h["bid"]) and message_h["bid"][i] is not None:
                    price = message_h["bid"][i].get("price")
                    qty = message_h["bid"][i].get("qtty")
                    if price is not None:
                        bp.append(float(price))
                    if qty is not None:
                        bq.append(float(qty))
            except (IndexError, KeyError, TypeError):
                continue

    if message_h.get("offer") is None:
        ap = []
        aq = []
    else:
        ap = []
        aq = []
        for i in range(bid_ask_size):
            try:
                if i < len(message_h["offer"]) and message_h["offer"][i] is not None:
                    price = message_h["offer"][i].get("price")
                    qty = message_h["offer"][i].get("qtty")
                    if price is not None:
                        ap.append(float(price))
                    if qty is not None:
                        aq.append(float(qty))
            except (IndexError, KeyError, TypeError):
                continue

    return [{
        "time": get_dnse_time_s_to_i(message_h['sendingTime']),
        "symbol": symbol,
        "bp": bp,
        "bq": bq,
        "ap": ap,
        "aq": aq,
        "total_bid": float(message_h.get('totalOfferQtty', 0)),
        "total_ask": float(message_h.get('totalBidQtty', 0)),
        "data_type": "TP",
        "source": "dnse"
    }]


def transform_tick(message_h: Dict[str, Any]) -> List[Dict[str, Any]]:
    side_mapping = {
        "SIDE_SELL": 'S',
        "SIDE_BUY": 'B'
    }
    side = side_mapping.get(message_h.get("side", ""), 'U')

    return [{
        "time": get_dnse_time_s_to_i(message_h['sendingTime']),
        "symbol": str(message_h["symbol"]),
        "price": float(message_h["matchPrice"]),
        "vol": float(message_h["matchQtty"]),
        "side": side,
        "data_type": "ST",
        "source": "dnse"
    }]


def transform_stock_info(message_h: Dict[str, Any]) -> List[Dict[str, Any]]:
    msg_time = get_dnse_time_s_to_i(message_h['tradingTime'])
    symbol = str(message_h["symbol"])

    foreign_message = {
        "time": msg_time,
        "symbol": symbol,
        "total_room": float(message_h.get("foreignerBuyPossibleQuantity", 0)),
        "current_room": float(message_h.get("foreignerOrderLimitQuantity", 0)),
        "buy_vol": float(message_h.get("buyForeignQuantity", 0)),
        "sell_vol": float(message_h.get("sellForeignQuantity", 0)),
        "buy_val": float(message_h.get("buyForeignValue", 0)),
        "sell_val": float(message_h.get("sellForeignValue", 0)),
        "data_type": "SF",
        "source": "dnse"
    }

    stock_info_message = {
        "time": msg_time,
        "symbol": symbol,
        "open": float(message_h.get("openPrice", 0)),
        "high": float(message_h.get("highestPrice", 0)),
        "low": float(message_h.get("lowestPrice", 0)),
        "close": float(message_h.get("closePrice", 0)),
        "avg": float(message_h.get("averagePrice", 0)),
        "ceil": float(message_h.get("highLimitPrice", 0)),
        "floor": float(message_h.get("lowLimitPrice", 0)),
        "prior": float(message_h.get("referencePrice", 0)),
        "data_type": "SI",
        "source": "dnse"
    }

    stock_price_board_message = {
        "time": msg_time,
        "symbol": symbol,
        "price": float(message_h.get("matchPrice", 0)),
        "vol": float(message_h.get("matchQuantity", 0)),
        "total_vol": float(message_h.get("totalVolumeTraded", 0)),
        "total_val": float(message_h.get("grossTradeAmount", 0)),
        "change": float(message_h.get("changedValue", 0)),
        "change_pct": float(message_h.get("changedRatio", 0)),
        "data_type": "PB",
        "source": "dnse"
    }
    
    return [foreign_message, stock_info_message, stock_price_board_message]


def transform_market_index(message_h: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{
        "time": get_dnse_time_s_to_i(message_h['transactTime']),
        "symbol": str(message_h["indexName"]),
        "name": str(message_h["indexName"]),
        "prior": float(message_h.get("priorValueIndexes", 0)),
        "value": float(message_h["valueIndexes"]),
        "total_vol": float(message_h["totalVolumeTraded"]),
        "total_val": float(message_h["grossTradeAmount"]),
        "advance": int(message_h.get("fluctuationUpIssueCount", 0)),
        "decline": int(message_h.get("fluctuationDownIssueCount", 0)),
        "nochange": int(message_h.get("fluctuationSteadinessIssueCount", 0)),
        "ceil": int(message_h.get("fluctuationUpperLimitIssueCount", 0)),
        "floor": int(message_h.get("fluctuationLowerLimitIssueCount", 0)),
        "change": float(message_h.get("changedValue", 0)),
        "change_pct": float(message_h.get("changedRatio", 0)),
        "data_type": "MI",
        "source": "dnse"
    }]


def transform_board_event(message_h: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{
        "time": get_dnse_time_s_to_i(message_h['sendingTime']),
        "symbol": str(message_h["symbol"]),
        "market_id": message_h.get("marketId"),
        "board_id": message_h.get("boardId"),
        "board_id_original": message_h.get("boardIdOriginal"),
        "event_id": message_h.get("eventId"),
        "trading_schedule_control_id": message_h.get("tscProdGrpId"),
        "trading_session_id": message_h.get("tradingSessionId"),
        "data_type": "BE",
        "source": "dnse"
    }]


def filter_transform(key: str, message_h: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        if key.startswith(STOCK_OHLC_PATTERN):
            new_datas = transform_ohlcv(message_h)
        elif key.startswith(STOCK_TOP_PRICE_PATTERN):
            new_datas = transform_top_price(message_h)
        elif key.startswith(STOCK_TICK_PATTERN):
            new_datas = transform_tick(message_h)
        elif key.startswith(STOCK_INFO_PATTERN):
            new_datas = transform_stock_info(message_h)
        elif key.startswith(STOCK_MARKET_INDEX_PATTERN):
            new_datas = transform_market_index(message_h)
        elif key.startswith(STOCK_BOARD_EVENT_PATTERN):
            new_datas = transform_board_event(message_h)
        else:
            new_datas = [{
                "time": int(time.time()),
                "data_type": "UK",
                "source": "dnse"
            }]
        
        return new_datas
        
    except Exception as e:
        print(f"Error transforming message: {e}")
        return []


def run_transform_pipeline():
    kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092')
    input_topic = 'dnse.raw'
    output_topic = 'dnse.transform'
    group_id = 'dnse.transform.pipeline'
    checksum_algorithm = os.getenv('CHECKSUM_ALGORITHM', 'sha256').lower()
    enable_checksum_verification = os.getenv('ENABLE_CHECKSUM_VERIFICATION', 'false').lower() == 'true'
    
    print(f"Starting Transform Pipeline: Kafka '{input_topic}' -> transform -> Kafka '{output_topic}'")
    print(f"Checksum Algorithm: {checksum_algorithm}")
    print(f"Checksum Verification: {'Enabled' if enable_checksum_verification else 'Disabled'}")
    
    try:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=kafka_servers.split(','),
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else {},
            key_deserializer=lambda k: k.decode('utf-8') if k else ''
        )
        print(f"Connected to Kafka consumer for topic: {input_topic}")
    except Exception as e:
        print(f"Error creating Kafka consumer: {e}")
        return
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=3,
            acks='all'
        )
        print(f"Connected to Kafka producer for topic: {output_topic}")
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        consumer.close()
        return
    
    processed = 0
    transformed_count = 0
    checksum_count = 0
    duplicates = 0
    seen_checksums: Set[str] = set()
    
    try:
        print("Starting message processing...")
        for message in consumer:
            try:
                key = message.key or ''
                value = message.value or {}
                
                input_checksum = value.get('checksum', '')
                if input_checksum and input_checksum in seen_checksums:
                    duplicates += 1
                    symbol = value.get('symbol', 'UNKNOWN')
                    print(f"Duplicate input detected: {symbol} - Checksum: {input_checksum[:8]}")
                    continue
                
                if input_checksum:
                    seen_checksums.add(input_checksum)
                
                transformed_list = filter_transform(key, value)
                
                for transformed in transformed_list:
                    try:
                        checksum = calculate_message_checksum(transformed, algorithm=checksum_algorithm)
                        transformed['checksum'] = checksum
                        
                        if enable_checksum_verification:
                            is_valid = verify_message_checksum(transformed, algorithm=checksum_algorithm)
                            if not is_valid:
                                print(f"Warning: Checksum verification failed for message")
                                continue
                        
                        msg_key = transformed.get('symbol', '')
                        
                        future = producer.send(
                            output_topic,
                            key=msg_key,
                            value=transformed
                        )
                        
                        data_type = transformed.get('data_type', 'UNKNOWN')
                        symbol = transformed.get('symbol', 'UNKNOWN')
                        checksum_short = checksum[:8] if checksum else "NONE"
                        print(f"Transformed: {data_type} - {symbol} - Checksum: {checksum_short}")
                        
                        transformed_count += 1
                        if checksum:
                            checksum_count += 1
                        
                    except Exception as e:
                        print(f"Error sending transformed message: {e}")
                
                processed += 1
                
                if processed % 100 == 0:
                    print(f"Processed {processed} messages, transformed {transformed_count} outputs, checksums added: {checksum_count}, duplicates: {duplicates}")
                
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        consumer.close()
        producer.close()
        print(f"Transform pipeline completed. Processed: {processed}, Transformed: {transformed_count}, Checksums: {checksum_count}, Duplicates: {duplicates}")


if __name__ == "__main__":
    run_transform_pipeline()
