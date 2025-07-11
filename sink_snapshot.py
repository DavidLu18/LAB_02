import json
import os
import psycopg2
from datetime import datetime
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer
import time
from dotenv import load_dotenv

load_dotenv()


def parse_timestamp(timestamp_value) -> str:
    try:
        if isinstance(timestamp_value, (int, float)):
            dt = datetime.fromtimestamp(timestamp_value)
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            dt = datetime.fromisoformat(str(timestamp_value).replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        print(f"Error parsing timestamp {timestamp_value}: {e}")
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


class PostgreSQLProcessor:
    
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.connection = None
        self.cursor = None
        self._connect()
    
    def _connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.connection_params['host'],
                port=self.connection_params['port'],
                database=self.connection_params['database'],
                user=self.connection_params['user'],
                password=self.connection_params['password']
            )
            self.cursor = self.connection.cursor()
            print("Connected to PostgreSQL")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
    
    def _ensure_connection(self):
        try:
            if self.connection is None or self.connection.closed:
                self._connect()
            if self.cursor is None:
                self._connect()
        except Exception as e:
            print(f"Error ensuring connection: {e}")
            self._connect()
    
    def process_item(self, item: Dict[str, Any]) -> bool:
        self._ensure_connection()
        
        try:
            data_type = item.get('data_type')
            
            if data_type == "OH":
                self._insert_ohlcv(item)
            elif data_type == "TP":
                self._insert_top_price(item)
            elif data_type == "ST":
                self._insert_stock_tick(item)
            elif data_type == "SF":
                self._insert_stock_foreign(item)
            elif data_type == "SI":
                self._insert_stock_info(item)
            elif data_type == "PB":
                self._insert_price_board(item)
            elif data_type == "MI":
                self._insert_market_index(item)
                self._insert_market_index_history(item)
            else:
                return False
                
            if self.connection:
                self.connection.commit()
            return True
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def _insert_ohlcv(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_ohlcv (
          time, symbol, resolution, source,
          open, high, low, close, volume, updated
        ) VALUES (
          %s::TIMESTAMP, %s, %s, %s,
          %s, %s, %s, %s, %s, %s::TIMESTAMP
        )
        ON CONFLICT (time, symbol, resolution, source)
        DO UPDATE SET
          open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          volume = EXCLUDED.volume,
          updated = EXCLUDED.updated
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['resolution'], 
            item['source'],
            item['open'],
            item['high'],
            item['low'],
            item['close'],
            item['volume'],
            parse_timestamp(item['updated'])
        )
        
        self.cursor.execute(sql, params)
    
    def _insert_top_price(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_top_price (
          time, symbol, source,
          bp, bq, ap, aq,
          total_bid, total_ask
        ) VALUES (
          %s::TIMESTAMP, %s, %s,
          %s::JSONB, %s::JSONB, %s::JSONB, %s::JSONB,
          %s, %s
        )
        ON CONFLICT (symbol, source)
        DO UPDATE SET
          time = EXCLUDED.time,
          bp = EXCLUDED.bp,
          bq = EXCLUDED.bq,
          ap = EXCLUDED.ap,
          aq = EXCLUDED.aq,
          total_bid = EXCLUDED.total_bid,
          total_ask = EXCLUDED.total_ask
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['source'],
            json.dumps(item['bp']),
            json.dumps(item['bq']),
            json.dumps(item['ap']),
            json.dumps(item['aq']),
            item['total_bid'],
            item['total_ask']
        )
        
        self.cursor.execute(sql, params)
    
    def _insert_stock_tick(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_tick (
          time, symbol, source,
          price, vol, side
        ) VALUES (
          %s::TIMESTAMP, %s, %s,
          %s, %s, %s
        )
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['source'],
            item['price'],
            item['vol'],
            item['side']
        )
        
        self.cursor.execute(sql, params)
    
    def _insert_stock_foreign(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_foreign (
          time, symbol, source,
          total_room, current_room,
          buy_vol, sell_vol,
          buy_val, sell_val
        ) VALUES (
          %s::TIMESTAMP, %s, %s,
          %s, %s,
          %s, %s,
          %s, %s
        )
        ON CONFLICT (symbol, source)
        DO UPDATE SET
          time = EXCLUDED.time,
          total_room = EXCLUDED.total_room,
          current_room = EXCLUDED.current_room,
          buy_vol = EXCLUDED.buy_vol,
          sell_vol = EXCLUDED.sell_vol,
          buy_val = EXCLUDED.buy_val,
          sell_val = EXCLUDED.sell_val
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['source'],
            item['total_room'],
            item['current_room'],
            item['buy_vol'],
            item['sell_vol'],
            item['buy_val'],
            item['sell_val']
        )
        
        self.cursor.execute(sql, params)
    
    def _insert_stock_info(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_info (
          time, symbol, source,
          open, high, low, close,
          avg, ceil, floor, prior
        ) VALUES (
          %s::TIMESTAMP, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s
        )
        ON CONFLICT (symbol, source)
        DO UPDATE SET
          time = EXCLUDED.time,
          open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          avg = EXCLUDED.avg,
          ceil = EXCLUDED.ceil,
          floor = EXCLUDED.floor,
          prior = EXCLUDED.prior
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['source'],
            item['open'],
            item['high'],
            item['low'],
            item['close'],
            item['avg'],
            item['ceil'],
            item['floor'],
            item['prior']
        )
        
        self.cursor.execute(sql, params)
    
    def _insert_price_board(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_price_board (
          time, symbol, source,
          price, vol, total_vol, total_val,
          change, change_pct
        ) VALUES (
          %s::TIMESTAMP, %s, %s,
          %s, %s, %s, %s,
          %s, %s
        )
        ON CONFLICT (symbol, source)
        DO UPDATE SET
          time = EXCLUDED.time,
          price = EXCLUDED.price,
          vol = EXCLUDED.vol,
          total_vol = EXCLUDED.total_vol,
          total_val = EXCLUDED.total_val,
          change = EXCLUDED.change,
          change_pct = EXCLUDED.change_pct
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['source'],
            item['price'],
            item['vol'],
            item['total_vol'],
            item['total_val'],
            item['change'],
            item['change_pct']
        )
        
        self.cursor.execute(sql, params)
    
    def _insert_market_index(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_market_index (
          time, symbol, source, name,
          prior, value, total_vol, total_val,
          advance, decline, nochange,
          ceil, floor, change, change_pct
        ) VALUES (
          %s::TIMESTAMP, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s,
          %s, %s, %s, %s
        )
        ON CONFLICT (symbol, source)
        DO UPDATE SET
          time = EXCLUDED.time,
          name = EXCLUDED.name,
          prior = EXCLUDED.prior,
          value = EXCLUDED.value,
          total_vol = EXCLUDED.total_vol,
          total_val = EXCLUDED.total_val,
          advance = EXCLUDED.advance,
          decline = EXCLUDED.decline,
          nochange = EXCLUDED.nochange,
          ceil = EXCLUDED.ceil,
          floor = EXCLUDED.floor,
          change = EXCLUDED.change,
          change_pct = EXCLUDED.change_pct
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['source'],
            item['name'],
            item['prior'],
            item['value'],
            item['total_vol'],
            item['total_val'],
            item['advance'],
            item['decline'],
            item['nochange'],
            item['ceil'],
            item['floor'],
            item['change'],
            item['change_pct']
        )
        
        self.cursor.execute(sql, params)
    
    def _insert_market_index_history(self, item: Dict[str, Any]):
        if not self.cursor:
            return
            
        sql = """
        INSERT INTO data.snapshot_stock_market_index_history (time, symbol, name, source, value)
        VALUES (%s::TIMESTAMP, %s, %s, %s, %s)
        """
        
        params = (
            parse_timestamp(item['time']),
            item['symbol'],
            item['name'],
            item['source'],
            item['value']
        )
        
        self.cursor.execute(sql, params)
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def run_sink_pipeline():
    kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092')
    input_topic = 'dnse.transform'
    group_id = 'dnse.sink.pipeline'
    
    pg_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'market_data'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password')
    }
    
    print(f"Starting Sink Pipeline: Kafka '{input_topic}' -> PostgreSQL DB")
    
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
        db_processor = PostgreSQLProcessor(pg_params)
    except Exception as e:
        print(f"Error creating PostgreSQL processor: {e}")
        consumer.close()
        return
    
    processed = 0
    successful = 0
    failed = 0
    
    try:
        print("Starting message processing...")
        for message in consumer:
            try:
                item = message.value
                
                data_type = item.get('data_type')
                if data_type in ["OH", "TS"]:
                    if 'time' in item:
                        item['time'] = parse_timestamp(item['time'])
                    if 'updated' in item:
                        item['updated'] = parse_timestamp(item['updated'])
                else:
                    if 'time' in item:
                        item['time'] = parse_timestamp(item['time'])
                
                success = db_processor.process_item(item)
                
                if success:
                    successful += 1
                    data_type = item.get('data_type', 'UNKNOWN')
                    symbol = item.get('symbol', 'UNKNOWN')
                    print(f"Sunk to DB: {data_type} - {symbol}")
                else:
                    failed += 1
                
                processed += 1
                
                if processed % 100 == 0:
                    print(f"Processed {processed} messages. Success: {successful}, Failed: {failed}")
                
            except Exception as e:
                failed += 1
                print(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        consumer.close()
        db_processor.close()
        print(f"Sink pipeline completed. Processed: {processed}, Success: {successful}, Failed: {failed}")


if __name__ == "__main__":
    run_sink_pipeline()
