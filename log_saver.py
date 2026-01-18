# -*- coding: utf-8 -*-
"""
================================================================================
log_saver.py - Redis 구독 → 파일 저장
================================================================================
역할: Redis 구독 → 파싱 → 텍스트 파일 저장

구독 채널 (6개):
- krx:futures:stock       (주식선물)
- krx:futures:index       (지수선물)
- krx:options:call:stock  (주식콜옵션)
- krx:options:call:index  (지수콜옵션)
- krx:options:put:stock   (주식풋옵션)
- krx:options:put:index   (지수풋옵션)

저장 경로: D:/MarketData/Logs/YYYY-MM-DD/
파일명:
- futures_stock.log       (주식선물)
- futures_index.log       (지수선물)
- options_call_stock.log  (주식콜옵션)
- options_call_index.log  (지수콜옵션)
- options_put_stock.log   (주식풋옵션)
- options_put_index.log   (지수풋옵션)

로그 포맷: 원본 패킷 그대로 (접속표준서 형식)

장시간 체크: 08:40 ~ 15:50 (파생상품 08:45~15:45 + 버퍼)
- 장시간 외에는 저장하지 않음
================================================================================
"""

import redis
import struct
import time
import json
import threading
from datetime import datetime
from pathlib import Path

# ==============================================================================
# 설정 (config.json에서 로드)
# ==============================================================================

def load_config() -> dict:
    """config.json 파일에서 설정 로드"""
    config_path = Path(__file__).parent / 'config.json'
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[!] config.json 파일을 찾을 수 없습니다: {config_path}")
        print("[!] 기본값을 사용합니다.")
        return {
            "redis": {"host": "localhost", "port": 6379},
            "log_base_dir": "D:/MarketData/Logs"
        }

CONFIG = load_config()

REDIS_HOST = CONFIG['redis']['host']
REDIS_PORT = CONFIG['redis']['port']

# 구독할 Redis 채널 및 파일 매핑
CHANNEL_FILE_MAP = {
    'krx:futures:stock': 'futures_stock.log',
    'krx:futures:index': 'futures_index.log',
    'krx:options:call:stock': 'options_call_stock.log',
    'krx:options:call:index': 'options_call_index.log',
    'krx:options:put:stock': 'options_put_stock.log',
    'krx:options:put:index': 'options_put_index.log',
}

# 저장 경로
LOG_BASE_DIR = Path(CONFIG.get('log_base_dir', 'D:/MarketData/Logs'))

# 장시간 설정 (파생상품: 08:45~15:45, 버퍼 포함 08:40~15:50)
MARKET_OPEN_HOUR = 8
MARKET_OPEN_MINUTE = 40
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 50

# 메시지 포맷
# SISE_receiver에서 보내는 메시지: timestamp(8) + port(2) + raw_packet
TIMESTAMP_SIZE = 8
PORT_SIZE = 2
HEADER_SIZE = TIMESTAMP_SIZE + PORT_SIZE  # 10 bytes

# 패킷 내 필드 위치 (raw_packet 기준)
TR_CODE_START = 0
TR_CODE_END = 5
ISIN_START = 17  # filtered_saver.py와 동일
ISIN_END = 29    # 12자리 ISIN


# ==============================================================================
# 장시간 체크
# ==============================================================================

def is_market_hours() -> bool:
    """
    현재 시각이 장시간인지 체크

    파생상품 거래시간: 08:45 ~ 15:45
    버퍼 포함: 08:40 ~ 15:50

    Returns:
        True if 장시간, False otherwise
    """
    now = datetime.now()
    current_minutes = now.hour * 60 + now.minute

    open_minutes = MARKET_OPEN_HOUR * 60 + MARKET_OPEN_MINUTE  # 08:40 = 520
    close_minutes = MARKET_CLOSE_HOUR * 60 + MARKET_CLOSE_MINUTE  # 15:50 = 950

    return open_minutes <= current_minutes <= close_minutes


# ==============================================================================
# 메시지 파싱
# ==============================================================================

def parse_message(data: bytes) -> dict:
    """
    Redis 메시지 파싱

    메시지 포맷: timestamp(8) + port(2) + raw_packet

    Returns:
        {
            'timestamp': float,      # Unix timestamp
            'port': int,            # UDP 포트 번호
            'tr_code': str,         # TR 코드 (5자리)
            'isin': str,            # ISIN 코드 (12자리)
            'raw_hex': str,         # Raw packet hex
            'raw': bytes            # Raw packet bytes
        }
    """
    if len(data) < HEADER_SIZE + TR_CODE_END:
        return None

    try:
        # 헤더 파싱
        timestamp = struct.unpack('d', data[:TIMESTAMP_SIZE])[0]
        port = struct.unpack('H', data[TIMESTAMP_SIZE:HEADER_SIZE])[0]

        # Raw packet
        raw = data[HEADER_SIZE:]

        # TR 코드
        tr_code = raw[TR_CODE_START:TR_CODE_END].decode('ascii', errors='ignore')

        # ISIN 코드
        if len(raw) >= ISIN_END:
            isin = raw[ISIN_START:ISIN_END].decode('ascii', errors='ignore')
        else:
            isin = ''

        # Raw hex
        raw_hex = raw.hex()

        return {
            'timestamp': timestamp,
            'port': port,
            'tr_code': tr_code,
            'isin': isin,
            'raw_hex': raw_hex,
            'raw': raw
        }
    except Exception:
        return None


def format_log_line(parsed: dict) -> str:
    """
    파싱된 데이터를 로그 라인으로 포맷

    포맷: 원본 패킷 그대로 (접속표준서 형식)
    - 0~4: TR코드
    - 17~28: ISIN
    - 등등... 접속표준서 참조
    """
    return parsed['raw'].decode('ascii', errors='ignore').strip()


# ==============================================================================
# 파일 관리
# ==============================================================================

class LogFileManager:
    """날짜별 로그 파일 관리"""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.files = {}  # channel -> file handle
        self.current_date = None
        self.lock = threading.Lock()
        self.stats = {ch: 0 for ch in CHANNEL_FILE_MAP}

    def _get_today_dir(self) -> Path:
        """오늘 날짜 디렉토리 반환"""
        today = datetime.now().strftime('%Y-%m-%d')
        today_dir = self.base_dir / today
        today_dir.mkdir(parents=True, exist_ok=True)
        return today_dir

    def _get_file(self, channel: str):
        """채널에 해당하는 파일 핸들 반환"""
        today = datetime.now().strftime('%Y-%m-%d')

        # 날짜가 바뀌면 기존 파일 닫고 새로 열기
        if self.current_date != today:
            self._close_all()
            self.current_date = today

        if channel not in self.files:
            filename = CHANNEL_FILE_MAP.get(channel)
            if not filename:
                return None

            filepath = self._get_today_dir() / filename
            self.files[channel] = open(filepath, 'a', encoding='utf-8')
            print(f"[*] 파일 오픈: {filepath}")

        return self.files[channel]

    def write(self, channel: str, log_line: str):
        """로그 라인 저장"""
        with self.lock:
            f = self._get_file(channel)
            if f:
                f.write(log_line + '\n')
                f.flush()

                # 통계 업데이트
                if channel in self.stats:
                    self.stats[channel] += 1

    def _close_all(self):
        """모든 파일 닫기"""
        for f in self.files.values():
            try:
                f.close()
            except:
                pass
        self.files.clear()

    def close(self):
        """종료 시 호출"""
        with self.lock:
            self._close_all()

    def get_stats(self) -> dict:
        """통계 반환"""
        with self.lock:
            return dict(self.stats)


# ==============================================================================
# Redis 구독 → 파일 저장
# ==============================================================================

def subscribe_and_save(file_manager: LogFileManager, running: list):
    """
    Redis 구독 → 파싱 → 파일 저장 루프

    구조: redis_saver.py 패턴 (single thread, subscribe all channels)
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    pubsub = r.pubsub()

    channels = list(CHANNEL_FILE_MAP.keys())
    pubsub.subscribe(*channels)

    print(f"[*] Redis 구독 시작: {channels}")

    skipped_out_of_hours = 0

    for message in pubsub.listen():
        if not running[0]:
            break

        if message['type'] != 'message':
            continue

        try:
            channel = message['channel'].decode()
            data = message['data']

            # 장시간 체크
            if not is_market_hours():
                skipped_out_of_hours += 1
                if skipped_out_of_hours % 1000 == 0:
                    print(f"[!] 장외시간 스킵: {skipped_out_of_hours:,}건")
                continue

            # 메시지 파싱
            parsed = parse_message(data)
            if not parsed:
                continue

            # 로그 포맷 변환
            log_line = format_log_line(parsed)

            # 파일에 저장
            file_manager.write(channel, log_line)

        except Exception as e:
            print(f"[!] 저장 에러: {e}")

    pubsub.close()


# ==============================================================================
# 메인
# ==============================================================================

def main():
    print("=" * 70)
    print("  log_saver - Redis 구독 → 파일 저장")
    print("=" * 70)
    print(f"  Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"  저장 경로: {LOG_BASE_DIR}")
    print(f"  구독 채널: {len(CHANNEL_FILE_MAP)}개")
    for ch, filename in CHANNEL_FILE_MAP.items():
        print(f"    {ch} → {filename}")
    print(f"  장 시간: {MARKET_OPEN_HOUR:02d}:{MARKET_OPEN_MINUTE:02d} ~ "
          f"{MARKET_CLOSE_HOUR:02d}:{MARKET_CLOSE_MINUTE:02d}")
    print(f"  로그 포맷: 원본 패킷 그대로 (접속표준서 형식)")
    print("=" * 70)

    # 로그 디렉토리 확인
    LOG_BASE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[*] 로그 디렉토리 확인: {LOG_BASE_DIR}")

    # Redis 연결 확인
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()
        print("[*] Redis 연결 성공")
    except Exception as e:
        print(f"[!] Redis 연결 실패: {e}")
        return

    file_manager = LogFileManager(LOG_BASE_DIR)
    running = [True]

    try:
        # 저장 스레드 시작
        t = threading.Thread(
            target=subscribe_and_save,
            args=(file_manager, running),
            daemon=True
        )
        t.start()

        print("[*] 저장 시작. Ctrl+C로 종료")
        print()

        # 현재 장시간 여부 출력
        if is_market_hours():
            print("[*] 현재 장시간 - 데이터 저장 활성화")
        else:
            print("[!] 현재 장외시간 - 데이터 저장 대기 중")
        print()

        # 상태 출력 루프 (30초마다)
        while True:
            time.sleep(30)
            stats = file_manager.get_stats()
            total = sum(stats.values())

            market_status = "장중" if is_market_hours() else "장외"
            print(f"[상태] {datetime.now().strftime('%H:%M:%S')} [{market_status}] - "
                  f"주선:{stats['krx:futures:stock']:,} "
                  f"지선:{stats['krx:futures:index']:,} "
                  f"주콜:{stats['krx:options:call:stock']:,} "
                  f"지콜:{stats['krx:options:call:index']:,} "
                  f"주풋:{stats['krx:options:put:stock']:,} "
                  f"지풋:{stats['krx:options:put:index']:,} "
                  f"(총 {total:,}건)")

    except KeyboardInterrupt:
        print("\n[*] 종료 요청...")
    finally:
        running[0] = False
        file_manager.close()
        stats = file_manager.get_stats()
        total = sum(stats.values())
        print(f"[*] 최종: 주선:{stats['krx:futures:stock']:,} "
              f"지선:{stats['krx:futures:index']:,} "
              f"주콜:{stats['krx:options:call:stock']:,} "
              f"지콜:{stats['krx:options:call:index']:,} "
              f"주풋:{stats['krx:options:put:stock']:,} "
              f"지풋:{stats['krx:options:put:index']:,} "
              f"(총 {total:,}건)")
        print("[*] 프로그램 종료")


if __name__ == '__main__':
    main()
