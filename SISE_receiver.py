# -*- coding: utf-8 -*-
"""
================================================================================
SISE_receiver.py - KRX 시세 수신 및 Redis 배포
================================================================================
역할: KRX UDP 멀티캐스트 수신 → ISIN 분류 → Redis publish

Redis 채널 (6개로 세분화):
- krx:futures:stock       (주식선물)
- krx:futures:index       (지수선물)
- krx:options:call:stock  (주식콜옵션)
- krx:options:call:index  (지수콜옵션)
- krx:options:put:stock   (주식풋옵션)
- krx:options:put:index   (지수풋옵션)

메시지 포맷:
- timestamp (8 bytes, double) + port (2 bytes, unsigned short) + raw packet

ISIN 분류 기준:
- 주식선물: DB(futures_master)에 등록된 ISIN prefix
- 지수선물: DB에 없는 선물 (코스피200, 코스닥150, 섹터지수, 금융상품선물 등)
- 주식옵션: DB(stock_options_master)에 등록된 ISIN prefix
- 지수옵션: DB에 없는 옵션 (코스피200옵션, 미니옵션 등)
================================================================================
"""

import socket
import struct
import threading
import time
import json
import redis
import psycopg2
from datetime import datetime
from pathlib import Path
from typing import Set

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
            "db": {"host": "localhost", "port": 5432, "dbname": "ds_stock",
                   "user": "postgres", "password": "dsstock"},
            "multicast_nic": "192.168.205.93"
        }

CONFIG = load_config()

REDIS_HOST = CONFIG['redis']['host']
REDIS_PORT = CONFIG['redis']['port']

# DB 설정 (주식선물 ISIN 목록 조회용)
DB_CONFIG = {
    'host': CONFIG['db']['host'],
    'port': CONFIG['db']['port'],
    'dbname': CONFIG['db']['dbname'],
    'user': CONFIG['db']['user'],
    'password': CONFIG['db']['password']
}

# 시세 수신용 NIC (네트워크 인터페이스)
# 시세 케이블이 연결된 NIC IP 주소
MULTICAST_NIC = CONFIG['multicast_nic']

# KRX 멀티캐스트 채널 설정
# 선물: 233.38.231.92, 포트 10302~10310 (9개)
# 콜옵션: 233.38.231.96, 포트 10322~10328 (7개)
# 풋옵션: 233.38.231.97, 포트 10331~10337 (7개)
MULTICAST_CHANNELS = [
    # 선물 채널 (9개)
    *[{'name': f'선물{i}', 'group': '233.38.231.92', 'port': 10302 + i,
       'type': 'futures'} for i in range(9)],

    # 콜옵션 채널 (7개)
    *[{'name': f'콜{i}', 'group': '233.38.231.96', 'port': 10322 + i,
       'type': 'call'} for i in range(7)],

    # 풋옵션 채널 (7개)
    *[{'name': f'풋{i}', 'group': '233.38.231.97', 'port': 10331 + i,
       'type': 'put'} for i in range(7)],
]

# 관심 TR 코드 (호가 + 체결)
TARGET_TR_CODES = [
    b'B604F', b'B605F',  # 호가
    b'A301F', b'A302F', b'A303F', b'A304F', b'A305F', b'A306F', b'A307F',
    b'A308F', b'A309F', b'A310F', b'A311F', b'A312F', b'A313F',
    b'A315F', b'A316F', b'A317F',  # 체결
]

# ISIN 위치 (패킷 내)
# TR코드: 0-5, ISIN: 17-29 (12자리) - filtered_saver.py와 동일
ISIN_START = 17
ISIN_END = 29
ISIN_LENGTH = 12

# 주식선물 ISIN prefix Set (DB에서 로드됨)
# 시작 시 load_stock_futures_prefixes()로 채워짐
STOCK_FUTURES_PREFIXES: Set[str] = set()

# 주식옵션 ISIN prefix Set (DB에서 로드됨)
# 시작 시 load_stock_options_prefixes()로 채워짐
STOCK_OPTIONS_PREFIXES: Set[str] = set()


# ==============================================================================
# DB에서 주식선물 ISIN prefix 로드
# ==============================================================================

def load_stock_futures_prefixes() -> Set[str]:
    """
    DB(futures_master)에서 주식선물 ISIN prefix 목록 로드

    Returns:
        주식선물 ISIN prefix Set (앞 6자리)
        예: {'KR4AB0', 'KR4A24', 'KR4ABN', ...}
    """
    global STOCK_FUTURES_PREFIXES

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # futures_master에서 모든 선물 코드의 앞 6자리 추출
        cur.execute("SELECT DISTINCT LEFT(future_code, 6) FROM futures_master")
        prefixes = {row[0] for row in cur.fetchall() if row[0]}

        cur.close()
        conn.close()

        STOCK_FUTURES_PREFIXES = prefixes
        print(f"[*] 주식선물 ISIN prefix {len(prefixes)}개 로드 완료")
        return prefixes

    except Exception as e:
        print(f"[!] DB 연결 실패, 기본값 사용: {e}")
        # DB 연결 실패 시 빈 Set 반환 (모두 지수선물로 분류됨)
        return set()


def load_stock_options_prefixes() -> Set[str]:
    """
    DB(stock_options_master)에서 주식옵션 ISIN prefix 목록 로드

    Returns:
        주식옵션 ISIN prefix Set (앞 6자리)
        예: {'KR4B11', 'KR4BBN', 'KR4C11', ...}
    """
    global STOCK_OPTIONS_PREFIXES

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # stock_options_master에서 모든 옵션 코드의 앞 6자리 추출
        cur.execute("SELECT DISTINCT LEFT(option_code, 6) FROM stock_options_master")
        prefixes = {row[0] for row in cur.fetchall() if row[0]}

        cur.close()
        conn.close()

        STOCK_OPTIONS_PREFIXES = prefixes
        print(f"[*] 주식옵션 ISIN prefix {len(prefixes)}개 로드 완료")
        return prefixes

    except Exception as e:
        print(f"[!] DB 연결 실패, 기본값 사용: {e}")
        # DB 연결 실패 시 빈 Set 반환 (모두 지수옵션으로 분류됨)
        return set()


# ==============================================================================
# ISIN 분류 함수
# ==============================================================================

def classify_futures_isin(isin_bytes: bytes) -> str:
    """
    선물 ISIN을 주식선물/지수선물로 분류

    주식선물: DB(futures_master)에 등록된 ISIN prefix
    지수선물: DB에 없는 선물 (코스피200, 코스닥150, 섹터지수, 금융상품선물 등)

    Returns:
        'krx:futures:stock' 또는 'krx:futures:index'
    """
    if len(isin_bytes) >= 6:
        # ISIN 앞 6자리로 주식선물 여부 판단
        try:
            prefix = isin_bytes[:6].decode('ascii', errors='ignore')
            if prefix in STOCK_FUTURES_PREFIXES:
                return 'krx:futures:stock'
        except:
            pass

    return 'krx:futures:index'


def classify_options_isin(isin_bytes: bytes, option_type: str) -> str:
    """
    옵션 ISIN을 주식옵션/지수옵션으로 분류

    주식옵션: DB(stock_options_master)에 등록된 ISIN prefix
    지수옵션: DB에 없는 옵션 (코스피200옵션, 미니옵션 등)

    Args:
        isin_bytes: ISIN 바이트
        option_type: 'call' 또는 'put'

    Returns:
        'krx:options:call:stock', 'krx:options:call:index',
        'krx:options:put:stock', 또는 'krx:options:put:index'
    """
    is_stock_option = False

    if len(isin_bytes) >= 6:
        try:
            prefix = isin_bytes[:6].decode('ascii', errors='ignore')
            if prefix in STOCK_OPTIONS_PREFIXES:
                is_stock_option = True
        except:
            pass

    if option_type == 'call':
        return 'krx:options:call:stock' if is_stock_option else 'krx:options:call:index'
    else:  # put
        return 'krx:options:put:stock' if is_stock_option else 'krx:options:put:index'


# ==============================================================================
# 통계
# ==============================================================================

class Stats:
    def __init__(self):
        self.received = 0
        self.published = 0
        self.by_channel = {
            'krx:futures:stock': 0,
            'krx:futures:index': 0,
            'krx:options:call:stock': 0,
            'krx:options:call:index': 0,
            'krx:options:put:stock': 0,
            'krx:options:put:index': 0,
        }
        self.errors = 0
        self.start_time = datetime.now()
        self.lock = threading.Lock()

    def add(self, channel: str):
        with self.lock:
            self.published += 1
            if channel in self.by_channel:
                self.by_channel[channel] += 1

    def add_received(self):
        with self.lock:
            self.received += 1

    def add_error(self):
        with self.lock:
            self.errors += 1

    def summary(self) -> str:
        with self.lock:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            rate = self.published / elapsed if elapsed > 0 else 0
            return (f"수신:{self.received:,} 발행:{self.published:,} "
                    f"(주선:{self.by_channel['krx:futures:stock']:,} "
                    f"지선:{self.by_channel['krx:futures:index']:,} "
                    f"주콜:{self.by_channel['krx:options:call:stock']:,} "
                    f"지콜:{self.by_channel['krx:options:call:index']:,} "
                    f"주풋:{self.by_channel['krx:options:put:stock']:,} "
                    f"지풋:{self.by_channel['krx:options:put:index']:,}) "
                    f"에러:{self.errors} ({rate:.1f}/초)")


stats = Stats()


# ==============================================================================
# UDP 수신 → Redis Publish
# ==============================================================================

def create_multicast_socket(group: str, port: int) -> socket.socket:
    """
    멀티캐스트 소켓 생성

    중요: 시세 케이블이 연결된 NIC(192.168.205.93)를 명시적으로 지정
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', port))

    # 멀티캐스트 그룹 가입 - 특정 NIC 지정
    # 4s4s = multicast_group(4bytes) + local_interface(4bytes)
    mreq = struct.pack('4s4s', socket.inet_aton(group), socket.inet_aton(MULTICAST_NIC))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    return sock


def receive_and_publish(channel: dict, redis_client: redis.Redis, running: list):
    """
    UDP 수신 → Redis publish 루프

    메시지 포맷: timestamp (8 bytes) + port (2 bytes) + raw packet
    """
    sock = create_multicast_socket(channel['group'], channel['port'])
    sock.settimeout(5)

    channel_type = channel['type']
    port_num = channel['port']

    while running[0]:
        try:
            data, _ = sock.recvfrom(2048)
            stats.add_received()

            # TR 코드 필터링 (패킷 첫 5바이트)
            if data[:5] not in TARGET_TR_CODES:
                continue

            # Redis 채널 결정
            if channel_type == 'futures':
                # 선물: ISIN으로 주식선물/지수선물 분류
                if len(data) >= ISIN_END:
                    isin = data[ISIN_START:ISIN_END]
                    redis_channel = classify_futures_isin(isin)
                else:
                    redis_channel = 'krx:futures:index'  # 기본값
            elif channel_type == 'call':
                # 콜옵션: ISIN으로 주식옵션/지수옵션 분류
                if len(data) >= ISIN_END:
                    isin = data[ISIN_START:ISIN_END]
                    redis_channel = classify_options_isin(isin, 'call')
                else:
                    redis_channel = 'krx:options:call:index'  # 기본값
            elif channel_type == 'put':
                # 풋옵션: ISIN으로 주식옵션/지수옵션 분류
                if len(data) >= ISIN_END:
                    isin = data[ISIN_START:ISIN_END]
                    redis_channel = classify_options_isin(isin, 'put')
                else:
                    redis_channel = 'krx:options:put:index'  # 기본값
            else:
                continue

            # 메시지 구성: timestamp + port + raw packet
            timestamp = struct.pack('d', time.time())
            port_bytes = struct.pack('H', port_num)
            message = timestamp + port_bytes + data

            # Redis publish
            redis_client.publish(redis_channel, message)
            stats.add(redis_channel)

        except socket.timeout:
            continue
        except Exception as e:
            stats.add_error()

    sock.close()


# ==============================================================================
# 메인
# ==============================================================================

def main():
    print("=" * 70)
    print("  SISE_receiver - KRX 시세 수신 및 Redis 배포")
    print("=" * 70)
    print(f"  Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"  수신 NIC: {MULTICAST_NIC}")
    print(f"  멀티캐스트 채널 수: {len(MULTICAST_CHANNELS)}개")
    print(f"    - 선물: 9개 (233.38.231.92:10302~10310)")
    print(f"    - 콜옵션: 7개 (233.38.231.96:10322~10328)")
    print(f"    - 풋옵션: 7개 (233.38.231.97:10331~10337)")
    print()
    print("  Redis 발행 채널 (6개):")
    print("    - krx:futures:stock       (주식선물)")
    print("    - krx:futures:index       (지수선물)")
    print("    - krx:options:call:stock  (주식콜옵션)")
    print("    - krx:options:call:index  (지수콜옵션)")
    print("    - krx:options:put:stock   (주식풋옵션)")
    print("    - krx:options:put:index   (지수풋옵션)")
    print()
    print("  메시지 포맷: timestamp(8) + port(2) + raw_packet")
    print("=" * 70)

    # DB에서 주식선물/주식옵션 ISIN prefix 로드
    load_stock_futures_prefixes()
    load_stock_options_prefixes()

    # Redis 연결
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()
        print("[*] Redis 연결 성공")
    except Exception as e:
        print(f"[!] Redis 연결 실패: {e}")
        return

    running = [True]
    threads = []

    try:
        # 각 멀티캐스트 채널별 수신 스레드 시작
        for ch in MULTICAST_CHANNELS:
            t = threading.Thread(
                target=receive_and_publish,
                args=(ch, r, running),
                daemon=True
            )
            threads.append(t)
            t.start()

        print(f"[*] {len(threads)}개 수신 스레드 시작")
        print("[*] Ctrl+C로 종료")
        print()

        # 상태 출력 루프 (30초마다)
        while True:
            time.sleep(30)
            print(f"[상태] {datetime.now().strftime('%H:%M:%S')} - {stats.summary()}")

    except KeyboardInterrupt:
        print("\n[*] 종료 요청...")
    finally:
        running[0] = False
        time.sleep(1)
        print(f"[*] 최종: {stats.summary()}")
        print("[*] 프로그램 종료")


if __name__ == '__main__':
    main()
