# MM Monitoring System

KRX 파생상품 시세 수신 및 로그 저장 시스템

## 시스템 구조

```
KRX UDP Multicast
       │
       ▼
┌─────────────────┐      ┌─────────┐      ┌─────────────┐
│ SISE_receiver   │ ───► │  Redis  │ ───► │ log_saver   │
│ (시세 수신)      │      │ (Pub/Sub)│     │ (로그 저장)  │
└─────────────────┘      └─────────┘      └─────────────┘
```

### Redis 채널 (6개)
| 채널명 | 설명 |
|--------|------|
| `krx:futures:stock` | 주식선물 |
| `krx:futures:index` | 지수선물 |
| `krx:options:call:stock` | 주식콜옵션 |
| `krx:options:call:index` | 지수콜옵션 |
| `krx:options:put:stock` | 주식풋옵션 |
| `krx:options:put:index` | 지수풋옵션 |

---

## 설치 방법

### 1. Python 패키지 설치

```bash
pip install -r requirements.txt
```

### 2. Redis 서버 설치 (Windows)

Redis는 원래 Linux용이라 Windows에서는 별도 설치가 필요합니다.

#### WSL (Windows Subsystem for Linux)
```bash
# PowerShell 관리자 권한으로 실행
wsl --install

# WSL 터미널에서
sudo apt update
sudo apt install redis-server
sudo service redis-server start
```

#### Redis 설치 확인
```bash
redis-cli ping
# 응답: PONG
```

### 3. 설정 파일 수정

`config.json` 파일을 본인 환경에 맞게 수정:

```json
{
  "redis": {
    "host": "localhost",
    "port": 6379
  },
  "db": {
    "host": "172.17.22.93",
    "port": 5432,
    "dbname": "ds_stock",
    "user": "postgres",
    "password": "dsstock"
  },
  "multicast_nic": "192.168.205.XX",
  "log_base_dir": "D:/MarketData/Logs"
}
```

#### 변경 필요 항목
| 항목 | 설명 | 변경 여부 |
|------|------|-----------|
| `redis.host` | Redis 서버 주소 | localhost 유지 |
| `db.host` | PostgreSQL 서버 주소 | 172.17.22.93 유지 (중앙 DB) |
| `multicast_nic` | 시세 케이블 NIC IP | **본인 IP로 변경** |
| `log_base_dir` | 로그 저장 경로 | **본인 경로로 변경** |

#### NIC IP 확인 방법
```bash
ipconfig
# 시세 케이블이 연결된 어댑터의 IPv4 주소 확인 (192.168.205.XX)
```

---

## 실행 방법

### SISE_receiver 실행 (시세 수신)
```bash
python SISE_receiver.py
```
또는
```bash
start_SISE_receiver.bat
```

### log_saver 실행 (로그 저장)
```bash
python log_saver.py
```
또는
```bash
start_log_saver.bat
```

---

## 파일 설명

| 파일 | 설명 |
|------|------|
| `SISE_receiver.py` | KRX UDP 멀티캐스트 수신 → Redis 발행 |
| `log_saver.py` | Redis 구독 → 로그 파일 저장 |
| `config.json` | 연결 설정 (Redis, DB, NIC, 경로) |
| `requirements.txt` | Python 패키지 목록 |

---

## 저장 로그 형식

- 저장 경로: `{log_base_dir}/YYYY-MM-DD/`
- 파일명: `futures_stock.log`, `futures_index.log`, `options_call_stock.log`, 등
- 포맷: 원본 패킷 그대로 (KRX 접속표준서 형식)
- 장시간: 08:40 ~ 15:50 (장외시간에는 저장하지 않음)

---