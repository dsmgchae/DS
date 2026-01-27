# -*- coding: utf-8 -*-
"""
DS투자증권 6개 종목 × 12 옵션 = 72개 전체 분석
- duty_requirements_2026.json에서 Q값 검증
- stock_options_master_2026-01-22.json에서 ISIN 분류
- v3 로직 (MM1/MM2 개별 추적, 승격 없음)
- 결과를 md 파일로 출력
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import json
import re
from collections import defaultdict
from datetime import datetime

# 파일 경로
LOG_DATE = "2026-01-22"
CALL_LOG = f"D:/MarketData/Logs/{LOG_DATE}/options_call_stock.log"
PUT_LOG = f"D:/MarketData/Logs/{LOG_DATE}/options_put_stock.log"
MASTER_FILE = f"D:/MarketData/Archive/stock_options_master_{LOG_DATE}.json"
DUTY_FILE = "C:/Users/ds/MM/duty_requirements_2026.json"
OUTPUT_FILE = f"C:/Users/ds/MM/analysis_workspace/mm_analysis_result_{LOG_DATE}.md"

# 의무시간 (09:05 ~ 15:20)
DUTY_START_SEC = 9 * 3600 + 5 * 60   # 32700
DUTY_END_SEC = 15 * 3600 + 20 * 60   # 55200
TOTAL_DUTY_SECONDS = DUTY_END_SEC - DUTY_START_SEC  # 22500초
PAIRING_WINDOW_MS = 100

# DS투자증권 담당 종목 (product_id로 매핑)
DS_STOCKS = {
    "KRDRVOPS14": {"name": "KT", "partner": "키움증권"},
    "KRDRVOPS24": {"name": "LG전자", "partner": "키움증권"},
    "KRDRVOPS32": {"name": "현대제철", "partner": "키움증권"},
    "KRDRVOPS45": {"name": "LG디스플레", "partner": "교보증권"},
    "KRDRVOPSCT": {"name": "삼성화재", "partner": "하나증권"},
    "KRDRVOPSF6": {"name": "카카오뱅크", "partner": "SK증권"},
}

OPTION_LEVELS = ["ITM1", "ATM", "OTM1", "OTM2", "OTM3", "OTM4"]


def load_duty_requirements():
    """Q값 로드"""
    with open(DUTY_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['stock_options_duty']


def load_master():
    """Master 파일 로드"""
    with open(MASTER_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def parse_time_to_seconds(time_str):
    return int(time_str[0:2]) * 3600 + int(time_str[2:4]) * 60 + int(time_str[4:6]) + int(time_str[6:12]) / 1000000


def extract_strike(name):
    """종목명에서 행사가 추출: 'LG디스플레 C 202602    12,000(  10)' -> 12000"""
    match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*\(', name)
    if match:
        return int(match.group(1).replace(',', ''))
    return 0


def get_underlying_name(name):
    """종목명에서 기초자산명 추출"""
    parts = name.split()
    if parts:
        return parts[0]
    return ''


def classify_options_for_stock(master, underlying_name):
    """특정 기초자산의 옵션들을 ATM/ITM/OTM으로 분류"""

    # 해당 기초자산의 모든 옵션 찾기
    calls = []
    puts = []

    for isin, info in master.items():
        name = info.get('종목명', '')
        if not name.startswith(underlying_name):
            continue

        expiry = info.get('만기일', '')[:6]  # 202602
        strike = extract_strike(name)
        atm_flag = info.get('ATM구분', '')  # 1=ATM, 2=ITM, 3=OTM

        opt_info = {
            'isin': isin,
            'name': name,
            'strike': strike,
            'expiry': expiry,
            'atm_flag': atm_flag
        }

        if ' C ' in name:
            calls.append(opt_info)
        elif ' P ' in name:
            puts.append(opt_info)

    # 최근월물 찾기
    all_expiries = set(c['expiry'] for c in calls) | set(p['expiry'] for p in puts)
    if not all_expiries:
        return None, None, None

    nearest_expiry = min(all_expiries)

    # 최근월물만 필터
    calls = [c for c in calls if c['expiry'] == nearest_expiry]
    puts = [p for p in puts if p['expiry'] == nearest_expiry]

    if not calls or not puts:
        return nearest_expiry, None, None

    # ATM 찾기 (atm_flag = '1')
    call_atm_list = [c for c in calls if c['atm_flag'] == '1']
    put_atm_list = [p for p in puts if p['atm_flag'] == '1']

    if not call_atm_list or not put_atm_list:
        return nearest_expiry, None, None

    # ATM이 여러 개면 중간값
    call_atm_list.sort(key=lambda x: x['strike'])
    put_atm_list.sort(key=lambda x: x['strike'])
    call_atm = call_atm_list[len(call_atm_list)//2]
    put_atm = put_atm_list[len(put_atm_list)//2]

    atm_strike = call_atm['strike']

    # 분류
    call_classified = {'ATM': call_atm}
    put_classified = {'ATM': put_atm}

    # CALL: 낮은 행사가 = ITM, 높은 행사가 = OTM
    calls_sorted = sorted(calls, key=lambda x: x['strike'])
    call_itm = [c for c in calls_sorted if c['strike'] < atm_strike]
    call_otm = [c for c in calls_sorted if c['strike'] > atm_strike]

    # ITM1 = ATM에서 가장 가까운 ITM
    if call_itm:
        call_classified['ITM1'] = call_itm[-1]  # 가장 높은 행사가 = ATM에 가장 가까움

    # OTM1~4 = ATM에서 가까운 순
    for i, opt in enumerate(call_otm[:4], 1):
        call_classified[f'OTM{i}'] = opt

    # PUT: 높은 행사가 = ITM, 낮은 행사가 = OTM
    puts_sorted = sorted(puts, key=lambda x: x['strike'])
    put_itm = [p for p in puts_sorted if p['strike'] > atm_strike]
    put_otm = [p for p in puts_sorted if p['strike'] < atm_strike]

    # ITM1 = ATM에서 가장 가까운 ITM
    if put_itm:
        put_classified['ITM1'] = put_itm[0]  # 가장 낮은 행사가 = ATM에 가장 가까움

    # OTM1~4 = ATM에서 가까운 순 (행사가 내림차순)
    put_otm_rev = list(reversed(put_otm))
    for i, opt in enumerate(put_otm_rev[:4], 1):
        put_classified[f'OTM{i}'] = opt

    return nearest_expiry, call_classified, put_classified


def parse_hoga(line, hoga_num):
    """호가 파싱"""
    base = 47 + (hoga_num - 1) * 46
    try:
        return int(line[base+18:base+27].strip() or '0'), int(line[base+27:base+36].strip() or '0')
    except:
        return 0, 0


def load_packets(log_file, target_isins):
    """패킷 로드"""
    packets = defaultdict(list)
    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if len(line) < 280:
                continue
            isin = line[17:29]
            if isin in target_isins:
                try:
                    time_str = line[35:47]
                    time_sec = parse_time_to_seconds(time_str)
                    if not (DUTY_START_SEC <= time_sec <= DUTY_END_SEC):
                        continue
                    total_ask, total_bid = 0, 0
                    for i in range(1, 6):
                        a, b = parse_hoga(line, i)
                        total_ask += a
                        total_bid += b
                    packets[isin].append({
                        'time_str': time_str,
                        'time_sec': time_sec,
                        'ask': total_ask,
                        'bid': total_bid
                    })
                except:
                    pass
    for isin in packets:
        packets[isin].sort(key=lambda x: x['time_sec'])
    return packets


class MMTrackerV3:
    """MM 상태 추적기 - 승격 없이 개별 추적"""

    def __init__(self, q_size):
        self.Q = q_size
        self.mm1 = None
        self.mm2 = None
        self.timeline = []
        self.baseline_ask = None
        self.baseline_bid = None

    def record_state(self, time_sec):
        self.timeline.append({
            'time_sec': time_sec,
            'mm1_present': self.mm1 is not None,
            'mm2_present': self.mm2 is not None
        })

    def mm1_enter(self, ask_q, bid_q, time_sec):
        self.mm1 = {'ask_q': ask_q, 'bid_q': bid_q}
        self.record_state(time_sec)

    def mm2_enter(self, ask_q, bid_q, time_sec):
        self.mm2 = {'ask_q': ask_q, 'bid_q': bid_q}
        self.record_state(time_sec)

    def mm1_exit(self, time_sec):
        self.mm1 = None  # 승격 없음!
        self.record_state(time_sec)

    def mm2_exit(self, time_sec):
        self.mm2 = None
        self.record_state(time_sec)


def analyze_option_v3(packets, q):
    """v3 로직: MM1/MM2 개별 추적 (승격 없음)"""
    if len(packets) < 2:
        return None

    tracker = MMTrackerV3(q)

    # 모든 변화 추적
    all_changes = []
    for i in range(1, len(packets)):
        prev, curr = packets[i-1], packets[i]
        delta_ask = curr['ask'] - prev['ask']
        delta_bid = curr['bid'] - prev['bid']

        if delta_ask != 0 or delta_bid != 0:
            all_changes.append({
                'time_sec': curr['time_sec'],
                'delta_ask': delta_ask,
                'delta_bid': delta_bid,
                'delta_ask_q': delta_ask // q if delta_ask >= 0 else -(abs(delta_ask) // q),
                'delta_bid_q': delta_bid // q if delta_bid >= 0 else -(abs(delta_bid) // q),
                'prev_ask': prev['ask'],
                'prev_bid': prev['bid'],
                'is_q_multiple_ask': abs(delta_ask) >= q and abs(delta_ask) % q == 0,
                'is_q_multiple_bid': abs(delta_bid) >= q and abs(delta_bid) % q == 0,
                'processed': False
            })

    # 메인 처리 루프
    i = 0
    while i < len(all_changes):
        change = all_changes[i]
        if change['processed']:
            i += 1
            continue

        time_sec = change['time_sec']
        delta_ask = change['delta_ask']
        delta_bid = change['delta_bid']
        delta_ask_q = change['delta_ask_q']
        delta_bid_q = change['delta_bid_q']

        # 상태 불일치 체크 (잔량 검증)
        if tracker.mm1 or tracker.mm2:
            expected_mm_ask = 0
            expected_mm_bid = 0
            if tracker.mm1:
                expected_mm_ask += tracker.mm1['ask_q'] * q
                expected_mm_bid += tracker.mm1['bid_q'] * q
            if tracker.mm2:
                expected_mm_ask += tracker.mm2['ask_q'] * q
                expected_mm_bid += tracker.mm2['bid_q'] * q

            if tracker.baseline_ask is not None:
                actual_mm_ask = change['prev_ask'] - tracker.baseline_ask
                actual_mm_bid = change['prev_bid'] - tracker.baseline_bid

                if actual_mm_ask < expected_mm_ask - q//2 or actual_mm_bid < expected_mm_bid - q//2:
                    missing_ask = expected_mm_ask - actual_mm_ask
                    missing_bid = expected_mm_bid - actual_mm_bid

                    if tracker.mm2:
                        mm2_ask = tracker.mm2['ask_q'] * q
                        mm2_bid = tracker.mm2['bid_q'] * q
                        if abs(missing_ask - mm2_ask) < q and abs(missing_bid - mm2_bid) < q:
                            tracker.mm2_exit(time_sec)
                        elif tracker.mm1:
                            mm1_ask = tracker.mm1['ask_q'] * q
                            mm1_bid = tracker.mm1['bid_q'] * q
                            if abs(missing_ask - mm1_ask) < q and abs(missing_bid - mm1_bid) < q:
                                tracker.mm1_exit(time_sec)
                    elif tracker.mm1:
                        mm1_ask = tracker.mm1['ask_q'] * q
                        mm1_bid = tracker.mm1['bid_q'] * q
                        if abs(missing_ask - mm1_ask) < q and abs(missing_bid - mm1_bid) < q:
                            tracker.mm1_exit(time_sec)

        # 100ms 페어링
        paired_event = None
        curr_is_ask_q = change['is_q_multiple_ask'] and delta_ask != 0
        curr_is_bid_q = change['is_q_multiple_bid'] and delta_bid != 0

        if curr_is_ask_q and not curr_is_bid_q:
            direction = '+' if delta_ask > 0 else '-'
            for j in range(i+1, len(all_changes)):
                other = all_changes[j]
                if other['processed']:
                    continue
                if (other['time_sec'] - time_sec) * 1000 > PAIRING_WINDOW_MS:
                    break
                if other['is_q_multiple_bid'] and other['delta_ask'] == 0:
                    other_direction = '+' if other['delta_bid'] > 0 else '-'
                    if direction == other_direction:
                        paired_event = {
                            'ask_q': delta_ask_q,
                            'bid_q': other['delta_bid_q'],
                            'direction': direction,
                            'time_sec': time_sec
                        }
                        other['processed'] = True
                        break

        elif curr_is_bid_q and not curr_is_ask_q:
            direction = '+' if delta_bid > 0 else '-'
            for j in range(i+1, len(all_changes)):
                other = all_changes[j]
                if other['processed']:
                    continue
                if (other['time_sec'] - time_sec) * 1000 > PAIRING_WINDOW_MS:
                    break
                if other['is_q_multiple_ask'] and other['delta_bid'] == 0:
                    other_direction = '+' if other['delta_ask'] > 0 else '-'
                    if direction == other_direction:
                        paired_event = {
                            'ask_q': other['delta_ask_q'],
                            'bid_q': delta_bid_q,
                            'direction': direction,
                            'time_sec': time_sec
                        }
                        other['processed'] = True
                        break

        elif curr_is_ask_q and curr_is_bid_q:
            ask_dir = '+' if delta_ask > 0 else '-'
            bid_dir = '+' if delta_bid > 0 else '-'
            if ask_dir == bid_dir:
                paired_event = {
                    'ask_q': delta_ask_q,
                    'bid_q': delta_bid_q,
                    'direction': ask_dir,
                    'time_sec': time_sec
                }

        # 페어링된 이벤트 처리
        if paired_event:
            change['processed'] = True
            ask_q_val = abs(paired_event['ask_q'])
            bid_q_val = abs(paired_event['bid_q'])
            direction = paired_event['direction']

            if direction == '+':
                if tracker.mm1 is None:
                    if tracker.baseline_ask is None:
                        tracker.baseline_ask = change['prev_ask']
                        tracker.baseline_bid = change['prev_bid']
                    tracker.mm1_enter(ask_q_val, bid_q_val, time_sec)
                elif tracker.mm2 is None:
                    tracker.mm2_enter(ask_q_val, bid_q_val, time_sec)
            else:
                exit_pattern = (ask_q_val, bid_q_val)
                mm1_match = tracker.mm1 and (tracker.mm1['ask_q'], tracker.mm1['bid_q']) == exit_pattern
                mm2_match = tracker.mm2 and (tracker.mm2['ask_q'], tracker.mm2['bid_q']) == exit_pattern

                if mm1_match and mm2_match:
                    tracker.mm2_exit(time_sec)
                elif mm2_match:
                    tracker.mm2_exit(time_sec)
                elif mm1_match:
                    tracker.mm1_exit(time_sec)

        change['processed'] = True
        i += 1

    # 시간 계산
    first_time = packets[0]['time_sec']
    last_time = packets[-1]['time_sec']

    if not tracker.timeline:
        tracker.timeline.append({'time_sec': first_time, 'mm1_present': False, 'mm2_present': False})

    mm1_total_time = 0.0
    mm2_total_time = 0.0
    only_mm1_time = 0.0
    only_mm2_time = 0.0
    both_time = 0.0
    none_time = 0.0

    if first_time > DUTY_START_SEC:
        none_time += first_time - DUTY_START_SEC

    if tracker.timeline[0]['time_sec'] > first_time:
        none_time += tracker.timeline[0]['time_sec'] - first_time

    for idx, state in enumerate(tracker.timeline):
        if idx < len(tracker.timeline) - 1:
            duration = tracker.timeline[idx+1]['time_sec'] - state['time_sec']
        else:
            duration = last_time - state['time_sec']

        if duration < 0:
            continue

        mm1_here = state['mm1_present']
        mm2_here = state['mm2_present']

        if mm1_here:
            mm1_total_time += duration
        if mm2_here:
            mm2_total_time += duration

        if mm1_here and mm2_here:
            both_time += duration
        elif mm1_here and not mm2_here:
            only_mm1_time += duration
        elif not mm1_here and mm2_here:
            only_mm2_time += duration
        else:
            none_time += duration

    if last_time < DUTY_END_SEC and tracker.timeline:
        post_data = DUTY_END_SEC - last_time
        last_state = tracker.timeline[-1]
        mm1_here = last_state['mm1_present']
        mm2_here = last_state['mm2_present']

        if mm1_here:
            mm1_total_time += post_data
        if mm2_here:
            mm2_total_time += post_data

        if mm1_here and mm2_here:
            both_time += post_data
        elif mm1_here and not mm2_here:
            only_mm1_time += post_data
        elif not mm1_here and mm2_here:
            only_mm2_time += post_data
        else:
            none_time += post_data

    return {
        'mm1_rate': mm1_total_time / TOTAL_DUTY_SECONDS * 100,
        'mm2_rate': mm2_total_time / TOTAL_DUTY_SECONDS * 100,
        'only_mm1_rate': only_mm1_time / TOTAL_DUTY_SECONDS * 100,
        'only_mm2_rate': only_mm2_time / TOTAL_DUTY_SECONDS * 100,
        'both_rate': both_time / TOTAL_DUTY_SECONDS * 100,
        'none_rate': none_time / TOTAL_DUTY_SECONDS * 100,
        'packets': len(packets)
    }


def main():
    print("=" * 100)
    print(f"DS투자증권 72개 옵션 전체 분석")
    print(f"분석 날짜: {LOG_DATE}")
    print(f"v3 로직: MM1/MM2 개별 추적, 승격 없음")
    print("=" * 100)

    # 데이터 로드
    duty_data = load_duty_requirements()
    master = load_master()

    all_results = []

    for product_id, stock_info in DS_STOCKS.items():
        stock_name = stock_info['name']
        partner = stock_info['partner']

        print(f"\n{'#'*80}")
        print(f"# {stock_name} (파트너: {partner})")
        print(f"{'#'*80}")

        # Q값 가져오기
        if product_id not in duty_data:
            print(f"  [오류] {product_id} Q값 없음")
            continue
        q_values = duty_data[product_id]['duty_qty']

        # 옵션 분류
        nearest_expiry, call_classified, put_classified = classify_options_for_stock(master, stock_name)

        if not call_classified or not put_classified:
            print(f"  [오류] 옵션 분류 실패")
            continue

        print(f"최근월물: {nearest_expiry}")

        # ISIN 수집
        call_isins = {opt['isin']: (level, opt) for level, opt in call_classified.items()}
        put_isins = {opt['isin']: (level, opt) for level, opt in put_classified.items()}

        # 패킷 로드
        print(f"패킷 로딩 중...")
        call_packets = load_packets(CALL_LOG, set(call_isins.keys()))
        put_packets = load_packets(PUT_LOG, set(put_isins.keys()))

        # CALL 분석
        print(f"\n[CALL 옵션]")
        for level in OPTION_LEVELS:
            opt = call_classified.get(level)
            if not opt:
                all_results.append({
                    'stock': stock_name, 'partner': partner,
                    'type': 'CALL', 'level': level, 'isin': '-', 'strike': '-', 'q': '-',
                    'packets': 0, 'mm1_rate': '-', 'mm2_rate': '-',
                    'only_mm1': '-', 'only_mm2': '-', 'both': '-', 'none': '-'
                })
                continue

            isin = opt['isin']
            strike = opt['strike']
            q = q_values.get(level, 0)
            packets = call_packets.get(isin, [])

            result = analyze_option_v3(packets, q) if packets and q > 0 else None

            if result:
                all_results.append({
                    'stock': stock_name, 'partner': partner,
                    'type': 'CALL', 'level': level, 'isin': isin, 'strike': strike, 'q': q,
                    'packets': result['packets'],
                    'mm1_rate': f"{result['mm1_rate']:.2f}%",
                    'mm2_rate': f"{result['mm2_rate']:.2f}%",
                    'only_mm1': f"{result['only_mm1_rate']:.2f}%",
                    'only_mm2': f"{result['only_mm2_rate']:.2f}%",
                    'both': f"{result['both_rate']:.2f}%",
                    'none': f"{result['none_rate']:.2f}%"
                })
                print(f"  {level}: {strike:,}원 (Q={q}) -> MM1={result['mm1_rate']:.2f}%, MM2={result['mm2_rate']:.2f}%")
            else:
                all_results.append({
                    'stock': stock_name, 'partner': partner,
                    'type': 'CALL', 'level': level, 'isin': isin, 'strike': strike, 'q': q,
                    'packets': len(packets), 'mm1_rate': '-', 'mm2_rate': '-',
                    'only_mm1': '-', 'only_mm2': '-', 'both': '-', 'none': '-'
                })
                print(f"  {level}: {strike:,}원 (Q={q}) -> 분석 불가 (패킷={len(packets)})")

        # PUT 분석
        print(f"\n[PUT 옵션]")
        for level in OPTION_LEVELS:
            opt = put_classified.get(level)
            if not opt:
                all_results.append({
                    'stock': stock_name, 'partner': partner,
                    'type': 'PUT', 'level': level, 'isin': '-', 'strike': '-', 'q': '-',
                    'packets': 0, 'mm1_rate': '-', 'mm2_rate': '-',
                    'only_mm1': '-', 'only_mm2': '-', 'both': '-', 'none': '-'
                })
                continue

            isin = opt['isin']
            strike = opt['strike']
            q = q_values.get(level, 0)
            packets = put_packets.get(isin, [])

            result = analyze_option_v3(packets, q) if packets and q > 0 else None

            if result:
                all_results.append({
                    'stock': stock_name, 'partner': partner,
                    'type': 'PUT', 'level': level, 'isin': isin, 'strike': strike, 'q': q,
                    'packets': result['packets'],
                    'mm1_rate': f"{result['mm1_rate']:.2f}%",
                    'mm2_rate': f"{result['mm2_rate']:.2f}%",
                    'only_mm1': f"{result['only_mm1_rate']:.2f}%",
                    'only_mm2': f"{result['only_mm2_rate']:.2f}%",
                    'both': f"{result['both_rate']:.2f}%",
                    'none': f"{result['none_rate']:.2f}%"
                })
                print(f"  {level}: {strike:,}원 (Q={q}) -> MM1={result['mm1_rate']:.2f}%, MM2={result['mm2_rate']:.2f}%")
            else:
                all_results.append({
                    'stock': stock_name, 'partner': partner,
                    'type': 'PUT', 'level': level, 'isin': isin, 'strike': strike, 'q': q,
                    'packets': len(packets), 'mm1_rate': '-', 'mm2_rate': '-',
                    'only_mm1': '-', 'only_mm2': '-', 'both': '-', 'none': '-'
                })
                print(f"  {level}: {strike:,}원 (Q={q}) -> 분석 불가 (패킷={len(packets)})")

    # MD 파일 생성
    print(f"\n\n결과 파일 생성 중: {OUTPUT_FILE}")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(f"# DS투자증권 MM 의무이행률 분석 결과\n\n")
        f.write(f"- **분석 날짜**: {LOG_DATE}\n")
        f.write(f"- **분석 시간**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"- **의무시간**: 09:05:00 ~ 15:20:00 ({TOTAL_DUTY_SECONDS}초 = {TOTAL_DUTY_SECONDS/60:.0f}분)\n")
        f.write(f"- **분석 로직**: v3 (MM1/MM2 개별 추적, 승격 없음, 패턴 매칭 퇴장 감지)\n\n")

        f.write("---\n\n")

        # 전체 72개 테이블
        f.write("## 전체 72개 옵션 분석 결과\n\n")
        f.write("| # | 기초자산 | 파트너 | 종류 | 레벨 | ISIN | 행사가 | Q | 패킷수 | MM1 의무이행률 | MM2 의무이행률 | MM1만 | MM2만 | 둘 다 | 없음 |\n")
        f.write("|---|----------|--------|------|------|------|--------|---|--------|----------------|----------------|-------|-------|-------|------|\n")

        for idx, r in enumerate(all_results, 1):
            strike_str = f"{r['strike']:,}" if isinstance(r['strike'], int) else r['strike']
            f.write(f"| {idx} | {r['stock']} | {r['partner']} | {r['type']} | {r['level']} | {r['isin']} | {strike_str} | {r['q']} | {r['packets']} | {r['mm1_rate']} | {r['mm2_rate']} | {r['only_mm1']} | {r['only_mm2']} | {r['both']} | {r['none']} |\n")

        f.write("\n---\n\n")

        # 종목별 요약
        f.write("## 종목별 ATM 요약\n\n")
        f.write("| 기초자산 | 파트너 | CALL ATM MM1 | CALL ATM MM2 | PUT ATM MM1 | PUT ATM MM2 |\n")
        f.write("|----------|--------|--------------|--------------|-------------|-------------|\n")

        for product_id, stock_info in DS_STOCKS.items():
            stock_name = stock_info['name']
            partner = stock_info['partner']

            call_atm = next((r for r in all_results if r['stock'] == stock_name and r['type'] == 'CALL' and r['level'] == 'ATM'), None)
            put_atm = next((r for r in all_results if r['stock'] == stock_name and r['type'] == 'PUT' and r['level'] == 'ATM'), None)

            call_mm1 = call_atm['mm1_rate'] if call_atm else '-'
            call_mm2 = call_atm['mm2_rate'] if call_atm else '-'
            put_mm1 = put_atm['mm1_rate'] if put_atm else '-'
            put_mm2 = put_atm['mm2_rate'] if put_atm else '-'

            f.write(f"| {stock_name} | {partner} | {call_mm1} | {call_mm2} | {put_mm1} | {put_mm2} |\n")

        f.write("\n---\n\n")

        # 통계
        f.write("## 통계\n\n")

        total = len(all_results)
        valid = [r for r in all_results if r['mm1_rate'] != '-']

        mm1_above_85 = sum(1 for r in valid if float(r['mm1_rate'].replace('%', '')) >= 85)
        mm2_above_85 = sum(1 for r in valid if float(r['mm2_rate'].replace('%', '')) >= 85)

        f.write(f"- **총 옵션 수**: {total}\n")
        f.write(f"- **분석 성공**: {len(valid)}\n")
        f.write(f"- **MM1 85% 이상**: {mm1_above_85}/{len(valid)} ({mm1_above_85/len(valid)*100:.1f}%)\n")
        f.write(f"- **MM2 85% 이상**: {mm2_above_85}/{len(valid)} ({mm2_above_85/len(valid)*100:.1f}%)\n")

    print(f"\n완료! 결과 파일: {OUTPUT_FILE}")

    # 콘솔 요약
    print("\n" + "=" * 100)
    print("분석 완료 요약")
    print("=" * 100)
    print(f"총 옵션 수: {total}")
    print(f"분석 성공: {len(valid)}")
    print(f"MM1 85% 이상: {mm1_above_85}/{len(valid)} ({mm1_above_85/len(valid)*100:.1f}%)")
    print(f"MM2 85% 이상: {mm2_above_85}/{len(valid)} ({mm2_above_85/len(valid)*100:.1f}%)")


if __name__ == "__main__":
    main()
