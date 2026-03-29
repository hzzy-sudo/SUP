#!/usr/bin/env python
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path


BASE_URL = "https://mix.lottery.sina.com.cn/gateway/index/entry"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Referer": "https://lottery.sina.com.cn/",
}
COMMON_PARAMS = {
    "format": "json",
    "__caller__": "web",
    "__version__": "1",
    "__verno__": "1",
}

RETRYABLE_HTTP_CODES = {429, 456, 500, 502, 503, 504}
DltLottoType = "201"
Pl3LottoType = "202"


def request_mix_json(cat1: str, **params) -> dict:
    query = dict(COMMON_PARAMS)
    query["cat1"] = cat1
    query.update({key: value for key, value in params.items() if value is not None})
    url = f"{BASE_URL}?{urllib.parse.urlencode(query)}"
    request = urllib.request.Request(url, headers=HEADERS)
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.load(response)
            break
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == 3:
                raise
            time.sleep(2 * (attempt + 1))
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt == 3:
                raise
            time.sleep(2 * (attempt + 1))
    else:
        raise RuntimeError(f"{cat1} 请求失败: {last_error}")
    status = payload["result"]["status"]
    if status["code"] != 0:
        raise RuntimeError(f"{cat1} 请求失败: {status['code']} {status['msg']}")
    return payload["result"]


def fetch_game_open_info(lotto_type: str) -> dict:
    return request_mix_json("gameOpenInfo", lottoType=lotto_type)["data"]


def fetch_game_open_list(lotto_type: str, page_size: int = 100) -> list[dict]:
    result: list[dict] = []
    total_page = 1
    for page in range(1, 1000):
        payload = request_mix_json(
            "gameOpenList",
            lottoType=lotto_type,
            paginationType="1",
            page=str(page),
            pageSize=str(page_size),
        )
        result.extend(payload["data"])
        total_page = int(payload["pagination"]["totalPage"])
        if page >= total_page:
            break
    return result


def pad2(value: int) -> str:
    return f"{value:02d}"


def ratio_str(left: int, right: int) -> str:
    return f"{left}:{right}"


def odd_even_counts(nums: list[int]) -> tuple[int, int]:
    odd = sum(1 for num in nums if num % 2 == 1)
    return odd, len(nums) - odd


def big_small_counts(nums: list[int], small_max: int) -> tuple[int, int]:
    small = sum(1 for num in nums if num <= small_max)
    return small, len(nums) - small


def route012_counts(nums: list[int]) -> tuple[int, int, int]:
    counts = [0, 0, 0]
    for num in nums:
        counts[num % 3] += 1
    return counts[0], counts[1], counts[2]


def zone3_counts(front: list[int]) -> tuple[int, int, int]:
    return (
        sum(1 for num in front if 1 <= num <= 12),
        sum(1 for num in front if 13 <= num <= 23),
        sum(1 for num in front if 24 <= num <= 35),
    )


def zone5_counts(front: list[int]) -> tuple[int, int, int, int, int]:
    bins = [(1, 7), (8, 14), (15, 21), (22, 28), (29, 35)]
    return tuple(sum(1 for num in front if start <= num <= end) for start, end in bins)


def consecutive_pairs(nums: list[int]) -> list[str]:
    pairs: list[str] = []
    for left, right in zip(nums, nums[1:]):
        if right - left == 1:
            pairs.append(f"{pad2(left)}-{pad2(right)}")
    return pairs


def repeat_with_previous(nums: list[int], previous_nums: list[int]) -> list[str]:
    return [pad2(num) for num in sorted(set(nums) & set(previous_nums))]


def tail_parity_counts(nums: list[int]) -> tuple[int, int]:
    tails = [num % 10 for num in nums]
    return odd_even_counts(tails)


def tail_big_small_counts(nums: list[int]) -> tuple[int, int]:
    tails = [num % 10 for num in nums]
    return big_small_counts(tails, 4)


def omission_map(history_desc: list[dict], field: str, universe: range) -> dict[str, int]:
    omissions: dict[str, int] = {}
    for target in universe:
        omission = 0
        found = False
        for draw in history_desc:
            if target in draw[field]:
                found = True
                break
            omission += 1
        omissions[pad2(target)] = omission if found else len(history_desc)
    return omissions


def sequence_number_support(values: list[int]) -> dict:
    odd, even = odd_even_counts(values)
    routes = route012_counts(values)
    tail_small = sum(1 for value in values if value % 10 <= 4)
    tail_big = len(values) - tail_small
    return {
        "values": [pad2(value) for value in values],
        "oddEven": {"odd": odd, "even": even, "ratio": ratio_str(odd, even)},
        "route012": {"r0": routes[0], "r1": routes[1], "r2": routes[2], "ratio": f"{routes[0]}:{routes[1]}:{routes[2]}"},
        "tailBigSmall": {"small": tail_small, "big": tail_big, "ratio": ratio_str(tail_small, tail_big)},
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def sequence_value_support(values: list[int]) -> dict:
    routes = route012_counts(values)
    return {
        "values": values,
        "route012": {"r0": routes[0], "r1": routes[1], "r2": routes[2], "ratio": f"{routes[0]}:{routes[1]}:{routes[2]}"},
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def recent_zone_support_dlt(history_desc: list[dict], omission_front: dict[str, int]) -> dict[str, dict]:
    recent = history_desc[:10]
    zones = {
        "zone1": range(1, 13),
        "zone2": range(13, 24),
        "zone3": range(24, 36),
    }
    support: dict[str, dict] = {}
    for label, universe in zones.items():
        universe_keys = [pad2(num) for num in universe]
        issue_hits = []
        appeared: set[int] = set()
        for draw in recent:
            nums = [num for num in draw["front"] if num in universe]
            appeared.update(nums)
            issue_hits.append(
                {
                    "issueNo": draw["issueNo"],
                    "count": len(nums),
                    "nums": [pad2(num) for num in nums],
                }
            )
        zone_omissions = {key: omission_front[key] for key in universe_keys}
        support[label] = {
            "range": f"{pad2(min(universe))}-{pad2(max(universe))}",
            "issueHits": issue_hits,
            "hitCountTotal": sum(item["count"] for item in issue_hits),
            "appeared": [pad2(num) for num in sorted(appeared)],
            "missing": [key for key in universe_keys if int(key) not in appeared],
            "omissions": zone_omissions,
            "topMissing": sorted(zone_omissions.items(), key=lambda item: (-item[1], item[0]))[:5],
        }
    return support


def normalize_dlt_history(raw_history: list[dict]) -> list[dict]:
    history: list[dict] = []
    for index, item in enumerate(raw_history):
        front = [int(value) for value in item["redResults"]]
        back = [int(value) for value in item["blueResults"]]
        previous_front = [int(value) for value in raw_history[index + 1]["redResults"]] if index + 1 < len(raw_history) else []
        odd, even = odd_even_counts(front)
        small, big = big_small_counts(front, 17)
        z3 = zone3_counts(front)
        z5 = zone5_counts(front)
        back_odd, back_even = odd_even_counts(back)
        back_small, back_big = big_small_counts(back, 6)
        record = {
            "issueNo": item["issueNo"],
            "openTime": item["openTime"],
            "front": front,
            "back": back,
            "frontDisplay": " ".join(pad2(num) for num in front),
            "backDisplay": " ".join(pad2(num) for num in back),
            "frontSum": sum(front),
            "frontSpan": max(front) - min(front),
            "frontOddEven": {"odd": odd, "even": even, "ratio": ratio_str(odd, even)},
            "frontBigSmall": {"small": small, "big": big, "ratio": ratio_str(small, big)},
            "frontRoute012": {
                "r0": route012_counts(front)[0],
                "r1": route012_counts(front)[1],
                "r2": route012_counts(front)[2],
                "ratio": ratio_str(route012_counts(front)[0], route012_counts(front)[1]) + ":" + str(route012_counts(front)[2]),
            },
            "zone3": {"values": list(z3), "ratio": ":".join(str(value) for value in z3)},
            "zone5": {"values": list(z5), "ratio": ":".join(str(value) for value in z5)},
            "head": front[0],
            "tail": front[-1],
            "frontConsecutivePairs": consecutive_pairs(front),
            "frontRepeatWithPrevious": repeat_with_previous(front, previous_front),
            "backSum": sum(back),
            "backSpan": max(back) - min(back),
            "backOddEven": {"odd": back_odd, "even": back_even, "ratio": ratio_str(back_odd, back_even)},
            "backBigSmall": {"small": back_small, "big": back_big, "ratio": ratio_str(back_small, back_big)},
            "backRoute012": {
                "r0": route012_counts(back)[0],
                "r1": route012_counts(back)[1],
                "r2": route012_counts(back)[2],
                "ratio": ratio_str(route012_counts(back)[0], route012_counts(back)[1]) + ":" + str(route012_counts(back)[2]),
            },
            "frontTailOddEven": {
                "odd": tail_parity_counts(front)[0],
                "even": tail_parity_counts(front)[1],
                "ratio": ratio_str(tail_parity_counts(front)[0], tail_parity_counts(front)[1]),
            },
            "frontTailRoute012": {
                "r0": route012_counts([num % 10 for num in front])[0],
                "r1": route012_counts([num % 10 for num in front])[1],
                "r2": route012_counts([num % 10 for num in front])[2],
                "ratio": ratio_str(route012_counts([num % 10 for num in front])[0], route012_counts([num % 10 for num in front])[1]) + ":" + str(route012_counts([num % 10 for num in front])[2]),
            },
            "frontTailBigSmall": {
                "small": tail_big_small_counts(front)[0],
                "big": tail_big_small_counts(front)[1],
                "ratio": ratio_str(tail_big_small_counts(front)[0], tail_big_small_counts(front)[1]),
            },
        }
        history.append(record)
    return history


def summarize_recent_dlt(history_desc: list[dict]) -> dict:
    recent = history_desc[:10]
    front_odd = sum(item["frontOddEven"]["odd"] for item in recent)
    front_even = sum(item["frontOddEven"]["even"] for item in recent)
    front_small = sum(item["frontBigSmall"]["small"] for item in recent)
    front_big = sum(item["frontBigSmall"]["big"] for item in recent)
    zone3 = [sum(item["zone3"]["values"][idx] for item in recent) for idx in range(3)]
    zone5 = [sum(item["zone5"]["values"][idx] for item in recent) for idx in range(5)]
    back_odd = sum(item["backOddEven"]["odd"] for item in recent)
    back_even = sum(item["backOddEven"]["even"] for item in recent)
    back_small = sum(item["backBigSmall"]["small"] for item in recent)
    back_big = sum(item["backBigSmall"]["big"] for item in recent)
    heads = [item["head"] for item in recent]
    tails = [item["tail"] for item in recent]
    front_sums = [item["frontSum"] for item in recent]
    front_spans = [item["frontSpan"] for item in recent]
    back_sums = [item["backSum"] for item in recent]
    return {
        "frontOddEven": {"odd": front_odd, "even": front_even, "ratio": ratio_str(front_odd, front_even)},
        "frontBigSmall": {"small": front_small, "big": front_big, "ratio": ratio_str(front_small, front_big)},
        "zone3": {"values": zone3, "ratio": ":".join(str(value) for value in zone3)},
        "zone5": {"values": zone5, "ratio": ":".join(str(value) for value in zone5)},
        "heads": [pad2(value) for value in heads],
        "tails": [pad2(value) for value in tails],
        "headSupport": sequence_number_support(heads),
        "tailSupport": sequence_number_support(tails),
        "frontSums": front_sums,
        "frontSumSupport": sequence_value_support(front_sums),
        "frontSpans": front_spans,
        "frontSpanSupport": sequence_value_support(front_spans),
        "backSums": back_sums,
        "backSumSupport": sequence_value_support(back_sums),
        "backOddEven": {"odd": back_odd, "even": back_even, "ratio": ratio_str(back_odd, back_even)},
        "backBigSmall": {"small": back_small, "big": back_big, "ratio": ratio_str(back_small, back_big)},
    }


def same_issue_support_dlt(history_desc: list[dict], next_issue_no: str) -> dict:
    suffix = next_issue_no[-3:]
    same_records = [item for item in history_desc if item["issueNo"].endswith(suffix)]
    tail_counter = Counter()
    tail_route_counter = Counter()
    tail_parity_counter = Counter()
    tail_size_counter = Counter()
    zone5_totals = [0, 0, 0, 0, 0]
    back_tail_counter = Counter()
    for record in same_records:
        for value in record["front"]:
            tail = value % 10
            tail_counter[pad2(tail)] += 1
            tail_route_counter[f"r{tail % 3}"] += 1
            tail_parity_counter["odd" if tail % 2 else "even"] += 1
            tail_size_counter["small" if tail <= 4 else "big"] += 1
        for idx, count in enumerate(record["zone5"]["values"]):
            zone5_totals[idx] += count
        for value in record["back"]:
            back_tail_counter[pad2(value % 10)] += 1
    records = [
        {
            "issueNo": record["issueNo"],
            "openTime": record["openTime"],
            "frontDisplay": record["frontDisplay"],
            "backDisplay": record["backDisplay"],
        }
        for record in same_records
    ]
    return {
        "targetIssueNo": next_issue_no,
        "targetSuffix": suffix,
        "recordCount": len(records),
        "records": records,
        "zone5Totals": {"values": zone5_totals, "ratio": ":".join(str(value) for value in zone5_totals)},
        "frontTailDigitCounts": {key: tail_counter.get(key, 0) for key in [pad2(num) for num in range(10)]},
        "frontTailOddEven": {
            "odd": tail_parity_counter["odd"],
            "even": tail_parity_counter["even"],
            "ratio": ratio_str(tail_parity_counter["odd"], tail_parity_counter["even"]),
        },
        "frontTailRoute012": {
            "r0": tail_route_counter["r0"],
            "r1": tail_route_counter["r1"],
            "r2": tail_route_counter["r2"],
            "ratio": f"{tail_route_counter['r0']}:{tail_route_counter['r1']}:{tail_route_counter['r2']}",
        },
        "frontTailBigSmall": {
            "small": tail_size_counter["small"],
            "big": tail_size_counter["big"],
            "ratio": ratio_str(tail_size_counter["small"], tail_size_counter["big"]),
        },
        "backTailCounts": {key: back_tail_counter.get(key, 0) for key in [pad2(num) for num in range(10)]},
    }


def ratio_follow_support_dlt(history_desc: list[dict], field: str, current_ratio: str, recent_n: int = 5) -> dict:
    chronological = list(reversed(history_desc))
    follow_pairs: list[tuple[dict, dict]] = []
    for source, nxt in zip(chronological, chronological[1:]):
        if source[field]["ratio"] == current_ratio:
            follow_pairs.append((source, nxt))

    recent_pairs = follow_pairs[-recent_n:]
    ratio_left_key, ratio_right_key = ("small", "big") if field == "frontBigSmall" else ("odd", "even")
    next_left_total = sum(pair[1][field][ratio_left_key] for pair in recent_pairs)
    next_right_total = sum(pair[1][field][ratio_right_key] for pair in recent_pairs)
    next_front_counter = Counter()
    for _, nxt in recent_pairs:
        for value in nxt["front"]:
            next_front_counter[pad2(value)] += 1

    return {
        "currentRatio": current_ratio,
        "occurrenceCount": sum(1 for item in history_desc if item[field]["ratio"] == current_ratio),
        "recentNextDrawCount": len(recent_pairs),
        "recentNextDraws": [
            {
                "sourceIssueNo": source["issueNo"],
                "nextIssueNo": nxt["issueNo"],
                "nextOpenTime": nxt["openTime"],
                "frontDisplay": nxt["frontDisplay"],
                "backDisplay": nxt["backDisplay"],
                "nextRatio": nxt[field]["ratio"],
            }
            for source, nxt in recent_pairs
        ],
        "nextRatioTotals": {
            ratio_left_key: next_left_total,
            ratio_right_key: next_right_total,
            "ratio": ratio_str(next_left_total, next_right_total),
        },
        "nextFrontCounts": {pad2(num): next_front_counter.get(pad2(num), 0) for num in range(1, 36)},
        "nextFrontHot": [
            {"num": key, "count": value}
            for key, value in sorted(next_front_counter.items(), key=lambda item: (-item[1], int(item[0])))[:10]
        ],
    }


def back_pair_follow_support_dlt(history_desc: list[dict], current_back: list[int], recent_n: int = 5) -> dict:
    chronological = list(reversed(history_desc))
    current_pair = "+".join(pad2(num) for num in current_back)
    follow_pairs: list[tuple[dict, dict]] = []
    for source, nxt in zip(chronological, chronological[1:]):
        if source["back"] == current_back:
            follow_pairs.append((source, nxt))

    recent_pairs = follow_pairs[-recent_n:]
    next_back_counter = Counter()
    for _, nxt in recent_pairs:
        for value in nxt["back"]:
            next_back_counter[pad2(value)] += 1

    return {
        "currentPair": current_pair,
        "occurrenceCount": sum(1 for item in history_desc if item["back"] == current_back),
        "recentNextDrawCount": len(recent_pairs),
        "recentNextDraws": [
            {
                "sourceIssueNo": source["issueNo"],
                "nextIssueNo": nxt["issueNo"],
                "nextOpenTime": nxt["openTime"],
                "frontDisplay": nxt["frontDisplay"],
                "backDisplay": nxt["backDisplay"],
            }
            for source, nxt in recent_pairs
        ],
        "nextBackCounts": {pad2(num): next_back_counter.get(pad2(num), 0) for num in range(1, 13)},
        "nextBackHot": [
            {"num": key, "count": value}
            for key, value in sorted(next_back_counter.items(), key=lambda item: (-item[1], int(item[0])))[:6]
        ],
    }


def jiang_chuan_support_dlt(history_desc: list[dict]) -> dict:
    latest = history_desc[0]
    return {
        "referenceIssueNo": latest["issueNo"],
        "frontSizeFollow": ratio_follow_support_dlt(history_desc, "frontBigSmall", latest["frontBigSmall"]["ratio"]),
        "frontOddEvenFollow": ratio_follow_support_dlt(history_desc, "frontOddEven", latest["frontOddEven"]["ratio"]),
        "backPairFollow": back_pair_follow_support_dlt(history_desc, latest["back"]),
    }


def build_transition_map_dlt(history_desc: list[dict], window_size: int = 460) -> dict:
    usable = history_desc[: window_size + 1]
    chronological = list(reversed(usable))
    front_counts = {pad2(num): Counter() for num in range(1, 36)}
    back_counts = {pad2(num): Counter() for num in range(1, 13)}
    for current, nxt in zip(chronological, chronological[1:]):
        for num in current["front"]:
            key = pad2(num)
            for out in nxt["front"]:
                front_counts[key][pad2(out)] += 1
        for num in current["back"]:
            key = pad2(num)
            for out in nxt["back"]:
                back_counts[key][pad2(out)] += 1

    def pack(counter_map: dict[str, Counter], universe: range, top_n: int, bottom_n: int) -> dict[str, dict]:
        packed: dict[str, dict] = {}
        all_keys = [pad2(num) for num in universe]
        for source in all_keys:
            counter = counter_map[source]
            hot = sorted(all_keys, key=lambda key: (-counter.get(key, 0), int(key)))[:top_n]
            cold = sorted(all_keys, key=lambda key: (counter.get(key, 0), int(key)))[:bottom_n]
            packed[source] = {
                "hot": [{"num": key, "count": counter.get(key, 0)} for key in hot],
                "cold": [{"num": key, "count": counter.get(key, 0)} for key in cold],
                "counts": {key: counter.get(key, 0) for key in all_keys},
            }
        return packed

    return {
        "windowDrawCount": min(window_size, max(len(history_desc) - 1, 0)),
        "front": pack(front_counts, range(1, 36), 5, 5),
        "back": pack(back_counts, range(1, 13), 3, 3),
    }


def latest_transition_snapshot_dlt(latest_record: dict, transition_support: dict) -> dict:
    return {
        "latestFront": [pad2(num) for num in latest_record["front"]],
        "latestBack": [pad2(num) for num in latest_record["back"]],
        "front": {pad2(num): transition_support["front"][pad2(num)] for num in latest_record["front"]},
        "back": {pad2(num): transition_support["back"][pad2(num)] for num in latest_record["back"]},
    }


def build_dlt_support(as_of_date: str) -> tuple[dict, dict, str]:
    raw_history = fetch_game_open_list(DltLottoType)
    open_info = fetch_game_open_info(DltLottoType)
    history_desc = normalize_dlt_history(raw_history)
    latest = history_desc[0]
    next_issue_no = open_info["nextIssueNo"]
    omission_front = omission_map(history_desc, "front", range(1, 36))
    omission_back = omission_map(history_desc, "back", range(1, 13))
    recent_zone_support = recent_zone_support_dlt(history_desc, omission_front)
    jiang_support = jiang_chuan_support_dlt(history_desc)
    transition_support = build_transition_map_dlt(history_desc)
    support = {
        "asOfDate": as_of_date,
        "lottoType": "dlt",
        "historyCount": len(history_desc),
        "latestIssueNo": latest["issueNo"],
        "latestOpenTime": latest["openTime"],
        "nextIssueNo": next_issue_no,
        "history": history_desc,
        "recent10": history_desc[:10],
        "recent10Summary": summarize_recent_dlt(history_desc),
        "recent10ZoneSupport": recent_zone_support,
        "jiangChuanPatternSupport": jiang_support,
        "sameIssueSupport": same_issue_support_dlt(history_desc, next_issue_no),
        "omissionSupport": {
            "front": omission_front,
            "back": omission_back,
            "frontTopMissing": sorted(omission_front.items(), key=lambda item: (-item[1], item[0]))[:10],
            "backTopMissing": sorted(omission_back.items(), key=lambda item: (-item[1], item[0]))[:12],
            "backGroups": {
                "hot": [key for key, value in omission_back.items() if value <= 3],
                "warm": [key for key, value in omission_back.items() if 4 <= value <= 9],
                "cold": [key for key, value in omission_back.items() if value >= 10],
            },
        },
        "transition460": transition_support,
        "currentTransitionSnapshot": latest_transition_snapshot_dlt(latest, transition_support),
    }
    history_payload = {
        "asOfDate": as_of_date,
        "lottoType": "dlt",
        "latestIssueNo": latest["issueNo"],
        "nextIssueNo": next_issue_no,
        "historyCount": len(history_desc),
        "draws": history_desc,
    }
    support_md = render_dlt_support_md(support)
    return history_payload, support, support_md


def render_dlt_support_md(support: dict) -> str:
    recent = support["recent10"]
    same_issue = support["sameIssueSupport"]
    omissions = support["omissionSupport"]
    snapshot = support["currentTransitionSnapshot"]
    jiang_support = support["jiangChuanPatternSupport"]
    head_support = support["recent10Summary"]["headSupport"]
    tail_support = support["recent10Summary"]["tailSupport"]
    zone_support = support["recent10ZoneSupport"]
    jiang_size_hot = " ".join(
        f"{item['num']}({item['count']})" for item in jiang_support["frontSizeFollow"]["nextFrontHot"]
    )
    jiang_odd_hot = " ".join(
        f"{item['num']}({item['count']})" for item in jiang_support["frontOddEvenFollow"]["nextFrontHot"]
    )
    jiang_back_hot = " ".join(
        f"{item['num']}({item['count']})" for item in jiang_support["backPairFollow"]["nextBackHot"]
    )
    lines = [
        "# 大乐透共享数据支撑",
        "",
        "> 本文件为动态生成快照。用于支撑大乐透专家 skill 的最近10期、历史同期、遗漏和 460 期条件转移分析。",
        "",
        "## 刷新口径",
        "",
        f"- 统计时间：{support['asOfDate']}",
        f"- 最新开奖期号：{support['latestIssueNo']}",
        f"- 下一期目标期号：{support['nextIssueNo']}",
        f"- 可访问历史期开奖：{support['historyCount']} 期",
        "",
        "## 最近10期原始开奖与指标",
        "",
        "| 期号 | 开奖时间 | 前区 | 后区 | 和值 | 跨度 | 奇偶比 | 大小比 | 三区比 | 五区比 | 龙头 | 凤尾 | 后区和值 | 后区奇偶比 | 后区大小比 | 连号 | 重号 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in recent:
        lines.append(
            f"| {item['issueNo']} | {item['openTime']} | {item['frontDisplay']} | {item['backDisplay']} | {item['frontSum']} | {item['frontSpan']} | "
            f"{item['frontOddEven']['ratio']} | {item['frontBigSmall']['ratio']} | {item['zone3']['ratio']} | {item['zone5']['ratio']} | "
            f"{pad2(item['head'])} | {pad2(item['tail'])} | {item['backSum']} | {item['backOddEven']['ratio']} | {item['backBigSmall']['ratio']} | "
            f"{'、'.join(item['frontConsecutivePairs']) if item['frontConsecutivePairs'] else '无'} | "
            f"{'、'.join(item['frontRepeatWithPrevious']) if item['frontRepeatWithPrevious'] else '无'} |"
        )
    lines.extend(
        [
            "",
            "## 最近10期累计统计",
            "",
            f"- 前区奇偶累计：{support['recent10Summary']['frontOddEven']['ratio']}",
            f"- 前区大小累计：{support['recent10Summary']['frontBigSmall']['ratio']}",
            f"- 前区三区累计：{support['recent10Summary']['zone3']['ratio']}",
            f"- 前区五区累计：{support['recent10Summary']['zone5']['ratio']}",
            f"- 龙头序列：{' '.join(support['recent10Summary']['heads'])}",
            f"- 凤尾序列：{' '.join(support['recent10Summary']['tails'])}",
            f"- 前区跨度序列：{' '.join(str(value) for value in support['recent10Summary']['frontSpans'])}",
            f"- 前区和值序列：{' '.join(str(value) for value in support['recent10Summary']['frontSums'])}",
            f"- 后区和值序列：{' '.join(str(value) for value in support['recent10Summary']['backSums'])}",
            f"- 后区奇偶累计：{support['recent10Summary']['backOddEven']['ratio']}",
            f"- 后区大小累计：{support['recent10Summary']['backBigSmall']['ratio']}",
            "",
            "## 龙头凤尾近10期支撑",
            "",
            f"- 龙头序列：{' '.join(head_support['values'])}",
            f"- 龙头奇偶：{head_support['oddEven']['ratio']}，012路：{head_support['route012']['ratio']}，尾数大小：{head_support['tailBigSmall']['ratio']}",
            f"- 凤尾序列：{' '.join(tail_support['values'])}",
            f"- 凤尾奇偶：{tail_support['oddEven']['ratio']}，012路：{tail_support['route012']['ratio']}，尾数大小：{tail_support['tailBigSmall']['ratio']}",
            "",
            "## 前三区近10期分层支撑",
            "",
            f"- 前一区 01-12 开出总量：{zone_support['zone1']['hitCountTotal']}；出现号码：{' '.join(zone_support['zone1']['appeared']) if zone_support['zone1']['appeared'] else '无'}",
            f"- 前一区 01-12 近10期未开号：{' '.join(zone_support['zone1']['missing']) if zone_support['zone1']['missing'] else '无'}",
            f"- 前一区 01-12 高遗漏前5：{' '.join(f'{key}:{value}' for key, value in zone_support['zone1']['topMissing'])}",
            f"- 前二区 13-23 开出总量：{zone_support['zone2']['hitCountTotal']}；出现号码：{' '.join(zone_support['zone2']['appeared']) if zone_support['zone2']['appeared'] else '无'}",
            f"- 前二区 13-23 近10期未开号：{' '.join(zone_support['zone2']['missing']) if zone_support['zone2']['missing'] else '无'}",
            f"- 前二区 13-23 高遗漏前5：{' '.join(f'{key}:{value}' for key, value in zone_support['zone2']['topMissing'])}",
            f"- 前三区 24-35 开出总量：{zone_support['zone3']['hitCountTotal']}；出现号码：{' '.join(zone_support['zone3']['appeared']) if zone_support['zone3']['appeared'] else '无'}",
            f"- 前三区 24-35 近10期未开号：{' '.join(zone_support['zone3']['missing']) if zone_support['zone3']['missing'] else '无'}",
            f"- 前三区 24-35 高遗漏前5：{' '.join(f'{key}:{value}' for key, value in zone_support['zone3']['topMissing'])}",
            "",
            "## 江川形态跟随支撑",
            "",
            f"- 参考上期：{jiang_support['referenceIssueNo']}",
            f"- 当前前区大小比：{jiang_support['frontSizeFollow']['currentRatio']}，历史出现 {jiang_support['frontSizeFollow']['occurrenceCount']} 次，最近5次同形态后的下期大小累计：{jiang_support['frontSizeFollow']['nextRatioTotals']['ratio']}",
            f"- 当前前区奇偶比：{jiang_support['frontOddEvenFollow']['currentRatio']}，历史出现 {jiang_support['frontOddEvenFollow']['occurrenceCount']} 次，最近5次同形态后的下期奇偶累计：{jiang_support['frontOddEvenFollow']['nextRatioTotals']['ratio']}",
            f"- 当前后区组合：{jiang_support['backPairFollow']['currentPair']}，历史出现 {jiang_support['backPairFollow']['occurrenceCount']} 次",
            f"- 大小形态后的前区热码：{jiang_size_hot}",
            f"- 奇偶形态后的前区热码：{jiang_odd_hot}",
            f"- 后区组合后的后区热码：{jiang_back_hot}",
            "",
            "## 历史同期开奖支撑",
            "",
            f"- 目标期号：{same_issue['targetIssueNo']}",
            f"- 同期样本条数：{same_issue['recordCount']}",
            f"- 同期五区累计：{same_issue['zone5Totals']['ratio']}",
            f"- 同期前区尾数奇偶：{same_issue['frontTailOddEven']['ratio']}",
            f"- 同期前区尾数012路：{same_issue['frontTailRoute012']['ratio']}",
            f"- 同期前区尾数大小：{same_issue['frontTailBigSmall']['ratio']}",
            f"- 同期后区尾数频次：{' '.join(f'{key}:{value}' for key, value in same_issue['backTailCounts'].items())}",
            "",
            "| 同期历史期号 | 开奖时间 | 前区 | 后区 |",
            "| --- | --- | --- | --- |",
        ]
    )
    for record in same_issue["records"]:
        lines.append(f"| {record['issueNo']} | {record['openTime']} | {record['frontDisplay']} | {record['backDisplay']} |")
    lines.extend(
        [
            "",
            "## 大乐透遗漏支撑",
            "",
            f"- 前区高遗漏前10：{' '.join(f'{key}:{value}' for key, value in omissions['frontTopMissing'])}",
            f"- 后区高遗漏：{' '.join(f'{key}:{value}' for key, value in omissions['backTopMissing'])}",
            f"- 后区热码(遗漏0-3)：{' '.join(omissions['backGroups']['hot'])}",
            f"- 后区温码(遗漏4-9)：{' '.join(omissions['backGroups']['warm'])}",
            f"- 后区冷码(遗漏10+)：{' '.join(omissions['backGroups']['cold'])}",
            "",
            "## 当前460期条件转移快照",
            "",
            f"- 最新前区：{' '.join(snapshot['latestFront'])}",
        ]
    )
    for key in snapshot["latestFront"]:
        hot = " ".join(f"{item['num']}({item['count']})" for item in snapshot["front"][key]["hot"])
        cold = " ".join(f"{item['num']}({item['count']})" for item in snapshot["front"][key]["cold"])
        lines.append(f"- 当 {key} 出现后，前区热5码：{hot}；冷5码：{cold}")
    lines.append(f"- 最新后区：{' '.join(snapshot['latestBack'])}")
    for key in snapshot["latestBack"]:
        hot = " ".join(f"{item['num']}({item['count']})" for item in snapshot["back"][key]["hot"])
        cold = " ".join(f"{item['num']}({item['count']})" for item in snapshot["back"][key]["cold"])
        lines.append(f"- 当 {key} 出现后，后区热3码：{hot}；冷3码：{cold}")
    lines.extend(
        [
            "",
            "## 说明",
            "",
            "- 若需要完整号码级频次，请读取 `dlt-support.json` 中的 `recent10Summary`、`recent10ZoneSupport`、`jiangChuanPatternSupport`、`transition460`、`sameIssueSupport` 和 `omissionSupport`。",
            "- 若需要全文历史号库，请读取 `dlt-history.json`。",
        ]
    )
    return "\n".join(lines) + "\n"


def prime_composite(value: int) -> str:
    return "prime" if value in {2, 3, 5, 7} else "composite"


def shape_pl3(digits: list[int]) -> str:
    distinct = len(set(digits))
    if distinct == 1:
        return "豹子"
    if distinct == 2:
        return "组三"
    return "组六"


def normalize_pl3_history(raw_history: list[dict]) -> list[dict]:
    history: list[dict] = []
    for item in raw_history:
        digits = [int(value) for value in item["openResults"]]
        odd, even = odd_even_counts(digits)
        small, big = big_small_counts(digits, 4)
        route = route012_counts(digits)
        record = {
            "issueNo": item["issueNo"],
            "openTime": item["openTime"],
            "digits": digits,
            "display": "".join(str(value) for value in digits),
            "shape": shape_pl3(digits),
            "sum": sum(digits),
            "span": max(digits) - min(digits),
            "oddEven": {"odd": odd, "even": even, "ratio": ratio_str(odd, even)},
            "bigSmall": {"small": small, "big": big, "ratio": ratio_str(small, big)},
            "route012": {"r0": route[0], "r1": route[1], "r2": route[2], "ratio": f"{route[0]}:{route[1]}:{route[2]}"},
            "positions": {
                "hundreds": digits[0],
                "tens": digits[1],
                "units": digits[2],
            },
        }
        history.append(record)
    return history


def position_stats(records: list[dict], position_key: str) -> dict:
    digits = [record["positions"][position_key] for record in records]
    routes = route012_counts(digits)
    digit_counts = Counter(digits)
    stats = {
        "digitCounts": {str(num): digit_counts.get(num, 0) for num in range(10)},
        "route012": {"r0": routes[0], "r1": routes[1], "r2": routes[2], "ratio": f"{routes[0]}:{routes[1]}:{routes[2]}"},
    }
    if position_key == "hundreds":
        small, big = big_small_counts(digits, 4)
        stats["bigSmall"] = {"small": small, "big": big, "ratio": ratio_str(small, big)}
    if position_key == "tens":
        odd, even = odd_even_counts(digits)
        stats["oddEven"] = {"odd": odd, "even": even, "ratio": ratio_str(odd, even)}
    if position_key == "units":
        prime = sum(1 for value in digits if prime_composite(value) == "prime")
        composite = len(digits) - prime
        stats["primeComposite"] = {"prime": prime, "composite": composite, "ratio": ratio_str(prime, composite)}
    return stats


def build_pl3_support(as_of_date: str) -> tuple[dict, dict, str]:
    raw_history = fetch_game_open_list(Pl3LottoType)
    open_info = fetch_game_open_info(Pl3LottoType)
    history_desc = normalize_pl3_history(raw_history)
    latest = history_desc[0]
    recent20 = history_desc[:20]
    recent18 = history_desc[:18]
    support = {
        "asOfDate": as_of_date,
        "lottoType": "pl3",
        "historyCount": len(history_desc),
        "latestIssueNo": latest["issueNo"],
        "latestOpenTime": latest["openTime"],
        "nextIssueNo": open_info["nextIssueNo"],
        "history": history_desc,
        "recent20": recent20,
        "recent18": recent18,
        "positionSupport": {
            "recent20": {
                "hundreds": position_stats(recent20, "hundreds"),
                "tens": position_stats(recent20, "tens"),
                "units": position_stats(recent20, "units"),
            },
            "recent18": {
                "hundreds": position_stats(recent18, "hundreds"),
                "tens": position_stats(recent18, "tens"),
                "units": position_stats(recent18, "units"),
            },
        },
    }
    history_payload = {
        "asOfDate": as_of_date,
        "lottoType": "pl3",
        "latestIssueNo": latest["issueNo"],
        "nextIssueNo": open_info["nextIssueNo"],
        "historyCount": len(history_desc),
        "draws": history_desc,
    }
    support_md = render_pl3_support_md(support)
    return history_payload, support, support_md


def render_pl3_support_md(support: dict) -> str:
    lines = [
        "# 排列三共享数据支撑",
        "",
        "> 本文件为动态生成快照。用于支撑排列三位置拆解类 skill 的最近18/20期统计。",
        "",
        "## 刷新口径",
        "",
        f"- 统计时间：{support['asOfDate']}",
        f"- 最新开奖期号：{support['latestIssueNo']}",
        f"- 下一期目标期号：{support['nextIssueNo']}",
        f"- 可访问历史期开奖：{support['historyCount']} 期",
        "",
        "## 最近20期原始开奖与指标",
        "",
        "| 期号 | 开奖时间 | 号码 | 形态 | 和值 | 跨度 | 奇偶比 | 大小比 | 012路 | 百位 | 十位 | 个位 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in support["recent20"]:
        lines.append(
            f"| {item['issueNo']} | {item['openTime']} | {item['display']} | {item['shape']} | {item['sum']} | {item['span']} | "
            f"{item['oddEven']['ratio']} | {item['bigSmall']['ratio']} | {item['route012']['ratio']} | "
            f"{item['positions']['hundreds']} | {item['positions']['tens']} | {item['positions']['units']} |"
        )
    lines.extend(["", "## 位置统计支撑", ""])
    for window in ["recent20", "recent18"]:
        lines.append(f"### {window}")
        lines.append("")
        hundred = support["positionSupport"][window]["hundreds"]
        tens = support["positionSupport"][window]["tens"]
        units = support["positionSupport"][window]["units"]
        lines.append(f"- 百位大小：{hundred['bigSmall']['ratio']}，百位012路：{hundred['route012']['ratio']}")
        lines.append(f"- 百位数字频次：{' '.join(f'{key}:{value}' for key, value in hundred['digitCounts'].items())}")
        lines.append(f"- 十位奇偶：{tens['oddEven']['ratio']}，十位012路：{tens['route012']['ratio']}")
        lines.append(f"- 十位数字频次：{' '.join(f'{key}:{value}' for key, value in tens['digitCounts'].items())}")
        lines.append(f"- 个位质合：{units['primeComposite']['ratio']}，个位012路：{units['route012']['ratio']}")
        lines.append(f"- 个位数字频次：{' '.join(f'{key}:{value}' for key, value in units['digitCounts'].items())}")
        lines.append("")
    lines.extend(
        [
            "## 说明",
            "",
            "- 江川 skill 主要使用最近18至20期的位置统计，因此 `pl3-support.json` 已直接给出百位、十位、个位两套窗口的拆解结果。",
            "- 若需要完整历史号库，请读取 `pl3-history.json`。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_support_files(root: Path, prefix: str, history_payload: dict, support_payload: dict, support_md: str) -> None:
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / f"{prefix}-history.json").write_text(
        json.dumps(history_payload, ensure_ascii=False, indent=2),
        encoding="utf-8-sig",
    )
    (data_dir / f"{prefix}-support.json").write_text(
        json.dumps(support_payload, ensure_ascii=False, indent=2),
        encoding="utf-8-sig",
    )
    (data_dir / f"{prefix}-support.md").write_text(support_md, encoding="utf-8-sig")


def refresh_dlt_support(root: Path, as_of_date: str) -> None:
    history_payload, support_payload, support_md = build_dlt_support(as_of_date)
    write_support_files(root, "dlt", history_payload, support_payload, support_md)


def refresh_pl3_support(root: Path, as_of_date: str) -> None:
    history_payload, support_payload, support_md = build_pl3_support(as_of_date)
    write_support_files(root, "pl3", history_payload, support_payload, support_md)
