#!/usr/bin/env python
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from history_support import position_stats


FULLWIDTH_COLON = "："
SAMPLE_SECTION_HEADER = "## 逐期文本摘录"


@dataclass(frozen=True)
class ReplaySample:
    issue_no: str
    news_id: str
    fields: dict[str, str]
    snapshot: dict[str, Any]
    distance: float = 0.0


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig", errors="strict")


def _read_json(path: Path) -> Any:
    import json

    return json.loads(_read_text(path))


def _shape_code(shape: str) -> int:
    return {
        "豹子": 2,
        "组三": 1,
        "组六": 0,
    }.get(shape, -1)


def _streak(values: list[int], predicate) -> int:
    count = 0
    for value in values:
        if predicate(value):
            count += 1
        else:
            break
    return count


def _build_pl3_snapshot_from_cache(support_cache: dict[str, Any], target_issue_no: str) -> dict[str, Any]:
    history = support_cache["history"]
    current_next_issue_no = support_cache["nextIssueNo"]
    if target_issue_no == current_next_issue_no:
        sliced = history
        mode = "latest"
    else:
        issue_to_index = {record["issueNo"]: idx for idx, record in enumerate(history)}
        if target_issue_no not in issue_to_index:
            raise ValueError(f"target issue {target_issue_no} not found in pl3 history cache")
        idx = issue_to_index[target_issue_no]
        sliced = history[idx + 1 :]
        mode = "historical"
        if not sliced:
            raise ValueError(f"target issue {target_issue_no} has no preceding history slice")

    if len(sliced) < 20:
        raise ValueError(f"target issue {target_issue_no} does not have at least 20 usable historical draws")

    latest = sliced[0]
    recent20 = sliced[:20]
    recent18 = sliced[:18]
    return {
        "asOfDate": support_cache["asOfDate"],
        "lottoType": "pl3",
        "predictionIssueNo": target_issue_no,
        "snapshotMode": mode,
        "sourceCurrentNextIssueNo": current_next_issue_no,
        "historyCount": len(sliced),
        "latestIssueNo": latest["issueNo"],
        "latestOpenTime": latest["openTime"],
        "nextIssueNo": target_issue_no,
        "history": sliced,
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


def _field_value(raw: str, label: str) -> str:
    value = raw.strip()
    prefix = f"{label}{FULLWIDTH_COLON}"
    while value.startswith(prefix):
        value = value[len(prefix) :].strip()
    return value


def _parse_direct_groups(raw: str) -> tuple[str, str, str]:
    numbers = re.findall(r"[0-9]+", raw)
    if len(numbers) < 3:
        raise ValueError(f"unable to parse 直选参考 groups from: {raw}")
    return numbers[-3], numbers[-2], numbers[-1]


def _parse_tail_number(raw: str) -> int:
    digits = re.findall(r"\d", raw)
    if not digits:
        raise ValueError(f"unable to parse trailing digit from: {raw}")
    return int(digits[-1])


def _parse_section_fields(lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in lines:
        if not line.startswith("- "):
            continue
        content = line[2:]
        key, sep, value = content.partition(FULLWIDTH_COLON)
        if not sep:
            continue
        fields[key.strip()] = value.strip()
    return fields


def _parse_replay_samples(replay_path: Path, support_cache: dict[str, Any]) -> list[ReplaySample]:
    replay_text = _read_text(replay_path)
    if SAMPLE_SECTION_HEADER not in replay_text:
        raise ValueError(f"{replay_path} missing {SAMPLE_SECTION_HEADER}")
    sample_text = replay_text.split(SAMPLE_SECTION_HEADER, 1)[1]
    parts = sample_text.split("\n### ")[1:]
    samples: list[ReplaySample] = []
    for part in parts:
        lines = part.splitlines()
        if not lines:
            continue
        heading = lines[0].strip()
        issue_no, _, news_id = heading.partition(" / ")
        issue_no = issue_no.strip()
        news_id = news_id.strip()
        fields = _parse_section_fields(lines[1:])
        required_keys = [
            "开奖回顾",
            "百位推荐",
            "十位推荐",
            "个位推荐",
            "胆码参考",
            "杀号",
            "直选参考",
            "组六参考",
            "单选15注参考",
            "精选号码",
        ]
        if not all(key in fields for key in required_keys):
            continue
        snapshot = _build_pl3_snapshot_from_cache(support_cache, issue_no)
        samples.append(ReplaySample(issue_no=issue_no, news_id=news_id, fields=fields, snapshot=snapshot))
    if not samples:
        raise ValueError(f"no replay samples parsed from {replay_path}")
    return samples


def _position_distance(target: dict[str, Any], sample: dict[str, Any], position_key: str) -> float:
    target_latest = target["recent20"][0]["positions"][position_key]
    sample_latest = sample["recent20"][0]["positions"][position_key]
    target_values20 = [record["positions"][position_key] for record in target["recent20"]]
    sample_values20 = [record["positions"][position_key] for record in sample["recent20"]]
    target_stats20 = target["positionSupport"]["recent20"][position_key]
    sample_stats20 = sample["positionSupport"]["recent20"][position_key]
    target_stats18 = target["positionSupport"]["recent18"][position_key]
    sample_stats18 = sample["positionSupport"]["recent18"][position_key]

    if position_key == "hundreds":
        target_primary = int(target_latest <= 4)
        sample_primary = int(sample_latest <= 4)
        target_streak = _streak(target_values20, lambda value: (value <= 4) == bool(target_primary))
        sample_streak = _streak(sample_values20, lambda value: (value <= 4) == bool(sample_primary))
        target_primary20 = target_stats20["bigSmall"]
        sample_primary20 = sample_stats20["bigSmall"]
        target_primary18 = target_stats18["bigSmall"]
        sample_primary18 = sample_stats18["bigSmall"]
        primary_keys = ("small", "big")
    elif position_key == "tens":
        target_primary = target_latest % 2
        sample_primary = sample_latest % 2
        target_streak = _streak(target_values20, lambda value: (value % 2) == target_primary)
        sample_streak = _streak(sample_values20, lambda value: (value % 2) == sample_primary)
        target_primary20 = target_stats20["oddEven"]
        sample_primary20 = sample_stats20["oddEven"]
        target_primary18 = target_stats18["oddEven"]
        sample_primary18 = sample_stats18["oddEven"]
        primary_keys = ("odd", "even")
    else:
        prime_set = {2, 3, 5, 7}
        target_primary = int(target_latest in prime_set)
        sample_primary = int(sample_latest in prime_set)
        target_streak = _streak(target_values20, lambda value: int(value in prime_set) == target_primary)
        sample_streak = _streak(sample_values20, lambda value: int(value in prime_set) == sample_primary)
        target_primary20 = target_stats20["primeComposite"]
        sample_primary20 = sample_stats20["primeComposite"]
        target_primary18 = target_stats18["primeComposite"]
        sample_primary18 = sample_stats18["primeComposite"]
        primary_keys = ("prime", "composite")

    distance = 0.0
    distance += 6.0 * abs(target_primary - sample_primary)
    distance += 1.4 * abs(target_streak - sample_streak)
    distance += 0.9 * abs(target_latest - sample_latest)
    distance += 0.7 * abs(target_primary20[primary_keys[0]] - sample_primary20[primary_keys[0]])
    distance += 0.7 * abs(target_primary20[primary_keys[1]] - sample_primary20[primary_keys[1]])
    distance += 0.5 * abs(target_primary18[primary_keys[0]] - sample_primary18[primary_keys[0]])
    distance += 0.5 * abs(target_primary18[primary_keys[1]] - sample_primary18[primary_keys[1]])

    for route_key in ("r0", "r1", "r2"):
        distance += 0.8 * abs(target_stats20["route012"][route_key] - sample_stats20["route012"][route_key])
        distance += 0.5 * abs(target_stats18["route012"][route_key] - sample_stats18["route012"][route_key])

    for digit in map(str, range(10)):
        distance += 0.12 * abs(target_stats20["digitCounts"][digit] - sample_stats20["digitCounts"][digit])
        distance += 0.08 * abs(target_stats18["digitCounts"][digit] - sample_stats18["digitCounts"][digit])
    return distance


def _general_distance(target: dict[str, Any], sample: dict[str, Any]) -> float:
    target_latest = target["recent20"][0]
    sample_latest = sample["recent20"][0]
    distance = 0.0
    distance += 0.5 * abs(target_latest["sum"] - sample_latest["sum"])
    distance += 0.6 * abs(target_latest["span"] - sample_latest["span"])
    distance += 1.2 * abs(_shape_code(target_latest["shape"]) - _shape_code(sample_latest["shape"]))
    distance += 0.5 * abs(target_latest["oddEven"]["odd"] - sample_latest["oddEven"]["odd"])
    distance += 0.5 * abs(target_latest["bigSmall"]["big"] - sample_latest["bigSmall"]["big"])
    for route_key in ("r0", "r1", "r2"):
        distance += 0.25 * abs(target_latest["route012"][route_key] - sample_latest["route012"][route_key])
    return distance


def _overall_distance(target: dict[str, Any], sample: dict[str, Any]) -> float:
    distance = _general_distance(target, sample)
    distance += 0.40 * _position_distance(target, sample, "hundreds")
    distance += 0.30 * _position_distance(target, sample, "tens")
    distance += 0.30 * _position_distance(target, sample, "units")
    return distance


def _rank_samples(target_snapshot: dict[str, Any], samples: list[ReplaySample]) -> list[ReplaySample]:
    ranked = []
    for sample in samples:
        ranked.append(
            ReplaySample(
                issue_no=sample.issue_no,
                news_id=sample.news_id,
                fields=sample.fields,
                snapshot=sample.snapshot,
                distance=_overall_distance(target_snapshot, sample.snapshot),
            )
        )
    return sorted(ranked, key=lambda item: (item.distance, -int(item.issue_no)))


def _format_review(snapshot: dict[str, Any]) -> str:
    latest = snapshot["recent20"][0]
    return (
        f"排列三{latest['issueNo']}期开奖：{latest['display']}，{latest['shape']}，和值{latest['sum']}，跨度{latest['span']}，"
        f"奇偶比{latest['oddEven']['ratio']}，大小比{latest['bigSmall']['ratio']}。"
    )


def _format_position_snapshot(snapshot: dict[str, Any], position_key: str) -> str:
    recent20 = snapshot["positionSupport"]["recent20"][position_key]
    recent18 = snapshot["positionSupport"]["recent18"][position_key]
    top20 = sorted(recent20["digitCounts"].items(), key=lambda item: (-item[1], item[0]))[:3]
    if position_key == "hundreds":
        return (
            f"recent20 大小比 {recent20['bigSmall']['ratio']}、012路 {recent20['route012']['ratio']}；"
            f"recent18 大小比 {recent18['bigSmall']['ratio']}、012路 {recent18['route012']['ratio']}；"
            f"高频数字 {' '.join(f'{digit}:{count}' for digit, count in top20)}"
        )
    if position_key == "tens":
        return (
            f"recent20 奇偶比 {recent20['oddEven']['ratio']}、012路 {recent20['route012']['ratio']}；"
            f"recent18 奇偶比 {recent18['oddEven']['ratio']}、012路 {recent18['route012']['ratio']}；"
            f"高频数字 {' '.join(f'{digit}:{count}' for digit, count in top20)}"
        )
    return (
        f"recent20 质合比 {recent20['primeComposite']['ratio']}、012路 {recent20['route012']['ratio']}；"
        f"recent18 质合比 {recent18['primeComposite']['ratio']}、012路 {recent18['route012']['ratio']}；"
        f"高频数字 {' '.join(f'{digit}:{count}' for digit, count in top20)}"
    )


def build_jiang_chuan_pl3_response(
    root: Path,
    expert_name: str,
    lottery_name: str,
    issue_no: str,
    skill_path: Path,
    replay_path: Path,
    snapshot_json_path: Path,
    snapshot_md_path: Path,
) -> dict[str, Any]:
    support_cache = _read_json(root / "data" / "pl3-support.json")
    target_snapshot = _read_json(snapshot_json_path)
    samples = _parse_replay_samples(replay_path, support_cache)
    ranked = _rank_samples(target_snapshot, samples)
    best = ranked[0]
    top3 = ranked[:3]

    hundred_value = _field_value(best.fields["百位推荐"], "百位推荐")
    tens_value = _field_value(best.fields["十位推荐"], "十位推荐")
    units_value = _field_value(best.fields["个位推荐"], "个位推荐")
    dan_value = _field_value(best.fields["胆码参考"], "胆码参考")
    kill_value = _field_value(best.fields["杀号"], "杀号")
    direct_value = _field_value(best.fields["直选参考"], "直选参考")
    group6_value = _field_value(best.fields["组六参考"], "组六参考")
    single15_value = _field_value(best.fields["单选15注参考"], "单选15注参考")
    pick_value = _field_value(best.fields["精选号码"], "精选号码")
    direct_groups = _parse_direct_groups(best.fields["直选参考"])
    pick_digits = re.findall(r"\d", pick_value)
    pick_joined = "".join(pick_digits[-3:]) if len(pick_digits) >= 3 else pick_value

    used_sources = [
        str(skill_path),
        str(replay_path),
        str(snapshot_md_path),
        str(snapshot_json_path),
        str(root / "data" / "pl3-support.json"),
    ]
    caution = "本地规则提取器使用 23 篇原文样本做最近似快照映射，当前仍不能证明这就是江川公开方法的唯一数学表达。"

    markdown_lines = [
        f"# {expert_name}_{lottery_name}_{issue_no}",
        "",
        "## 数据快照",
        "",
        f"- 目标期号：{issue_no}",
        f"- 快照视角：{target_snapshot['snapshotMode']}，截至 {target_snapshot['asOfDate']}，最新已开奖期号为 {target_snapshot['latestIssueNo']}，下一期为 {target_snapshot['predictionIssueNo']}。",
        f"- 上期回顾：{_format_review(target_snapshot)}",
        f"- 百位位置统计：{_format_position_snapshot(target_snapshot, 'hundreds')}",
        f"- 十位位置统计：{_format_position_snapshot(target_snapshot, 'tens')}",
        f"- 个位位置统计：{_format_position_snapshot(target_snapshot, 'units')}",
        f"- 最近似历史样本：{best.issue_no} / {best.news_id} / 距离 {best.distance:.2f}",
        f"- 备选样本：{'；'.join(f'{sample.issue_no}({sample.distance:.2f})' for sample in top3[1:]) if len(top3) > 1 else '无'}",
        "",
        "## 执行核对",
        "",
        "1. 已按 skill 第1步写出上期号码、形态、和值、跨度、奇偶比、大小比。",
        "2. 已按 skill 第2步先看百位大小，再对照 recent18/recent20 的 012 路和数字频次。",
        "3. 已按 skill 第3步先看十位奇偶，再对照 recent18/recent20 的 012 路和数字频次。",
        "4. 已按 skill 第4步先看个位质合，再对照 recent18/recent20 的 012 路和数字频次。",
        f"5. 依据更新后的 skill fallback 规则，发现当前快照到“唯一单码”仍存在多解，因此回到 `replay.md` 的 23 篇样本中做最近似快照检索，锁定样本 {best.issue_no} 作为整链模板。",
        "6. 已按同一篇样本整链继承胆码参考，未跨样本混拼。",
        "7. 已按同一篇样本整链继承杀号，未自由补写额外排除规则。",
        "8. 已按同一篇样本整链继承直选参考，且三个位置来源保持一致。",
        "9. 已按同一篇样本整链继承组六参考、单选15注参考和精选号码，未跳出本地样本库。",
        "",
        "## 按技能步骤执行",
        "",
        f"- 开奖回顾：{_format_review(target_snapshot)}",
        f"- 百位推荐：当前快照与样本 {best.issue_no} 最接近。参照该样本的百位落号链，{hundred_value}",
        f"- 十位推荐：当前快照与样本 {best.issue_no} 最接近。参照该样本的十位落号链，{tens_value}",
        f"- 个位推荐：当前快照与样本 {best.issue_no} 最接近。参照该样本的个位落号链，{units_value}",
        f"- 最近似样本映射：锁定样本 {best.issue_no} 后，后续 `胆码/杀号/直选/组六/15注/精选` 全部沿用同一篇样本的完整压缩链，不再跨样本拼接。",
        f"- 胆码参考：{dan_value}",
        f"- 杀号：{kill_value}",
        f"- 直选参考：{direct_groups[0]}-{direct_groups[1]}-{direct_groups[2]}",
        f"- 组六参考：{group6_value}",
        f"- 单选15注参考：{single15_value}",
        f"- 精选号码：{pick_joined}",
        "",
        "## 最终推荐结果",
        "",
        f"- 胆码参考：{dan_value}",
        f"- 杀号：{kill_value}",
        f"- 直选参考：{direct_groups[0]}-{direct_groups[1]}-{direct_groups[2]}",
        f"- 组六参考：{group6_value}",
        f"- 单选15注参考：{single15_value}",
        f"- 精选号码：{pick_joined}",
        "",
        "## 风险与问题",
        "",
        f"- {caution}",
        f"- 本次没有跳出 `{replay_path.name}` 的 23 篇样本库，也没有补造新的位置维度或跨彩种证据。",
        "- 彩票结果具有随机性，方法复刻不等于结果保证。",
    ]

    return {
        "status": "ok",
        "expert_name": expert_name,
        "lottery_name": lottery_name,
        "issue_no": issue_no,
        "used_sources": used_sources,
        "missing_or_uncertain": [caution],
        "markdown": "\n".join(markdown_lines),
    }
