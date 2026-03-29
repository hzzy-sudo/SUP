#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from history_support import (
    build_transition_map_dlt,
    jiang_chuan_support_dlt,
    latest_transition_snapshot_dlt,
    omission_map,
    position_stats,
    recent_zone_support_dlt,
    render_dlt_support_md,
    render_pl3_support_md,
    same_issue_support_dlt,
    summarize_recent_dlt,
)
from jiang_chuan_pl3_rules import build_jiang_chuan_pl3_response


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUTPUT_ROOT = ROOT / "predictions"
SCHEMA_PATH = ROOT / "scripts" / "prediction_result.schema.json"


@dataclass(frozen=True)
class ExpertSpec:
    slug: str
    expert_name: str
    lottery_key: str
    lottery_name: str
    order: int

    @property
    def skill_path(self) -> Path:
        return ROOT / self.slug / "SKILL.md"

    @property
    def replay_path(self) -> Path:
        return ROOT / self.slug / "references" / "replay.md"


@dataclass(frozen=True)
class IsolationRule:
    required_markers: tuple[str, ...]
    leak_markers: tuple[str, ...] = ()


EXPERT_SPECS = [
    ExpertSpec("dlt-expert-chen-taopu", "陈涛普", "dlt", "超级大乐透", 1),
    ExpertSpec("dlt-expert-fuge", "富哥", "dlt", "超级大乐透", 2),
    ExpertSpec("dlt-expert-chen-qingfeng", "陈青峰", "dlt", "超级大乐透", 3),
    ExpertSpec("dlt-expert-wan-miaoxian", "万妙仙", "dlt", "超级大乐透", 4),
    ExpertSpec("dlt-expert-wen-xinlin", "文新林", "dlt", "超级大乐透", 5),
    ExpertSpec("dlt-expert-chen-bing", "陈冰", "dlt", "超级大乐透", 6),
    ExpertSpec("dlt-expert-chen-yingfang", "陈樱芳", "dlt", "超级大乐透", 7),
    ExpertSpec("dlt-expert-hai-tian", "海天", "dlt", "超级大乐透", 8),
    ExpertSpec("dlt-expert-jiang-chuan", "江川", "dlt", "超级大乐透", 9),
    ExpertSpec("pl3-expert-jiang-chuan", "江川", "pl3", "排列三", 10),
]

EXPERT_BY_SLUG = {spec.slug: spec for spec in EXPERT_SPECS}

COMMON_REQUIRED_SECTIONS = (
    "## 数据快照",
    "## 执行核对",
    "## 按技能步骤执行",
    "## 最终推荐结果",
    "## 风险与问题",
)

EXPERT_ISOLATION_RULES: dict[str, IsolationRule] = {
    "dlt-expert-chen-taopu": IsolationRule(
        required_markers=("极距分析", "和值分析"),
        leak_markers=("极距分析", "和值分析"),
    ),
    "dlt-expert-fuge": IsolationRule(
        required_markers=("龙头：", "凤尾：", "前区五区比", "前区双胆", "大乐透15+5复式"),
        leak_markers=("龙头：", "凤尾：", "前区五区比", "前区双胆", "大乐透15+5复式"),
    ),
    "dlt-expert-chen-qingfeng": IsolationRule(
        required_markers=("历史同期尾数分析", "尾数012路", "前区必杀一尾", "前区杀8码", "后区杀4码"),
        leak_markers=("历史同期尾数分析", "尾数012路", "前区必杀一尾", "前区杀8码", "后区杀4码"),
    ),
    "dlt-expert-wan-miaoxian": IsolationRule(
        required_markers=("前区热号分析", "其下期表现最热的5码为", "前区冷号分析", "其下期表现最冷的5码为", "前区必杀8码"),
        leak_markers=("前区热号分析", "其下期表现最热的5码为", "前区冷号分析", "其下期表现最冷的5码为", "前区必杀8码"),
    ),
    "dlt-expert-wen-xinlin": IsolationRule(
        required_markers=("三区走势", "龙头凤尾", "跨度分析", "后区重点关注", "大乐透前区12码推荐"),
        leak_markers=("三区走势", "龙头凤尾", "跨度分析", "后区重点关注", "大乐透前区12码推荐"),
    ),
    "dlt-expert-chen-bing": IsolationRule(
        required_markers=("连续10期龙头分别开出", "前区龙头注意号码", "前区凤尾号码大范围关注", "前区绝杀10码", "后区和值"),
        leak_markers=("连续10期龙头分别开出", "前区龙头注意号码", "前区凤尾号码大范围关注", "前区绝杀10码", "后区和值"),
    ),
    "dlt-expert-chen-yingfang": IsolationRule(
        required_markers=("大乐透最近10期号码分析", "跨度推荐", "连号分析", "重号分析"),
        leak_markers=("大乐透最近10期号码分析", "跨度推荐", "连号分析", "重号分析"),
    ),
    "dlt-expert-hai-tian": IsolationRule(
        required_markers=("前一区【01-12】", "前二区【13-23】", "前三区【24-35】", "前区缩水11码", "后区3码参考"),
        leak_markers=("前一区【01-12】", "前二区【13-23】", "前三区【24-35】", "前区缩水11码", "后区3码参考"),
    ),
    "dlt-expert-jiang-chuan": IsolationRule(
        required_markers=("前区大小比分析", "前区奇偶比分析", "前区两胆参考", "前区杀号参考", "后区五码参考"),
        leak_markers=("前区大小比分析", "前区奇偶比分析", "前区两胆参考", "前区杀号参考", "后区五码参考"),
    ),
    "pl3-expert-jiang-chuan": IsolationRule(
        required_markers=("百位推荐", "十位推荐", "个位推荐", "直选参考", "精选号码"),
        leak_markers=("百位推荐", "十位推荐", "个位推荐", "直选参考", "精选号码"),
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按本地专家 skill 顺序生成预测 markdown 文件。")
    parser.add_argument("--skill", action="append", choices=sorted(EXPERT_BY_SLUG), help="仅执行指定 skill，可重复传入。")
    parser.add_argument(
        "--lottery",
        choices=["all", "dlt", "pl3"],
        default="all",
        help="限制执行的彩票类型，默认 all。",
    )
    parser.add_argument("--dlt-issue", help="指定大乐透预测目标期号；默认使用当前缓存里的 nextIssueNo。")
    parser.add_argument("--pl3-issue", help="指定排列三预测目标期号；默认使用当前缓存里的 nextIssueNo。")
    parser.add_argument(
        "--refresh-policy",
        choices=["auto", "force", "never"],
        default="auto",
        help="数据刷新策略。auto=当天无缓存才刷新；force=强制刷新一次；never=完全复用缓存。",
    )
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="预测运行日期，默认今天。用于刷新判断和结果目录命名。",
    )
    parser.add_argument("--model", default="gpt-5.4", help="codex exec 使用的模型，默认 gpt-5.4。")
    parser.add_argument("--codex-executable", help="Explicit codex.exe path override.")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="连续调用模型时的停顿秒数，默认 1 秒。",
    )
    parser.add_argument(
        "--output-root",
        default=str(OUTPUT_ROOT),
        help="结果输出根目录，默认 supser/predictions。",
    )
    return parser.parse_args()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig", errors="strict")


def read_json(path: Path) -> Any:
    return json.loads(read_text(path))


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8", newline="\n")


def is_http_456_error(message: str) -> bool:
    return "HTTP Error 456" in message


def cache_is_current(prefix: str, as_of_date: str) -> bool:
    support_path = DATA_DIR / f"{prefix}-support.json"
    if not support_path.exists():
        return False
    try:
        payload = read_json(support_path)
    except Exception:
        return False
    return payload.get("asOfDate") == as_of_date


def run_command(cmd: list[str], cwd: Path | None = None) -> tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd or ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def resolve_codex_executable(explicit_path: str | None = None) -> str:
    candidates: list[str] = []
    if explicit_path:
        candidates.append(explicit_path)
    for key in ("CODEX_EXECUTABLE", "CODEX_PATH"):
        value = os.environ.get(key)
        if value:
            candidates.append(value)

    for binary_name in ("codex", "codex.exe"):
        resolved = shutil.which(binary_name)
        if resolved:
            candidates.append(resolved)

    user_profile = os.environ.get("USERPROFILE")
    if user_profile:
        extension_root = Path(user_profile) / ".vscode" / "extensions"
        if extension_root.exists():
            matches = sorted(extension_root.glob("openai.chatgpt-*-win32-x64/bin/windows-x86_64/codex.exe"))
            candidates.extend(str(path) for path in reversed(matches))

    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(Path(candidate))

    raise RuntimeError(
        "未找到 codex 可执行文件。请将 codex 加入 PATH，或设置 CODEX_EXECUTABLE / CODEX_PATH 指向 codex.exe。"
    )


def ensure_cache(prefix: str, refresh_policy: str, as_of_date: str) -> None:
    if refresh_policy == "never":
        if not cache_is_current(prefix, as_of_date):
            raise RuntimeError(f"{prefix} 缓存不存在或不是 {as_of_date}，但 refresh-policy=never。")
        return
    if refresh_policy == "auto" and cache_is_current(prefix, as_of_date):
        return

    script = ROOT / "scripts" / ("update_dlt_replays.py" if prefix == "dlt" else "update_pl3_replays.py")
    code, stdout, stderr = run_command([sys.executable, str(script), "--all", "--as-of-date", as_of_date], cwd=ROOT)
    if code == 0:
        return
    error_text = "\n".join([stdout.strip(), stderr.strip()]).strip()
    if is_http_456_error(error_text) and cache_is_current(prefix, as_of_date):
        return
    raise RuntimeError(f"{prefix} 刷新失败：{error_text}")


def selected_experts(args: argparse.Namespace) -> list[ExpertSpec]:
    if args.skill:
        selected = [EXPERT_BY_SLUG[slug] for slug in args.skill]
    else:
        selected = list(EXPERT_SPECS)
    if args.lottery != "all":
        selected = [spec for spec in selected if spec.lottery_key == args.lottery]
    return sorted(selected, key=lambda item: item.order)


def current_issue_targets(args: argparse.Namespace) -> dict[str, str | None]:
    return {
        "dlt": args.dlt_issue,
        "pl3": args.pl3_issue,
    }


def resolve_history_slice(history: list[dict], current_next_issue_no: str, target_issue_no: str | None) -> tuple[list[dict], str, str]:
    resolved_target = target_issue_no or current_next_issue_no
    if resolved_target == current_next_issue_no:
        return history, resolved_target, "latest"
    issue_to_index = {record["issueNo"]: idx for idx, record in enumerate(history)}
    if resolved_target not in issue_to_index:
        raise ValueError(f"目标期号 {resolved_target} 不在当前历史库中，也不是当前 nextIssueNo={current_next_issue_no}。")
    idx = issue_to_index[resolved_target]
    sliced = history[idx + 1 :]
    if not sliced:
        raise ValueError(f"目标期号 {resolved_target} 前面没有可用历史样本。")
    return sliced, resolved_target, "historical"


def build_dlt_snapshot(as_of_date: str, target_issue_no: str | None) -> tuple[dict, str]:
    support_cache = read_json(DATA_DIR / "dlt-support.json")
    history = support_cache["history"]
    sliced, resolved_target, mode = resolve_history_slice(history, support_cache["nextIssueNo"], target_issue_no)
    if len(sliced) < 10:
        raise ValueError(f"大乐透目标期号 {resolved_target} 可用历史不足 10 期，当前仅 {len(sliced)} 期。")
    latest = sliced[0]
    omission_front = omission_map(sliced, "front", range(1, 36))
    omission_back = omission_map(sliced, "back", range(1, 13))
    transition_support = build_transition_map_dlt(sliced)
    support = {
        "asOfDate": as_of_date,
        "lottoType": "dlt",
        "predictionIssueNo": resolved_target,
        "snapshotMode": mode,
        "sourceCurrentNextIssueNo": support_cache["nextIssueNo"],
        "historyCount": len(sliced),
        "latestIssueNo": latest["issueNo"],
        "latestOpenTime": latest["openTime"],
        "nextIssueNo": resolved_target,
        "history": sliced,
        "recent10": sliced[:10],
        "recent10Summary": summarize_recent_dlt(sliced),
        "recent10ZoneSupport": recent_zone_support_dlt(sliced, omission_front),
        "jiangChuanPatternSupport": jiang_chuan_support_dlt(sliced),
        "sameIssueSupport": same_issue_support_dlt(sliced, resolved_target),
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
    return support, render_dlt_support_md(support)


def build_pl3_snapshot(as_of_date: str, target_issue_no: str | None) -> tuple[dict, str]:
    support_cache = read_json(DATA_DIR / "pl3-support.json")
    history = support_cache["history"]
    sliced, resolved_target, mode = resolve_history_slice(history, support_cache["nextIssueNo"], target_issue_no)
    if len(sliced) < 20:
        raise ValueError(f"排列三目标期号 {resolved_target} 可用历史不足 20 期，当前仅 {len(sliced)} 期。")
    latest = sliced[0]
    recent20 = sliced[:20]
    recent18 = sliced[:18]
    support = {
        "asOfDate": as_of_date,
        "lottoType": "pl3",
        "predictionIssueNo": resolved_target,
        "snapshotMode": mode,
        "sourceCurrentNextIssueNo": support_cache["nextIssueNo"],
        "historyCount": len(sliced),
        "latestIssueNo": latest["issueNo"],
        "latestOpenTime": latest["openTime"],
        "nextIssueNo": resolved_target,
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
    return support, render_pl3_support_md(support)


def write_snapshot_files(output_root: Path, lottery_key: str, issue_no: str, support_payload: dict, support_md: str) -> tuple[Path, Path]:
    context_dir = output_root / "_contexts" / lottery_key / issue_no
    json_path = context_dir / "snapshot-support.json"
    md_path = context_dir / "snapshot-support.md"
    write_json(json_path, support_payload)
    write_text(md_path, support_md)
    return json_path, md_path


def build_isolated_workspace(
    spec: ExpertSpec,
    snapshot_json: Path,
    snapshot_md: Path,
    temp_dir: str,
) -> tuple[Path, Path, Path, Path, Path]:
    workspace = Path(temp_dir) / "workspace"
    references_dir = workspace / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    skill_copy = workspace / "SKILL.md"
    replay_copy = references_dir / "replay.md"
    snapshot_md_copy = workspace / "snapshot-support.md"
    snapshot_json_copy = workspace / "snapshot-support.json"
    shutil.copy2(spec.skill_path, skill_copy)
    shutil.copy2(spec.replay_path, replay_copy)
    shutil.copy2(snapshot_md, snapshot_md_copy)
    shutil.copy2(snapshot_json, snapshot_json_copy)
    return workspace, skill_copy, replay_copy, snapshot_md_copy, snapshot_json_copy


def build_prompt(
    spec: ExpertSpec,
    issue_no: str,
    snapshot_json_path: Path,
    skill_text: str,
    replay_text: str,
    snapshot_md_text: str,
) -> str:
    rule = EXPERT_ISOLATION_RULES[spec.slug]
    ordered_markers = " -> ".join(rule.required_markers)
    return f"""
你现在是一个“本地专家 skill 严格执行器”，当前处于单专家隔离工作区，只负责给单个专家生成一个预测 markdown 文件。

目标专家：{spec.expert_name}
目标彩票：{spec.lottery_name}
目标期号：{issue_no}

以下三份本地材料已经直接内嵌在本次提示里，它们就是你必须执行的唯一主要依据：

<skill_md>
{skill_text}
</skill_md>

<replay_md>
{replay_text}
</replay_md>

<snapshot_support_md>
{snapshot_md_text}
</snapshot_support_md>

如需核对结构化字段，隔离工作区里还只允许读取这一份结构化快照：
- {snapshot_json_path}
`snapshot-support.json` 不是可选补充，而是允许主动读取的主要本地证据；如果 `snapshot_support_md` 只给了摘要，你必须继续读取 json 里的相关字段，不要把“摘要未展开”误判成“本地没有数据”。

强约束：
- 只能使用上面内嵌材料和这一个结构化快照中的事实与指标，禁止联网，禁止 web，禁止使用任何隔离工作区外资料。
- 必须完全遵守 skill 里的 `分析步骤`、`取舍规则`、`输出模板`，不能跳步，不能换顺序，不能省略步骤。
- 不允许补写 skill 历史上没有稳定使用过的维度，不允许把别的专家的方法混进来。
- 你的正文里必须按顺序显式落出这位专家的专属模块锚点：{ordered_markers}
- 禁止出现其他专家的人名、其他专家的专属栏目名、其他彩票体系的步骤骨架；一旦无法保持专家纯度，必须返回 `blocked`。
- 不允许因为数据不足而自行脑补；任何缺口都必须写进 `missing_or_uncertain`。只要仍能基于现有本地证据完成完整预测，就保持 `status` 为 `ok`，并在 `风险与问题` 中明确说明证据边界。
- 如果 `replay.md` 已经提供逐期文本摘录或逐期真实输出样本，必须先把这些样本当成“如何把统计落成具体数字”的本地证据，再决定能否继续，不能因为存在并列统计就立刻跳成 blocked。
- 如果 `replay.md` 已经包含 `## 逐期文本摘录` 或等价的原文输出句摘录，就不要再把“缺少逐期原文落号样本”写进 `missing_or_uncertain` 或 `风险与问题`。
- 如果缺少逐条样本，但 `snapshot_support_md`、`snapshot-support.json`、`replay.md` 已经提供了足以支撑收敛的统计、热码、遗漏、分布或历史跟随结论，优先生成正常预测，把缺口写进 `missing_or_uncertain` 和 `风险与问题`，不要轻易返回 `blocked`。
- 如果 `snapshot-support.json` 中已经存在 `recentNextDraws`、`sameIssueSupport`、`jiangChuanPatternSupport`、`follow` 这类结构化样本链，你必须直接使用它们，不要声称“快照未提供逐条样本”。
- 不要编辑任何文件，不要运行刷新脚本，不要修改工作区；只输出最终 JSON。
- 如果目标期号是历史期号，只能使用快照文件中已经截断好的历史视角，不能引用目标期号之后的开奖事实。

markdown 必须用中文，并严格包含以下板块：
1. `# {spec.expert_name}_{spec.lottery_name}_{issue_no}`
2. `## 数据快照`
3. `## 执行核对`
4. `## 按技能步骤执行`
5. `## 最终推荐结果`
6. `## 风险与问题`

`执行核对` 要逐条列出你实际执行了 skill 中的哪些步骤，顺序必须和 skill 一致。
`按技能步骤执行` 必须按该专家的真实模块顺序展开，不能换成你自己的通用模板。
`按技能步骤执行` 和 `最终推荐结果` 里涉及专家模块名时，必须直接使用该专家输出模板中的原栏目名，不得改写成其他近义词。
`风险与问题` 只允许写真实存在的问题或边界，不要为了凑版块主动制造风险描述。
- 如果没有明显本地数据缺口、没有方法阻断，也没有会影响完整输出的证据边界，就只保留一条“未发现会影响本期完整输出的明显本地数据缺口或方法阻断”，再补一条随机性提醒。
- 只有真正影响把统计落成号码的缺口，才写进 `missing_or_uncertain` 和 `风险与问题`。
- 不要把“共享数据是结构摘要”“彩票本身有随机性”扩写成冗长免责声明。
- 即使没有数据缺口，也要明确写出“彩票结果具有随机性，方法复刻不等于结果保证”

如果数据完整可执行：
- `status` 返回 `ok`
- `missing_or_uncertain` 返回空数组或只保留真正的不确定边界

如果数据不完整但仍能基于现有证据给出完整预测：
- `status` 返回 `ok`
- `missing_or_uncertain` 写明真实缺口或证据边界
- markdown 继续正常输出预测结果，并在 `风险与问题` 里明确提醒不确定性

只有在以下情况才返回 `blocked`：
- 你无法给出任何完整预测号码
- 你无法满足固定章节结构
- 你无法满足当前专家的核心锚点顺序
- 或者你明显混入了其他专家姓名/其他彩票体系
""".strip()


def validate_section_order(markdown: str, expected_sections: tuple[str, ...]) -> list[str]:
    errors: list[str] = []
    cursor = 0
    for section in expected_sections:
        idx = markdown.find(section, cursor)
        if idx < 0:
            errors.append(f"缺少固定章节：{section}")
            continue
        cursor = idx + len(section)
    return errors


def validate_ordered_markers(markdown: str, markers: tuple[str, ...]) -> list[str]:
    errors: list[str] = []
    cursor = 0
    for marker in markers:
        idx = markdown.find(marker, cursor)
        if idx < 0:
            errors.append(f"缺少专家专属锚点：{marker}")
            continue
        cursor = idx + len(marker)
    return errors


def validate_expert_isolation(spec: ExpertSpec, issue_no: str, response: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    markdown = str(response.get("markdown", ""))
    expected_title = f"# {spec.expert_name}_{spec.lottery_name}_{issue_no}"
    if not markdown.startswith(expected_title):
        errors.append(f"标题不匹配，期望以 `{expected_title}` 开头")

    errors.extend(validate_section_order(markdown, COMMON_REQUIRED_SECTIONS))

    other_names = sorted({item.expert_name for item in EXPERT_SPECS if item.expert_name != spec.expert_name})
    for other_name in other_names:
        if other_name and other_name in markdown:
            errors.append(f"出现其他专家姓名：{other_name}")

    if response.get("status") == "ok":
        rule = EXPERT_ISOLATION_RULES[spec.slug]
        errors.extend(validate_ordered_markers(markdown, rule.required_markers))

    deduped: list[str] = []
    seen: set[str] = set()
    for error in errors:
        if error not in seen:
            deduped.append(error)
            seen.add(error)
    return deduped


def summarize_isolation_errors(errors: list[str]) -> str:
    categories: list[str] = []
    if any(error.startswith("标题不匹配") or error.startswith("缺少固定章节") for error in errors):
        categories.append("输出结构不符合主入口协议")
    if any(error.startswith("缺少专家专属锚点") for error in errors):
        categories.append("缺少当前专家的必要锚点")
    if any(error.startswith("出现其他专家姓名") for error in errors):
        categories.append("检测到跨专家污染")
    if not categories:
        categories.append("专家隔离校验未通过")
    return "专家隔离校验失败：" + "；".join(categories)


def sanitize_missing_or_uncertain(items: list[str]) -> list[str]:
    sanitized: list[str] = []
    seen: set[str] = set()
    replay_fulltext_pattern = re.compile(
        r"replay(?:_md|\.md).*(逐篇原文|逐期正文|原文摘录|落号样本|荐号明细|全文)",
        re.IGNORECASE,
    )
    for item in items:
        cleaned = str(item).strip()
        if not cleaned:
            continue
        if replay_fulltext_pattern.search(cleaned):
            continue
        if cleaned not in seen:
            sanitized.append(cleaned)
            seen.add(cleaned)
    return sanitized


def normalize_risk_section(markdown: str, missing_or_uncertain: list[str]) -> str:
    deduped_items = sanitize_missing_or_uncertain(missing_or_uncertain)

    if deduped_items:
        body_lines = [f"- {item}" for item in deduped_items]
    else:
        body_lines = ["- 未发现会影响本期完整输出的明显本地数据缺口或方法阻断。"]
    body_lines.append("- 彩票结果具有随机性，方法复刻不等于结果保证。")

    replacement = "## 风险与问题\n\n" + "\n".join(body_lines)
    pattern = r"(?ms)^## 风险与问题\s*\n.*?(?=^## |\Z)"
    if re.search(pattern, markdown):
        return re.sub(pattern, replacement + "\n\n", markdown, count=1)
    return markdown.rstrip() + "\n\n" + replacement + "\n"


def blocked_payload(spec: ExpertSpec, issue_no: str, sources: list[str], reason: str) -> dict[str, Any]:
    markdown = "\n".join(
        [
            f"# {spec.expert_name}_{spec.lottery_name}_{issue_no}",
            "",
            "## 数据快照",
            "",
            *(f"- {source}" for source in sources),
            "",
            "## 执行核对",
            "",
            "1. 主入口已尝试按顺序调用该专家 skill。",
            "2. 在正式生成预测前发生阻塞，未输出伪造号码。",
            "",
            "## 按技能步骤执行",
            "",
            f"- 阻塞原因：{reason}",
            "",
            "## 最终推荐结果",
            "",
            "- 未生成预测号码，因为当前执行链无法保证严格按 skill 无幻觉完成。",
            "",
            "## 风险与问题",
            "",
            f"- {reason}",
            "- 彩票结果具有随机性，方法复刻不等于结果保证。",
        ]
    )
    return {
        "status": "blocked",
        "expert_name": spec.expert_name,
        "lottery_name": spec.lottery_name,
        "issue_no": issue_no,
        "used_sources": sources,
        "missing_or_uncertain": [reason],
        "markdown": markdown,
    }


def run_codex(
    spec: ExpertSpec,
    issue_no: str,
    snapshot_json: Path,
    snapshot_md: Path,
    model: str,
    codex_executable_override: str | None = None,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="supser-predict-") as temp_dir:
        workspace, skill_copy, replay_copy, snapshot_md_copy, snapshot_json_copy = build_isolated_workspace(
            spec,
            snapshot_json,
            snapshot_md,
            temp_dir,
        )
        prompt = build_prompt(
            spec,
            issue_no,
            snapshot_json_copy,
            read_text(skill_copy),
            read_text(replay_copy),
            read_text(snapshot_md_copy),
        )
        out_path = Path(temp_dir) / "response.json"
        codex_executable = resolve_codex_executable(codex_executable_override)
        cmd = [
            codex_executable,
            "exec",
            "--skip-git-repo-check",
            "--ephemeral",
            "-s",
            "read-only",
            "-m",
            model,
            "-C",
            str(workspace),
            "--output-schema",
            str(SCHEMA_PATH),
            "-o",
            str(out_path),
            prompt,
        ]
        code, stdout, stderr = run_command(cmd, cwd=workspace)
        if code != 0:
            raise RuntimeError("\n".join([stdout.strip(), stderr.strip()]).strip())
        return json.loads(read_text(out_path))


def result_path(output_root: Path, spec: ExpertSpec, issue_no: str) -> Path:
    file_name = f"{spec.expert_name}_{spec.lottery_name}_{issue_no}.md"
    return output_root / spec.slug / file_name


def json_path_for_result(output_root: Path, spec: ExpertSpec, issue_no: str) -> Path:
    file_name = f"{spec.expert_name}_{spec.lottery_name}_{issue_no}.json"
    return output_root / spec.slug / file_name


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    experts = selected_experts(args)
    if not experts:
        raise SystemExit("没有匹配到需要执行的 skill。")

    needed_lotteries = sorted({expert.lottery_key for expert in experts})
    for lottery_key in needed_lotteries:
        ensure_cache(lottery_key, args.refresh_policy, args.as_of_date)

    issue_targets = current_issue_targets(args)
    snapshots: dict[str, tuple[dict, Path, Path]] = {}
    snapshot_errors: dict[str, str] = {}
    for lottery_key in needed_lotteries:
        try:
            if lottery_key == "dlt":
                payload, md = build_dlt_snapshot(args.as_of_date, issue_targets["dlt"])
            else:
                payload, md = build_pl3_snapshot(args.as_of_date, issue_targets["pl3"])
            json_path, md_path = write_snapshot_files(output_root, lottery_key, payload["predictionIssueNo"], payload, md)
            snapshots[lottery_key] = (payload, json_path, md_path)
        except Exception as exc:
            snapshot_errors[lottery_key] = str(exc)

    failures = 0
    for index, spec in enumerate(experts, start=1):
        if spec.lottery_key in snapshots:
            snapshot_payload, snapshot_json, snapshot_md = snapshots[spec.lottery_key]
            issue_no = snapshot_payload["predictionIssueNo"]
            sources = [
                str(spec.skill_path),
                str(spec.replay_path),
                str(snapshot_md),
                str(snapshot_json),
            ]
        else:
            issue_no = issue_targets[spec.lottery_key] or "unknown"
            sources = [
                str(spec.skill_path),
                str(spec.replay_path),
            ]
        print(f"[{index}/{len(experts)}] generating {spec.slug} -> {issue_no}")
        if spec.lottery_key in snapshot_errors:
            response = blocked_payload(spec, issue_no, sources, f"预测快照构建失败：{snapshot_errors[spec.lottery_key]}")
        else:
            try:
                if spec.slug == "pl3-expert-jiang-chuan":
                    response = build_jiang_chuan_pl3_response(
                        root=ROOT,
                        expert_name=spec.expert_name,
                        lottery_name=spec.lottery_name,
                        issue_no=issue_no,
                        skill_path=spec.skill_path,
                        replay_path=spec.replay_path,
                        snapshot_json_path=snapshot_json,
                        snapshot_md_path=snapshot_md,
                    )
                else:
                    response = run_codex(
                        spec,
                        issue_no,
                        snapshot_json,
                        snapshot_md,
                        args.model,
                        args.codex_executable,
                    )
            except Exception as exc:
                response = blocked_payload(spec, issue_no, sources, f"模型调用失败：{exc}")

        if response.get("expert_name") != spec.expert_name:
            response["expert_name"] = spec.expert_name
        if response.get("lottery_name") != spec.lottery_name:
            response["lottery_name"] = spec.lottery_name
        if response.get("issue_no") != issue_no:
            response["issue_no"] = issue_no
        if not response.get("used_sources"):
            response["used_sources"] = sources
        response["missing_or_uncertain"] = sanitize_missing_or_uncertain(response.get("missing_or_uncertain") or [])
        if response.get("status") == "ok" and response.get("markdown"):
            response["markdown"] = normalize_risk_section(
                response["markdown"],
                response["missing_or_uncertain"],
            )

        isolation_errors = validate_expert_isolation(spec, issue_no, response)
        if isolation_errors:
            response = blocked_payload(
                spec,
                issue_no,
                sources,
                summarize_isolation_errors(isolation_errors),
            )
            response["missing_or_uncertain"] = isolation_errors

        md_out = result_path(output_root, spec, issue_no)
        json_out = json_path_for_result(output_root, spec, issue_no)
        write_text(md_out, response["markdown"])
        write_json(json_out, response)
        print(f"saved markdown: {md_out}")
        if response["status"] != "ok":
            failures += 1
            print(f"warning: {spec.slug} returned status={response['status']}")

        if index < len(experts) and args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    if failures:
        print(f"completed with {failures} non-ok results")
        return 1

    print("all predictions generated successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
