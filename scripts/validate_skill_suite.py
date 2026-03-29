from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SYSTEM_SKILL_CREATOR = (
    Path.home()
    / ".codex"
    / "skills"
    / ".system"
    / "skill-creator"
    / "scripts"
    / "quick_validate.py"
)


REQUIRED_HEADINGS = [
    "## 使用场景",
    "## 必读输入",
    "## 专家核心思路",
    "### 核心定位",
    "### 分析框架",
    "### 核心分析逻辑",
    "### 出号心理和策略",
    "## 分析步骤",
    "### 步骤中的死扣细节",
    "## 取舍规则",
    "## 输出模板",
    "## 可迁移经验",
    "### 核心经验总结",
    "### 复制时的执行习惯",
    "## 局限与禁忌",
]


DLT_SKILLS = [
    "dlt-expert-chen-taopu",
    "dlt-expert-fuge",
    "dlt-expert-chen-qingfeng",
    "dlt-expert-wan-miaoxian",
    "dlt-expert-wen-xinlin",
    "dlt-expert-chen-bing",
    "dlt-expert-chen-yingfang",
    "dlt-expert-hai-tian",
    "dlt-expert-jiang-chuan",
]

PL3_SKILLS = ["pl3-expert-jiang-chuan"]


@dataclass(frozen=True)
class SkillValidation:
    slug: str
    lottery: str
    replay_label: str
    replay_count_label: str
    needs_prize_section: str
    support_checks: list[str]
    recent_min: int


SKILL_VALIDATIONS = [
    SkillValidation(
        slug="dlt-expert-chen-taopu",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "recent10",
            "recent10Summary.frontOddEven.ratio",
            "recent10Summary.frontSpanSupport.values",
            "recent10Summary.frontSumSupport.values",
            "recent10Summary.backOddEven.ratio",
            "recent10Summary.backBigSmall.ratio",
        ],
        recent_min=10,
    ),
    SkillValidation(
        slug="dlt-expert-fuge",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "recent10",
            "recent10Summary.zone5.ratio",
            "recent10Summary.headSupport.values",
            "recent10Summary.tailSupport.values",
            "recent10Summary.frontOddEven.ratio",
            "recent10Summary.frontBigSmall.ratio",
            "recent10Summary.backOddEven.ratio",
            "recent10Summary.backBigSmall.ratio",
        ],
        recent_min=10,
    ),
    SkillValidation(
        slug="dlt-expert-chen-qingfeng",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "sameIssueSupport.recordCount",
            "sameIssueSupport.zone5Totals.ratio",
            "sameIssueSupport.frontTailOddEven.ratio",
            "sameIssueSupport.frontTailRoute012.ratio",
            "sameIssueSupport.frontTailBigSmall.ratio",
            "sameIssueSupport.backTailCounts",
        ],
        recent_min=10,
    ),
    SkillValidation(
        slug="dlt-expert-wan-miaoxian",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "transition460.front",
            "transition460.back",
            "currentTransitionSnapshot.latestFront",
            "currentTransitionSnapshot.latestBack",
            "currentTransitionSnapshot.front",
            "currentTransitionSnapshot.back",
        ],
        recent_min=10,
    ),
    SkillValidation(
        slug="dlt-expert-wen-xinlin",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "recent10",
            "recent10Summary.zone3.ratio",
            "recent10Summary.headSupport.values",
            "recent10Summary.tailSupport.values",
            "recent10Summary.frontSpanSupport.values",
            "recent10Summary.backOddEven.ratio",
            "recent10Summary.backBigSmall.ratio",
        ],
        recent_min=9,
    ),
    SkillValidation(
        slug="dlt-expert-chen-bing",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "recent10",
            "recent10Summary.headSupport.values",
            "recent10Summary.tailSupport.values",
            "recent10Summary.backSumSupport.values",
            "recent10Summary.backOddEven.ratio",
            "recent10Summary.backBigSmall.ratio",
        ],
        recent_min=10,
    ),
    SkillValidation(
        slug="dlt-expert-chen-yingfang",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "recent10",
            "recent10Summary.frontSpanSupport.values",
            "recent10Summary.backBigSmall.ratio",
        ],
        recent_min=9,
    ),
    SkillValidation(
        slug="dlt-expert-hai-tian",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "recent10ZoneSupport.zone1.topMissing",
            "recent10ZoneSupport.zone2.topMissing",
            "recent10ZoneSupport.zone3.topMissing",
            "omissionSupport.frontTopMissing",
            "omissionSupport.backTopMissing",
            "omissionSupport.backGroups.hot",
            "omissionSupport.backGroups.warm",
            "omissionSupport.backGroups.cold",
        ],
        recent_min=10,
    ),
    SkillValidation(
        slug="dlt-expert-jiang-chuan",
        lottery="dlt",
        replay_label="大乐透",
        replay_count_label="可访问大乐透往期",
        needs_prize_section="## 大乐透一二等命中记录",
        support_checks=[
            "jiangChuanPatternSupport.referenceIssueNo",
            "jiangChuanPatternSupport.frontSizeFollow.currentRatio",
            "jiangChuanPatternSupport.frontOddEvenFollow.currentRatio",
            "jiangChuanPatternSupport.backPairFollow.currentPair",
        ],
        recent_min=10,
    ),
    SkillValidation(
        slug="pl3-expert-jiang-chuan",
        lottery="pl3",
        replay_label="排列三",
        replay_count_label="可访问排列三往期",
        needs_prize_section="## 跨彩种一二等战绩说明",
        support_checks=[
            "recent18",
            "recent20",
            "positionSupport.recent18.hundreds.bigSmall.ratio",
            "positionSupport.recent18.tens.oddEven.ratio",
            "positionSupport.recent18.units.primeComposite.ratio",
            "positionSupport.recent20.hundreds.bigSmall.ratio",
            "positionSupport.recent20.tens.oddEven.ratio",
            "positionSupport.recent20.units.primeComposite.ratio",
        ],
        recent_min=18,
    ),
]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig", errors="strict")


def run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    proc = subprocess.run(cmd, cwd=ROOT.parent, text=True, capture_output=True, env=merged_env)
    if proc.returncode != 0:
        message = "\n".join(
            [
                f"command failed: {' '.join(cmd)}",
                proc.stdout.strip(),
                proc.stderr.strip(),
            ]
        ).strip()
        raise RuntimeError(message)


def is_http_456_error(message: str) -> bool:
    return "HTTP Error 456" in message


def parse_frontmatter(text: str) -> tuple[str, str]:
    if not text.startswith("---\n"):
        raise AssertionError("SKILL.md 缺少 frontmatter 开始标记")
    end = text.find("\n---\n", 4)
    if end == -1:
        raise AssertionError("SKILL.md 缺少 frontmatter 结束标记")
    frontmatter = text[4:end].splitlines()
    values: dict[str, str] = {}
    for line in frontmatter:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip()
    if set(values) != {"name", "description"}:
        raise AssertionError(f"frontmatter 字段异常: {sorted(values)}")
    return values["name"], values["description"]


def dig(data: Any, dotted_path: str) -> Any:
    current = data
    for part in dotted_path.split("."):
        if isinstance(current, list):
            if not part.isdigit():
                raise KeyError(f"列表节点需要数字索引: {part}")
            current = current[int(part)]
            continue
        if part not in current:
            raise KeyError(part)
        current = current[part]
    return current


def count_markdown_rows(table_text: str) -> int:
    count = 0
    for line in table_text.splitlines():
        if not line.startswith("|"):
            continue
        if line.startswith("| ---"):
            continue
        if "期号" in line and "newsId" in line:
            continue
        if "彩种" in line and "newsId" in line:
            continue
        count += 1
    return count


def parse_declared_count(text: str, label: str) -> int:
    match = re.search(rf"- {re.escape(label)}：(\d+) 篇", text)
    if not match:
        raise AssertionError(f"未找到 `{label}` 统计行")
    return int(match.group(1))


def parse_recommend_declared(text: str) -> str:
    match = re.search(r"- 推荐位历史：(.+)", text)
    if not match:
        raise AssertionError("未找到推荐位历史统计行")
    return match.group(1).strip()


def validate_replay(path: Path, current_date: str, validation: SkillValidation) -> None:
    text = read_text(path)
    if "动态生成快照" not in text:
        raise AssertionError(f"{path.name} 缺少动态快照提示")
    if f"- 统计时间：{current_date}" not in text:
        raise AssertionError(f"{path.name} 统计时间不是 {current_date}")
    if validation.needs_prize_section not in text:
        raise AssertionError(f"{path.name} 缺少奖级/战绩章节")
    declared = parse_declared_count(text, validation.replay_count_label)
    if declared < validation.recent_min:
        raise AssertionError(f"{path.name} 历史样本不足，声明 {declared} < {validation.recent_min}")
    try:
        record_section = text.split("## 逐期记录", 1)[1]
    except IndexError as exc:
        raise AssertionError(f"{path.name} 缺少逐期记录章节") from exc
    rows = count_markdown_rows(record_section)
    if rows != declared:
        raise AssertionError(f"{path.name} 逐期记录行数 {rows} 与声明 {declared} 不一致")
    recommended_yes = record_section.count("| 是 |")
    declared_recommend = parse_recommend_declared(text)
    if declared_recommend == "无":
        if recommended_yes != 0:
            raise AssertionError(f"{path.name} 声明无推荐位，但表格中有 {recommended_yes} 条")
    else:
        declared_count = len([item for item in declared_recommend.split("、") if item])
        if declared_count != recommended_yes:
            raise AssertionError(
                f"{path.name} 推荐位声明 {declared_count} 条，与表格中 `是` 的 {recommended_yes} 条不一致"
            )


def cache_has_expected_date(current_date: str) -> bool:
    targets = [
        DATA_DIR / "dlt-support.json",
        DATA_DIR / "pl3-support.json",
        *(ROOT / slug / "references" / "replay.md" for slug in DLT_SKILLS + PL3_SKILLS),
    ]
    for path in targets:
        if not path.exists():
            return False
        text = read_text(path)
        if current_date not in text:
            return False
    return True


def validate_recent10_detail(dlt_support: dict[str, Any]) -> None:
    recent10 = dlt_support["recent10"]
    if len(recent10) < 9:
        raise AssertionError("dlt recent10 数据不足")
    required_record_keys = {
        "issueNo",
        "front",
        "back",
        "frontSum",
        "frontSpan",
        "frontOddEven",
        "frontBigSmall",
        "zone3",
        "zone5",
        "head",
        "tail",
        "frontConsecutivePairs",
        "frontRepeatWithPrevious",
        "backOddEven",
        "backBigSmall",
        "frontTailOddEven",
        "frontTailRoute012",
        "frontTailBigSmall",
    }
    missing = required_record_keys - set(recent10[0])
    if missing:
        raise AssertionError(f"dlt recent10 记录缺字段: {sorted(missing)}")


def validate_support_payloads() -> tuple[dict[str, Any], dict[str, Any]]:
    dlt_support = json.loads(read_text(DATA_DIR / "dlt-support.json"))
    pl3_support = json.loads(read_text(DATA_DIR / "pl3-support.json"))

    for required in [
        "history",
        "recent10",
        "recent10Summary",
        "recent10ZoneSupport",
        "sameIssueSupport",
        "omissionSupport",
        "transition460",
        "currentTransitionSnapshot",
        "jiangChuanPatternSupport",
    ]:
        dig(dlt_support, required)
    validate_recent10_detail(dlt_support)

    for required in ["recent18", "recent20", "positionSupport.recent18", "positionSupport.recent20"]:
        dig(pl3_support, required)
    if len(pl3_support["recent18"]) < 18 or len(pl3_support["recent20"]) < 20:
        raise AssertionError("pl3 recent18/recent20 数据不足")
    return dlt_support, pl3_support


def validate_skill_structure(skill_dir: Path) -> None:
    skill_md = skill_dir / "SKILL.md"
    replay_md = skill_dir / "references" / "replay.md"
    if not skill_md.exists():
        raise AssertionError(f"{skill_dir.name} 缺少 SKILL.md")
    if not replay_md.exists():
        raise AssertionError(f"{skill_dir.name} 缺少 references/replay.md")
    text = read_text(skill_md)
    name, description = parse_frontmatter(text)
    if name != skill_dir.name:
        raise AssertionError(f"{skill_dir.name} frontmatter.name 不匹配: {name}")
    if not description:
        raise AssertionError(f"{skill_dir.name} description 为空")
    for heading in REQUIRED_HEADINGS:
        if heading not in text:
            raise AssertionError(f"{skill_dir.name} 缺少章节: {heading}")


def validate_support_requirements(
    validation: SkillValidation,
    dlt_support: dict[str, Any],
    pl3_support: dict[str, Any],
) -> None:
    support = dlt_support if validation.lottery == "dlt" else pl3_support
    for check in validation.support_checks:
        value = dig(support, check)
        if value in (None, "", [], {}):
            raise AssertionError(f"{validation.slug} 依赖的数据为空: {check}")


def validate_official_skill(skill_dir: Path) -> None:
    if not SYSTEM_SKILL_CREATOR.exists():
        raise AssertionError(f"未找到官方 quick_validate.py: {SYSTEM_SKILL_CREATOR}")
    run(
        [sys.executable, str(SYSTEM_SKILL_CREATOR), str(skill_dir)],
        env={"PYTHONUTF8": "1"},
    )


def refresh_everything(individual: bool, current_date: str) -> None:
    try:
        run([sys.executable, str(ROOT / "scripts" / "update_dlt_replays.py"), "--all"])
    except RuntimeError as exc:
        if not (is_http_456_error(str(exc)) and cache_has_expected_date(current_date)):
            raise
        print("warning: 大乐透全量刷新遇到 HTTP 456，继续使用同日缓存数据完成验证。")
    try:
        run([sys.executable, str(ROOT / "scripts" / "update_pl3_replays.py"), "--all"])
    except RuntimeError as exc:
        if not (is_http_456_error(str(exc)) and cache_has_expected_date(current_date)):
            raise
        print("warning: 排列三全量刷新遇到 HTTP 456，继续使用同日缓存数据完成验证。")
    if individual:
        for slug in DLT_SKILLS:
            try:
                run(
                    [
                        sys.executable,
                        str(ROOT / "scripts" / "update_dlt_replays.py"),
                        "--skill",
                        slug,
                        "--skip-support-refresh",
                    ]
                )
            except RuntimeError as exc:
                replay_path = ROOT / slug / "references" / "replay.md"
                if not (is_http_456_error(str(exc)) and replay_path.exists() and current_date in read_text(replay_path)):
                    raise
                print(f"warning: {slug} 单独刷新遇到 HTTP 456，继续使用同日 replay 缓存完成验证。")
        for slug in PL3_SKILLS:
            try:
                run(
                    [
                        sys.executable,
                        str(ROOT / "scripts" / "update_pl3_replays.py"),
                        "--skill",
                        slug,
                        "--skip-support-refresh",
                    ]
                )
            except RuntimeError as exc:
                replay_path = ROOT / slug / "references" / "replay.md"
                if not (is_http_456_error(str(exc)) and replay_path.exists() and current_date in read_text(replay_path)):
                    raise
                print(f"warning: {slug} 单独刷新遇到 HTTP 456，继续使用同日 replay 缓存完成验证。")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate all expert skills, refresh scripts, and support payloads.")
    parser.add_argument("--refresh", action="store_true", help="Refresh data before validation.")
    parser.add_argument(
        "--skip-individual-refresh",
        action="store_true",
        help="Skip per-skill refresh commands after the all-in-one refresh.",
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Expected statistics date in replay files. Defaults to today.",
    )
    args = parser.parse_args()

    run(
        [
            sys.executable,
            "-m",
            "py_compile",
            str(ROOT / "scripts" / "history_support.py"),
            str(ROOT / "scripts" / "update_dlt_replays.py"),
            str(ROOT / "scripts" / "update_pl3_replays.py"),
        ]
    )

    if args.refresh:
        refresh_everything(individual=not args.skip_individual_refresh, current_date=args.date)

    dlt_support, pl3_support = validate_support_payloads()

    for validation in SKILL_VALIDATIONS:
        skill_dir = ROOT / validation.slug
        validate_skill_structure(skill_dir)
        validate_official_skill(skill_dir)
        validate_support_requirements(validation, dlt_support, pl3_support)
        validate_replay(skill_dir / "references" / "replay.md", args.date, validation)

    print("All skill, replay, and support validations passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
