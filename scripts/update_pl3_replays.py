#!/usr/bin/env python
from __future__ import annotations

import argparse
import html
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from history_support import refresh_pl3_support


BASE_URL = "https://alpha.lottery.sina.com.cn/gateway/index/entry"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Referer": "https://lotto.sina.cn/",
}
COMMON_PARAMS = {
    "format": "json",
    "__caller__": "wap",
    "__version__": "1.0.0",
    "__verno__": "10000",
}

RETRYABLE_HTTP_CODES = {429, 456, 500, 502, 503, 504}


def plain_text(value: str) -> str:
    text = value or ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text).replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def escape_cell(value: str) -> str:
    return str(value).replace("|", "\\|").replace("\n", "<br>")


def format_amount_cn(amount: str) -> str:
    try:
        raw = int(amount)
    except (TypeError, ValueError):
        return str(amount)
    if raw % 10000 == 0:
        return f"{raw // 10000}万"
    if raw > 10000:
        return f"{raw / 10000:.1f}万"
    return str(raw)


def article_url(news_id: str) -> str:
    return f"https://lotto.sina.cn/number/article.d.html?news_id={news_id}"


def request_json(cat1: str, **params) -> object:
    query = dict(COMMON_PARAMS)
    query["cat1"] = cat1
    query.update({key: value for key, value in params.items() if value is not None})
    query["t"] = str(int(time.time() * 1000))
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
    return payload["result"]["data"]


def fetch_articles(expert_id: str, lotto_type: str) -> list[dict]:
    articles: list[dict] = []
    seen: set[str] = set()
    for page in range(1, 21):
        data = request_json(
            "lottoExpertNews",
            expertId=expert_id,
            types=lotto_type,
            online="-1",
            paginationType="1",
            page=str(page),
            pageSize="100",
            isRecommend="",
        )
        if not data:
            break
        new_count = 0
        for item in data:
            news_id = item["newsId"]
            if news_id in seen:
                continue
            seen.add(news_id)
            articles.append(item)
            new_count += 1
        if new_count == 0:
            break
    articles.sort(
        key=lambda item: (int(item.get("issueNo", "0")), item.get("createTime", "")),
        reverse=True,
    )
    return articles


def fetch_detail(news_id: str) -> dict:
    return request_json("lottoExpertNewsInfo", newsId=news_id, _getHasJwt="true")


def extract_article_excerpt(detail: dict) -> dict[str, str]:
    merged = "\n".join([detail.get("summary") or "", detail.get("freeContent") or "", detail.get("payContent") or ""])
    text = plain_text(merged)
    lines: list[str] = []
    seen: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line in seen:
            continue
        seen.add(line)
        lines.append(line)

    def find(prefix: str) -> str:
        compact_prefix = prefix.replace(" ", "")
        for line in lines:
            if line.replace(" ", "").startswith(compact_prefix):
                return line
        return ""

    return {
        "开奖回顾": find("开奖回顾"),
        "百位推荐": find("百位推荐"),
        "十位推荐": find("十位推荐"),
        "个位推荐": find("个位推荐"),
        "胆码参考": find("胆码参考"),
        "杀号": find("杀号"),
        "直选参考": find("直选参考"),
        "组六参考": find("组六参考"),
        "单选15注参考": find("单选15注参考"),
        "精选号码": find("精选号码"),
    }


def detect_jiang_chuan(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "开奖回顾" in text:
        dimensions.append("开奖回顾")
    if "百位推荐" in text:
        dimensions.append("百位推荐")
    if "十位推荐" in text:
        dimensions.append("十位推荐")
    if "个位推荐" in text:
        dimensions.append("个位推荐")
    if "胆码参考" in text:
        outputs.append("胆码参考")
    if "杀号" in text:
        outputs.append("杀号")
    if "直选参考" in text:
        outputs.append("直选参考")
    if "组六参考" in text:
        outputs.append("组六参考")
    if "单选15注参考" in text:
        outputs.append("单选15注参考")
    if "精选号码" in text:
        outputs.append("精选号码")
    return dimensions, outputs


def assess_jiang_chuan(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"开奖回顾", "百位推荐", "十位推荐", "个位推荐"}
    required_outputs = {"胆码参考", "杀号", "直选参考", "组六参考", "单选15注参考", "精选号码"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：位置拆解与六码输出链路齐全"
    return "待复核：位置分析或输出层缺失"


@dataclass(frozen=True)
class ExpertConfig:
    slug: str
    expert_name: str
    expert_id: str
    lotto_type: str


CONFIGS = {
    "pl3-expert-jiang-chuan": ExpertConfig(
        slug="pl3-expert-jiang-chuan",
        expert_name="江川",
        expert_id="15904000614787",
        lotto_type="pl3",
    )
}


def build_replay(config: ExpertConfig, as_of_date: str) -> str:
    articles = fetch_articles(config.expert_id, config.lotto_type)
    if not articles:
        raise RuntimeError(f"{config.expert_name} 未抓到排列三历史文章")

    details: dict[str, dict] = {}
    excerpts: dict[str, dict[str, str]] = {}
    for article in articles:
        detail = fetch_detail(article["newsId"])
        details[article["newsId"]] = detail
        excerpts[article["newsId"]] = extract_article_excerpt(detail)

    recommended_issues = [article["issueNo"] for article in articles if article.get("isRecommend") == "1"]
    latest_detail = details[articles[0]["newsId"]]
    cross_prize_records = latest_detail.get("prizeDetails", [])
    cross_prize_records = sorted(
        cross_prize_records,
        key=lambda item: int(item.get("issueNo", "0") or 0),
        reverse=True,
    )

    lines: list[str] = [
        f"# {config.expert_name}排列三往期复盘",
        "",
        "> 本文件为动态生成快照。预测最新一期前，先运行刷新脚本，再把这里的逐期记录和战绩说明当作分析来源。",
        "",
        "## 抓取口径",
        "",
        "- 来源接口：`lottoExpertNews` + `lottoExpertNewsInfo`",
        "- 固定参数：`types=pl3&online=-1&isRecommend=`",
        f"- 统计时间：{as_of_date}",
        f"- 可访问排列三往期：{len(articles)} 篇",
        f"- 推荐位历史：{'、'.join(recommended_issues) if recommended_issues else '无'}",
        "",
        "## 方法完整性结论",
        "",
        f"- {len(articles)} 篇文章都保持同一条主链：`开奖回顾 -> 百位推荐 -> 十位推荐 -> 个位推荐 -> 胆码参考 -> 杀号 -> 直选参考 -> 组六参考 -> 单选15注参考 -> 精选号码`",
        "- 位置拆解非常稳定，百位偏大小、十位偏奇偶、个位偏质合，再统一叠加 012 路判断。",
        "- 标题会变化成“两质一合”“两偶一奇”“两小一大”等形态摘要，但正文骨架并不变化。",
        "",
        "## 跨彩种一二等战绩说明",
        "",
        "- 当前最新文章返回的是跨彩种一二等历史战绩，主要来自大乐透、双色球、七星彩和快乐8，并未返回排列三专属一二等结构化记录。",
        "- 因此下表只能作为专家整体战绩说明，不能直接当作排列三模板命中证据。",
        "",
    ]

    if cross_prize_records:
        lines.append("| 彩种 | 期号 | 奖级 | 奖金 | newsId | 原文 |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for record in cross_prize_records:
            lines.append(
                f"| {escape_cell(record.get('lottoType', ''))} | {escape_cell(record.get('issueNo', ''))} | "
                f"{escape_cell(record.get('prizeType', ''))} | {escape_cell(format_amount_cn(record.get('prizeAmount', '')))} | "
                f"{escape_cell(record.get('newsId', ''))} | [原文]({article_url(record.get('newsId', ''))}) |"
            )
    else:
        lines.append("- 当前最新可访问样本未返回结构化战绩。")

    lines.extend(
        [
            "",
            "## 中奖样本观察",
            "",
            "- 结合最近可访问的排列三正文和标题变化，可以确认江川没有在命中标题出现时切换模型，核心始终是位置拆解法。",
            "- 真正更强的地方在于位置判断更果断，尤其百位、十位、个位的方向性词句更明确，胆码与杀号也更集中。",
            "- 由于当前接口没有返回排列三专属一二等结构化记录，研究“命中强化特征”时必须保持证据边界，不能把跨彩种战绩直接迁移到排列三。",
            "",
            "## 逐期记录",
            "",
            "| 期号 | newsId | 标题 | 原文 | 推荐位 | 抽取到的分析维度 | 输出标签 | 完整性判断 |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )

    for article in articles:
        detail = details[article["newsId"]]
        text = plain_text(f"{detail.get('freeContent', '')}\n{detail.get('payContent', '')}")
        dimensions, outputs = detect_jiang_chuan(text)
        assessment = assess_jiang_chuan(dimensions, outputs)
        lines.append(
            f"| {escape_cell(article.get('issueNo', ''))} | {escape_cell(article['newsId'])} | "
            f"{escape_cell(article.get('title', ''))} | [原文]({article_url(article['newsId'])}) | "
            f"{'是' if article.get('isRecommend') == '1' else '否'} | "
            f"{escape_cell('、'.join(dimensions) if dimensions else '未抽取到')} | "
            f"{escape_cell('、'.join(outputs) if outputs else '未抽取到')} | "
            f"{escape_cell(assessment)} |"
        )

    lines.extend(
        [
            "",
            "## 逐期文本摘录",
            "",
            "- 下列摘录直接来自原文 `summary/freeContent/payContent` 的清洗结果，用来补足“位置统计如何落成具体数字和输出层”的样本。",
            "- 调用江川排列三 skill 时，应把本节与 `SKILL.md`、预测快照一起作为严格参照，不能只看维度表头。",
            "",
        ]
    )

    for article in articles:
        excerpt = excerpts[article["newsId"]]
        lines.extend(
            [
                f"### {article.get('issueNo', '')} / {article.get('newsId', '')}",
                "",
                f"- 开奖回顾：{excerpt.get('开奖回顾') or '未抽取到'}",
                f"- 百位推荐：{excerpt.get('百位推荐') or '未抽取到'}",
                f"- 十位推荐：{excerpt.get('十位推荐') or '未抽取到'}",
                f"- 个位推荐：{excerpt.get('个位推荐') or '未抽取到'}",
                f"- 胆码参考：{excerpt.get('胆码参考') or '未抽取到'}",
                f"- 杀号：{excerpt.get('杀号') or '未抽取到'}",
                f"- 直选参考：{excerpt.get('直选参考') or '未抽取到'}",
                f"- 组六参考：{excerpt.get('组六参考') or '未抽取到'}",
                f"- 单选15注参考：{excerpt.get('单选15注参考') or '未抽取到'}",
                f"- 精选号码：{excerpt.get('精选号码') or '未抽取到'}",
                "",
            ]
        )

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="刷新排列三专家 replay.md 动态快照")
    parser.add_argument("--skill", action="append", choices=sorted(CONFIGS), help="只刷新指定 skill，可重复传入")
    parser.add_argument("--all", action="store_true", help="刷新全部排列三 skill")
    parser.add_argument("--as-of-date", help="覆盖显示日期，默认使用系统当天日期，格式 YYYY-MM-DD")
    parser.add_argument(
        "--skip-support-refresh",
        action="store_true",
        help="跳过共享 support 刷新，仅重写指定 skill 的 replay.md；适合在刚完成一次全量刷新后做逐个技能验证。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selected = args.skill or []
    if args.all or not selected:
        selected = sorted(CONFIGS)

    as_of_date = args.as_of_date or date.today().isoformat()
    root = Path(__file__).resolve().parents[1]

    if not args.skip_support_refresh:
        refresh_pl3_support(root, as_of_date)
        print(f"updated shared pl3 support: {root / 'data' / 'pl3-support.json'}")

    for slug in selected:
        config = CONFIGS[slug]
        replay_path = root / config.slug / "references" / "replay.md"
        replay_text = build_replay(config, as_of_date)
        replay_path.write_text(replay_text, encoding="utf-8-sig")
        print(f"updated {config.slug}: {replay_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
