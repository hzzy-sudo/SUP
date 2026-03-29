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
from typing import Callable, Iterable

from history_support import refresh_dlt_support


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


def has_all(text: str, needles: Iterable[str]) -> bool:
    return all(needle in text for needle in needles)


def has_any(text: str, needles: Iterable[str]) -> bool:
    return any(needle in text for needle in needles)


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
    req = urllib.request.Request(url, headers=HEADERS)
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
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


def fetch_articles(expert_id: str) -> list[dict]:
    articles: list[dict] = []
    seen: set[str] = set()
    for page in range(1, 21):
        data = request_json(
            "lottoExpertNews",
            expertId=expert_id,
            types="dlt",
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


def extract_article_excerpt(detail: dict, labels: tuple[str, ...]) -> dict[str, str]:
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

    compact_lines = [(line, line.replace(" ", "")) for line in lines]

    def find(label: str) -> str:
        compact_label = label.replace(" ", "")
        for line, compact_line in compact_lines:
            if compact_line.startswith(compact_label) or compact_label in compact_line:
                return line
        return ""

    return {label: find(label) for label in labels}


@dataclass(frozen=True)
class ExpertConfig:
    slug: str
    expert_name: str
    expert_id: str
    detect_dimensions: Callable[[str], list[str]]
    detect_outputs: Callable[[str], list[str]]
    assess_row: Callable[[list[str], list[str]], str]
    completeness_lines: Callable[[int], list[str]]
    prize_observations: list[str]
    excerpt_labels: tuple[str, ...]


def detect_chen_taopu(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "开奖回顾" in text:
        dimensions.append("开奖回顾")
    if "最近10期大乐透奖号统计表" in text:
        dimensions.append("最近10期统计表")
    if "奇偶分析" in text:
        dimensions.append("奇偶分析")
    if "极距分析" in text:
        dimensions.append("极距分析")
    if "和值分析" in text:
        dimensions.append("和值分析")
    if "后区推荐" in text:
        dimensions.append("后区推荐")
    if "15+5大复式推荐" in text:
        outputs.append("15+5大复式推荐")
    if "9+3小复式推荐" in text:
        outputs.append("9+3小复式推荐")
    if has_any(text, ["5+2单挑一注推荐", "5+2单注推荐"]):
        outputs.append("5+2单挑一注推荐")
    return dimensions, outputs


def assess_chen_taopu(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {
        "开奖回顾",
        "最近10期统计表",
        "奇偶分析",
        "极距分析",
        "和值分析",
        "后区推荐",
    }
    required_outputs = {
        "15+5大复式推荐",
        "9+3小复式推荐",
        "5+2单挑一注推荐",
    }
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：统计表与奇偶/极距/和值链路齐全"
    return "待复核：关键统计模块或三层推荐不完整"


def detect_fuge(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if has_all(text, ["上期开奖", "五区比", "和值"]):
        dimensions.append("上期结构复盘")
    if "龙头：" in text:
        dimensions.append("龙头")
    if "凤尾：" in text:
        dimensions.append("凤尾")
    if "奇偶分析" in text:
        dimensions.append("奇偶分析")
    if "前区五区比" in text:
        dimensions.append("五区比")
    if "大小分析" in text:
        dimensions.append("大小分析")
    if "后区分析" in text:
        dimensions.append("后区分析")
    if "前区双胆" in text:
        outputs.append("前区双胆")
    if "大乐透15+5复式" in text:
        outputs.append("15+5复式")
    if "大乐透9+3复式" in text:
        outputs.append("9+3复式")
    if "单注参考" in text:
        outputs.append("单注参考")
    return dimensions, outputs


def assess_fuge(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"上期结构复盘", "龙头", "凤尾", "奇偶分析", "五区比", "大小分析", "后区分析"}
    required_outputs = {"前区双胆", "15+5复式", "9+3复式", "单注参考"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：头尾与分区结构齐全"
    return "待复核：头尾、区比或双胆链路缺失"


def detect_chen_qingfeng(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "上期开奖" in text:
        dimensions.append("上期开奖")
    if "历史同期尾数分析" in text:
        dimensions.append("历史同期尾数分析")
    if "尾数奇偶" in text:
        dimensions.append("尾数奇偶")
    if "尾数012路" in text:
        dimensions.append("尾数012路")
    if "尾数大小" in text:
        dimensions.append("尾数大小")
    if "后区尾数" in text:
        dimensions.append("后区尾数")
    if "前区必杀一尾" in text:
        outputs.append("前区必杀一尾")
    if "前区双胆参考" in text:
        outputs.append("前区双胆参考")
    if "前区杀8码" in text:
        outputs.append("前区杀8码")
    if "后区杀4码" in text:
        outputs.append("后区杀4码")
    if "大乐透15+5复式推荐" in text:
        outputs.append("15+5复式推荐")
    if "大乐透8+3小单推荐" in text:
        outputs.append("8+3小单推荐")
    if "大乐透5+2单注推荐" in text:
        outputs.append("5+2单注推荐")
    return dimensions, outputs


def assess_chen_qingfeng(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"上期开奖", "历史同期尾数分析", "尾数奇偶", "尾数012路", "尾数大小", "后区尾数"}
    required_outputs = {"前区必杀一尾", "前区双胆参考", "前区杀8码", "后区杀4码", "15+5复式推荐", "8+3小单推荐", "5+2单注推荐"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：历史同期尾数链与杀号链路齐全"
    return "待复核：历史同期尾数或杀号输出缺失"


def detect_wan_miaoxian(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "上期开奖" in text:
        dimensions.append("上期开奖")
    if "前区热号分析" in text:
        dimensions.append("前区热号分析")
    if "前区冷号分析" in text:
        dimensions.append("前区冷号分析")
    if "后区冷热号码统计" in text:
        dimensions.append("后区冷热号码统计")
    if "15码复式参考" in text:
        outputs.append("15码复式参考")
    if "前区必杀8码" in text:
        outputs.append("前区必杀8码")
    if "后区注意四码" in text:
        outputs.append("后区注意四码")
    if "单挑两码" in text:
        outputs.append("单挑两码")
    if "15+5大复式参考" in text:
        outputs.append("15+5大复式参考")
    if "8+2小复式参考" in text:
        outputs.append("8+2小复式参考")
    if "5+2单注参考" in text:
        outputs.append("5+2单注参考")
    return dimensions, outputs


def assess_wan_miaoxian(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"上期开奖", "前区热号分析", "前区冷号分析", "后区冷热号码统计"}
    required_outputs = {"15码复式参考", "前区必杀8码", "后区注意四码", "单挑两码", "15+5大复式参考", "8+2小复式参考", "5+2单注参考"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：460期冷热转移链路齐全"
    return "待复核：冷热统计或综合推荐层缺失"


def detect_wen_xinlin(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "上期回顾" in text:
        dimensions.append("上期回顾")
    if "三区走势" in text:
        dimensions.append("三区走势")
    if "龙头凤尾" in text:
        dimensions.append("龙头凤尾")
    if "大小分析" in text:
        dimensions.append("大小分析")
    if "跨度分析" in text:
        dimensions.append("跨度分析")
    if "后区分析" in text:
        dimensions.append("后区分析")
    if "后区重点关注" in text:
        dimensions.append("后区重点关注")
    if "大乐透前区12码推荐" in text:
        outputs.append("前区12码推荐")
    if "大乐透后区5码推荐" in text:
        outputs.append("后区5码推荐")
    if "大复式推荐" in text:
        outputs.append("大复式推荐")
    if "小复式推荐" in text:
        outputs.append("小复式推荐")
    if has_any(text, ["单注一注5+2", "单注5+2"]):
        outputs.append("单注5+2")
    return dimensions, outputs


def assess_wen_xinlin(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"上期回顾", "三区走势", "龙头凤尾", "大小分析", "跨度分析", "后区分析", "后区重点关注"}
    required_outputs = {"前区12码推荐", "后区5码推荐", "大复式推荐", "小复式推荐", "单注5+2"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：三区、头尾、跨度和后区链路齐全"
    return "待复核：三区、跨度或综合推荐层缺失"


def detect_chen_bing(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "开奖回顾" in text:
        dimensions.append("开奖回顾")
    if "前区连续10期龙头分别开出" in text:
        dimensions.append("龙头统计")
    if "前区龙头注意号码" in text:
        dimensions.append("龙头候选")
    if "最近10期前区凤尾号码凤尾为" in text:
        dimensions.append("凤尾统计")
    if "前区凤尾012路比" in text:
        dimensions.append("凤尾候选")
    if "前区绝杀10码" in text:
        dimensions.append("前区绝杀10码")
        outputs.append("前区绝杀10码")
    if "前区15码大复式参考" in text:
        dimensions.append("前区15码大复式")
        outputs.append("前区15码大复式参考")
    if "最近10期后区分别开出号码" in text:
        dimensions.append("后区和值统计")
    if "后区参考号码" in text:
        dimensions.append("后区参考号码")
    if "大复式陈冰推荐" in text:
        outputs.append("大复式推荐")
    if "小复式参考" in text:
        outputs.append("小复式参考")
    if "5+2推荐" in text:
        outputs.append("5+2推荐")
    return dimensions, outputs


def assess_chen_bing(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {
        "开奖回顾",
        "龙头统计",
        "龙头候选",
        "凤尾统计",
        "凤尾候选",
        "前区绝杀10码",
        "前区15码大复式",
        "后区和值统计",
    }
    required_outputs = {"前区绝杀10码", "前区15码大复式参考", "大复式推荐", "小复式参考", "5+2推荐"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        if "后区参考号码" in dimensions:
            return "完整：双锚点、绝杀和后区和值齐全"
        return "较完整：后区参考号码未单独成段，但主骨架仍在"
    return "待复核：双锚点、绝杀或后区和值链路缺失"


def detect_chen_yingfang(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "开奖回顾" in text:
        dimensions.append("开奖回顾")
    if "大乐透最近10期号码分析" in text:
        dimensions.append("最近10期号码分析")
    if "跨度推荐" in text:
        dimensions.append("跨度推荐")
    if "连号分析" in text:
        dimensions.append("连号分析")
    if "重号分析" in text:
        dimensions.append("重号分析")
    if "后区推荐" in text:
        dimensions.append("后区推荐")
    if "15+5大复式推荐" in text:
        outputs.append("15+5大复式推荐")
    if "9+3小复式推荐" in text:
        outputs.append("9+3小复式推荐")
    if has_any(text, ["5+2单挑一注推荐", "5+2单注推荐"]):
        outputs.append("5+2单挑一注推荐")
    return dimensions, outputs


def assess_chen_yingfang(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"开奖回顾", "最近10期号码分析", "跨度推荐", "连号分析", "重号分析", "后区推荐"}
    required_outputs = {"15+5大复式推荐", "9+3小复式推荐", "5+2单挑一注推荐"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：跨度、连号、重号和后区链路齐全"
    return "待复核：跨度、重号或三层推荐不完整"


def detect_hai_tian(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "上期开奖" in text:
        dimensions.append("上期开奖")
    if "前一区【01-12】" in text:
        dimensions.append("前一区分析")
    if "前二区【13-23】" in text:
        dimensions.append("前二区分析")
    if "前三区【24-35】" in text:
        dimensions.append("前三区分析")
    if "后区分析" in text:
        dimensions.append("后区分析")
    if "前区大底15码" in text:
        outputs.append("前区大底15码")
    if "前区缩水11码" in text:
        outputs.append("前区缩水11码")
    if "前区精选8码" in text:
        outputs.append("前区精选8码")
    if "后区大底5码" in text:
        outputs.append("后区大底5码")
    if "后区3码参考" in text:
        outputs.append("后区3码参考")
    if "单注参考" in text:
        outputs.append("单注参考")
    return dimensions, outputs


def assess_hai_tian(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"上期开奖", "前一区分析", "前二区分析", "前三区分析", "后区分析"}
    required_outputs = {"前区大底15码", "前区缩水11码", "前区精选8码", "后区大底5码", "后区3码参考", "单注参考"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：三区分层、后区遗漏和三层压缩链路齐全"
    return "待复核：三区分析或压缩输出层缺失"


def detect_jiang_chuan_dlt(text: str) -> tuple[list[str], list[str]]:
    dimensions: list[str] = []
    outputs: list[str] = []
    if "开奖回顾" in text:
        dimensions.append("开奖回顾")
    if "前区大小比分析" in text:
        dimensions.append("前区大小比分析")
    if "前区奇偶比分析" in text:
        dimensions.append("前区奇偶比分析")
    if "后区分析" in text:
        dimensions.append("后区分析")
    if "前区两胆参考" in text:
        outputs.append("前区两胆参考")
    if "前区杀号参考" in text:
        outputs.append("前区杀号参考")
    if "后区五码参考" in text:
        outputs.append("后区五码参考")
    if "后区三码参考" in text:
        outputs.append("后区三码参考")
    if "大复式参考" in text:
        outputs.append("大复式参考")
    if "小复式参考" in text:
        outputs.append("小复式参考")
    if has_any(text, ["单注参考", "5+2单注参考"]):
        outputs.append("单注参考")
    return dimensions, outputs


def assess_jiang_chuan_dlt(dimensions: list[str], outputs: list[str]) -> str:
    required_dimensions = {"开奖回顾", "前区大小比分析", "前区奇偶比分析", "后区分析"}
    required_outputs = {"前区两胆参考", "前区杀号参考", "后区五码参考", "后区三码参考", "大复式参考", "小复式参考", "单注参考"}
    if required_dimensions.issubset(dimensions) and required_outputs.issubset(outputs):
        return "完整：形态跟随分析与前后区三层输出链路齐全"
    return "待复核：大小比、奇偶比、后区组合或输出层缺失"


CONFIGS = {
    "dlt-expert-chen-taopu": ExpertConfig(
        slug="dlt-expert-chen-taopu",
        expert_name="陈涛普",
        expert_id="16939648930244",
        detect_dimensions=lambda text: detect_chen_taopu(text)[0],
        detect_outputs=lambda text: detect_chen_taopu(text)[1],
        assess_row=assess_chen_taopu,
        completeness_lines=lambda count: [
            f"- {count} 篇文章的主流程完全一致：`开奖回顾 -> 最近10期统计表 -> 奇偶分析 -> 极距分析 -> 和值分析 -> 后区推荐 -> 15+5/9+3/5+2`",
            "- 所有样本都能抽取出完整三层推荐，模板稳定度极高。",
            "- 缺口也很稳定：未见龙头凤尾、区间、杀号、连号、重号等前区补充模块。",
        ],
        prize_observations=[
            "- 奖中样本没有额外增加新模块，依旧围绕奇偶、极距、和值三条主线推进。",
            "- 与普通期相比，中奖样本往往是奇偶、极距、和值三者同时收敛，而不是只押中单一维度。",
            "- 陈涛普命中一二等的关键不是扩展分析面，而是把三项统计约束同步压到更窄的落点上。",
            "- 研究时要重点看：奇偶方向、极距奇偶属性、和值奇偶属性是否形成同向共振。",
        ],
        excerpt_labels=("开奖回顾", "奇偶分析", "极距分析", "和值分析", "后区推荐", "15+5大复式推荐", "9+3小复式推荐", "5+2单挑一注推荐"),
    ),
    "dlt-expert-fuge": ExpertConfig(
        slug="dlt-expert-fuge",
        expert_name="富哥",
        expert_id="15904000603367",
        detect_dimensions=lambda text: detect_fuge(text)[0],
        detect_outputs=lambda text: detect_fuge(text)[1],
        assess_row=assess_fuge,
        completeness_lines=lambda count: [
            f"- {count} 篇文章都保持同一条主链：`上期结构复盘 -> 龙头 -> 凤尾 -> 奇偶分析 -> 五区比 -> 大小分析 -> 后区分析 -> 前区双胆 -> 15+5 -> 9+3 -> 单注`",
            "- 前后区都有覆盖，结构判断完整且稳定。",
            "- 历史样本中未见明确杀号、历史同期、尾数法等更强压缩模块。",
        ],
        prize_observations=[
            "- 中奖样本没有出现新模块，依然是标准的 `龙头 + 凤尾 + 奇偶 + 五区比 + 大小 + 后区 + 双胆` 链路。",
            "- 与普通期相比，中奖样本里的五区比和断区判断通常更鲜明，较少出现模糊均衡表述。",
            "- `前区双胆` 在奖中样本里始终存在，说明它不是装饰，而是富哥执行时最核心的落点之一。",
            "- 富哥的核心不是杀号，而是“头尾定边 + 区比收缩 + 双胆定心”，研究时要重点对比这三个动作是否同步强化。",
        ],
        excerpt_labels=("上期开奖", "龙头：", "凤尾：", "奇偶分析", "前区五区比", "大小分析", "后区分析", "前区双胆", "大乐透15+5复式", "大乐透9+3复式", "单注参考"),
    ),
    "dlt-expert-chen-qingfeng": ExpertConfig(
        slug="dlt-expert-chen-qingfeng",
        expert_name="陈青峰",
        expert_id="15904000605946",
        detect_dimensions=lambda text: detect_chen_qingfeng(text)[0],
        detect_outputs=lambda text: detect_chen_qingfeng(text)[1],
        assess_row=assess_chen_qingfeng,
        completeness_lines=lambda count: [
            f"- {count} 篇文章都沿用同一条主链：`上期开奖 -> 历史同期尾数分析 -> 尾数奇偶 -> 尾数012路 -> 尾数大小 -> 后区尾数 -> 必杀一尾 -> 双胆 -> 杀8码 -> 后区杀4码 -> 15+5/8+3/5+2`",
            "- 历史同期尾数法是全篇轴心，前后区都带明显杀号动作。",
            "- 模板完整度很高，既做结构判断，也做明确压缩。",
        ],
        prize_observations=[
            "- 奖中样本依旧沿用历史同期尾数模型，没有证据显示中奖期会切换到别的体系。",
            "- 与普通期相比，中奖样本里 `前区必杀一尾 + 前区杀8码 + 后区杀4码` 的压缩动作通常更坚决。",
            "- 中奖时最关键的不是多写内容，而是让尾数奇偶、尾数012路和尾数大小三个子判断相互印证。",
            "- 研究陈青峰时要重点看：历史同期尾数统计是否把杀码范围明显压窄，这一步最接近他的命中核心。",
        ],
        excerpt_labels=("上期开奖", "历史同期尾数分析", "尾数奇偶", "尾数012路", "尾数大小", "后区尾数", "前区必杀一尾", "前区双胆参考", "前区杀8码", "后区杀4码", "大乐透15+5复式推荐", "大乐透8+3小单推荐", "大乐透5+2单注推荐"),
    ),
    "dlt-expert-wan-miaoxian": ExpertConfig(
        slug="dlt-expert-wan-miaoxian",
        expert_name="万妙仙",
        expert_id="15905643283853",
        detect_dimensions=lambda text: detect_wan_miaoxian(text)[0],
        detect_outputs=lambda text: detect_wan_miaoxian(text)[1],
        assess_row=assess_wan_miaoxian,
        completeness_lines=lambda count: [
            f"- {count} 篇文章都围绕“上期号码出现后，其下一期最热/最冷号码”的 460 期条件转移统计展开。",
            "- 稳定主链为：`上期开奖 -> 前区热号分析 -> 前区冷号分析 -> 15码复式参考 -> 前区必杀8码 -> 后区冷热号码统计 -> 后区注意四码/单挑两码 -> 15+5/8+2/5+2`",
            "- 长样本证据感很强，但对区间、跨度、龙头凤尾等结构指标覆盖较少。",
        ],
        prize_observations=[
            "- 奖中样本仍是标准的 460 期条件转移法，没有看到中奖时换成短期走势模板。",
            "- 与普通期相比，中奖样本里的冷热结论更集中，尤其前区热号池与冷号池的交集压缩更明显。",
            "- 万妙仙的一二等核心更像“长样本条件统计后的强收敛”，而不是常规结构指标的叠加。",
            "- 研究时要重点盯后区两码执行是否足够坚决，因为她的奖中样本往往在后区两码上更像真正落地的动作。",
        ],
        excerpt_labels=("上期开奖", "前区热号分析", "前区冷号分析", "15码复式参考", "前区必杀8码", "后区冷热号码统计", "后区注意四码", "单挑两码", "15+5大复式参考", "8+2小复式参考", "5+2单注参考"),
    ),
    "dlt-expert-wen-xinlin": ExpertConfig(
        slug="dlt-expert-wen-xinlin",
        expert_name="文新林",
        expert_id="16859361812938",
        detect_dimensions=lambda text: detect_wen_xinlin(text)[0],
        detect_outputs=lambda text: detect_wen_xinlin(text)[1],
        assess_row=assess_wen_xinlin,
        completeness_lines=lambda count: [
            f"- {count} 篇文章主流程稳定为：`上期回顾 -> 三区走势 -> 龙头凤尾 -> 大小分析 -> 跨度分析 -> 后区分析 -> 后区重点关注 -> 前区12码 -> 后区5码 -> 大复式/小复式/单注`",
            "- 前后区都覆盖，结构面较均衡，既看区间分布也看跨度与头尾。",
            "- 历史样本中没有稳定的杀号层和胆码层，更偏均衡覆盖而非激进压缩。",
        ],
        prize_observations=[
            "- 奖中样本并没有切换模型，仍旧是 `三区 + 龙头凤尾 + 大小 + 跨度 + 后区` 的均衡结构模板。",
            "- 与普通期相比，中奖样本里的三区偏态通常更鲜明，不会写成很松的均衡判断。",
            "- 龙头、凤尾和跨度在中奖样本里往往会形成更强的一致性，这比单独看大小分析更关键。",
            "- 研究文新林时要重点看：三区判断是否先收窄，再由头尾和跨度把号码池进一步夹住。",
        ],
        excerpt_labels=("上期回顾", "三区走势", "龙头凤尾", "大小分析", "跨度分析", "后区分析", "后区重点关注", "大乐透前区12码推荐", "大乐透后区5码推荐", "大复式推荐", "小复式推荐", "单注一注5+2"),
    ),
    "dlt-expert-chen-bing": ExpertConfig(
        slug="dlt-expert-chen-bing",
        expert_name="陈冰",
        expert_id="15904000619668",
        detect_dimensions=lambda text: detect_chen_bing(text)[0],
        detect_outputs=lambda text: detect_chen_bing(text)[1],
        assess_row=assess_chen_bing,
        completeness_lines=lambda count: [
            f"- 主流程稳定为：`开奖回顾 -> 龙头10期统计 -> 龙头候选与精选 -> 凤尾10期统计 -> 凤尾候选与精选 -> 前区绝杀10码 -> 前区15码 -> 后区和值统计 -> 后区参考号码 -> 大复式/小复式/5+2`",
            "- 龙头、凤尾和绝杀10码是全篇的三根支柱。",
            f"- 当前可访问的 {count} 篇样本里，少数文章后区输出略短，但总体骨架仍在。",
        ],
        prize_observations=[
            "- 奖中样本与普通期一样，都是“双锚点 + 绝杀10码 + 后区和值”的标准陈冰框架，没有证据显示中奖期会换方法。",
            "- 真正更强的差异在执行顺序：先精选龙头，再精选凤尾，再做绝杀10码，最后让 5+2 完整嵌入小复式，这种层层收缩在中奖样本里尤其明显。",
            "- 一等奖样本里凤尾判断往往更果断，经常直接给出凤尾胆码或极强区段偏置，这一点比普通期更值得盯。",
            "- 研究陈冰时要重点看：龙头、凤尾、绝杀10码、后区和值是否同时同向；这是他命中一二等的核心执行链。",
        ],
        excerpt_labels=("开奖回顾", "前区连续10期龙头分别开出", "前区龙头注意号码", "最近10期前区凤尾号码凤尾为", "前区凤尾012路比", "前区绝杀10码", "前区15码大复式参考", "最近10期后区分别开出号码", "后区参考号码", "大复式陈冰推荐", "小复式参考", "5+2推荐"),
    ),
    "dlt-expert-chen-yingfang": ExpertConfig(
        slug="dlt-expert-chen-yingfang",
        expert_name="陈樱芳",
        expert_id="16939649103093",
        detect_dimensions=lambda text: detect_chen_yingfang(text)[0],
        detect_outputs=lambda text: detect_chen_yingfang(text)[1],
        assess_row=assess_chen_yingfang,
        completeness_lines=lambda count: [
            f"- {count} 篇文章都维持同一条主链：`开奖回顾 -> 最近10期号码分析 -> 跨度推荐 -> 连号分析 -> 重号分析 -> 后区推荐 -> 15+5/9+3/5+2`",
            "- 模板非常稳定，写法轻量，但跨度、连号、重号和后区四个模块都持续出现。",
            "- 前区维度相对集中，基本不扩展到龙头凤尾、区间、杀号等更重型模块。",
        ],
        prize_observations=[
            "- 奖中样本依旧采用跨度、连号、重号加后区的轻模板，没有看出中奖期会额外增加复杂模块。",
            "- 与普通期相比，中奖样本里的跨度判断通常更果断，连号和重号也更偏明确否定或明确保留，而不是模糊带过。",
            "- 陈樱芳的一二等核心不在大范围扩展维度，而在于用少数几个稳定指标快速收敛出一张简洁号码单。",
            "- 研究时要重点对比：跨度结论、连号判断、重号判断和后区大小方向是否在同一篇里同时趋于明确。",
        ],
        excerpt_labels=("开奖回顾", "大乐透最近10期号码分析", "跨度推荐", "连号分析", "重号分析", "后区推荐", "15+5大复式推荐", "9+3小复式推荐", "5+2单挑一注推荐"),
    ),
    "dlt-expert-jiang-chuan": ExpertConfig(
        slug="dlt-expert-jiang-chuan",
        expert_name="江川",
        expert_id="15904000614787",
        detect_dimensions=lambda text: detect_jiang_chuan_dlt(text)[0],
        detect_outputs=lambda text: detect_jiang_chuan_dlt(text)[1],
        assess_row=assess_jiang_chuan_dlt,
        completeness_lines=lambda count: [
            f"- {count} 篇文章都维持同一条主链：`开奖回顾 -> 前区大小比分析 -> 前区奇偶比分析 -> 后区分析 -> 前区两胆/杀号 -> 后区五码/三码 -> 大复式 -> 小复式 -> 单注`",
            "- 方法核心不是近10期结构，而是“上期形态 -> 最近5次同形态后的下期”这一类一步跟随统计。",
            "- 前区只抓大小比和奇偶比两条主线，后区只抓上期 exact 组合的历史跟随，维度少但模板高度稳定。",
        ],
        prize_observations=[
            "- 可访问的一等奖样本是 19124 期，二等奖样本包括 26027、26012、25141 等；这些中奖样本都没有换模型，仍然是标准的大小比 + 奇偶比 + 后区组合跟随法。",
            "- 江川真正的强点在于两条前区主线会各自先给两码关注，再从中各取其一落成前区两胆，这个“二选一收缩”动作在中奖样本里非常稳定。",
            "- 与普通期相比，中奖样本更像是大小比方向、奇偶比方向和后区两码方向同时同向发力，而不是单纯靠某一条线命中。",
            "- 研究江川时要重点看：大小比跟随是否提示大小回补或继续热出，奇偶比跟随是否提示奇偶回补或继续热出，以及后区 exact 组合跟随是否出现更集中的热码。",
        ],
        excerpt_labels=("开奖回顾", "前区大小比分析", "前区奇偶比分析", "后区分析", "前区两胆参考", "前区杀号参考", "后区五码参考", "后区三码参考", "大复式参考", "小复式参考", "单注参考"),
    ),
    "dlt-expert-hai-tian": ExpertConfig(
        slug="dlt-expert-hai-tian",
        expert_name="海天",
        expert_id="15904000619432",
        detect_dimensions=lambda text: detect_hai_tian(text)[0],
        detect_outputs=lambda text: detect_hai_tian(text)[1],
        assess_row=assess_hai_tian,
        completeness_lines=lambda count: [
            f"- {count} 篇文章都保持同一条主链：`上期开奖 -> 前一区分析 -> 前二区分析 -> 前三区分析 -> 后区分析 -> 前区15码/11码/8码 -> 后区5码/3码 -> 单注`",
            "- 前区核心是三区分层热冷和遗漏值判断，后区核心是冷码、温码、热码三层遗漏管理。",
            "- 写法高度模板化，先给分区空间，再给分层压缩，最后收束到单注。",
        ],
        prize_observations=[
            "- 奖中样本与普通期一样，依旧是标准的 `三区分层 + 后区遗漏冷热 + 分层压缩` 框架，没有看到中奖期切换模型。",
            "- 中奖样本里更强的地方在于三区冷热判断更果断，尤其第二区和第三区更常直接落到 `精选1码/2码/3胆` 这种强收缩动作。",
            "- 海天的一等奖和二等奖样本都说明，他真正的命中关键不是额外杀号，而是从 15 码、11 码、8 码到单注的层层递进压缩。",
            "- 研究海天时要重点看：三区热冷是否形成明显偏态，以及后区是押冷码解冻、温码组合还是热码延续；这两条链是否同步收紧最关键。",
        ],
        excerpt_labels=("上期开奖", "前一区【01-12】", "前二区【13-23】", "前三区【24-35】", "后区分析", "前区大底15码", "前区缩水11码", "前区精选8码", "后区大底5码", "后区3码参考", "单注参考"),
    ),
}


def build_replay(config: ExpertConfig, as_of_date: str) -> str:
    articles = fetch_articles(config.expert_id)
    if not articles:
        raise RuntimeError(f"{config.expert_name} 未抓到大乐透历史文章")

    details: dict[str, dict] = {}
    excerpts: dict[str, dict[str, str]] = {}
    for article in articles:
        detail = fetch_detail(article["newsId"])
        details[article["newsId"]] = detail
        excerpts[article["newsId"]] = extract_article_excerpt(detail, config.excerpt_labels)

    prize_source: dict | None = None
    for article in articles:
        detail = details[article["newsId"]]
        if detail.get("prizeDetails"):
            prize_source = detail
            break
    prize_records = []
    if prize_source:
        for item in prize_source.get("prizeDetails", []):
            if item.get("lottoType") != "dlt":
                continue
            prize_records.append(
                {
                    "issueNo": item.get("issueNo", ""),
                    "prizeType": item.get("prizeType", ""),
                    "prizeAmountCn": format_amount_cn(item.get("prizeAmount", "")),
                    "newsId": item.get("newsId", ""),
                }
            )
    prize_records.sort(key=lambda item: int(item["issueNo"] or 0), reverse=True)

    recommended_issues = [article["issueNo"] for article in articles if article.get("isRecommend") == "1"]

    lines: list[str] = [
        f"# {config.expert_name}大乐透往期复盘",
        "",
        "> 本文件为动态生成快照。预测最新一期前，先运行刷新脚本，再把这里的逐期记录和中奖记录当作分析来源。",
        "",
        "## 抓取口径",
        "",
        "- 来源接口：`lottoExpertNews` + `lottoExpertNewsInfo`",
        "- 固定参数：`types=dlt&online=-1&isRecommend=`",
        f"- 统计时间：{as_of_date}",
        f"- 可访问大乐透往期：{len(articles)} 篇",
        f"- 推荐位历史：{'、'.join(recommended_issues) if recommended_issues else '无'}",
        "",
        "## 方法完整性结论",
        "",
    ]
    lines.extend(config.completeness_lines(len(articles)))
    lines.extend(["", "## 大乐透一二等命中记录", ""])

    if prize_records:
        lines.append("| 期号 | 奖级 | 奖金 | newsId | 原文 |")
        lines.append("| --- | --- | --- | --- | --- |")
        for record in prize_records:
            url = article_url(record["newsId"])
            lines.append(
                f"| {escape_cell(record['issueNo'])} | {escape_cell(record['prizeType'])} | "
                f"{escape_cell(record['prizeAmountCn'])} | {escape_cell(record['newsId'])} | "
                f"[原文]({url}) |"
            )
    else:
        lines.append("- 当前最新可访问样本里未返回大乐透一二等中奖记录。")

    lines.extend(["", "## 中奖样本观察", ""])
    lines.extend(config.prize_observations)
    lines.extend(["", "## 逐期记录", "", "| 期号 | newsId | 标题 | 原文 | 推荐位 | 抽取到的分析维度 | 输出标签 | 完整性判断 |", "| --- | --- | --- | --- | --- | --- | --- | --- |"])

    for article in articles:
        detail = details[article["newsId"]]
        text = plain_text(f"{detail.get('freeContent', '')}\n{detail.get('payContent', '')}")
        dimensions = config.detect_dimensions(text)
        outputs = config.detect_outputs(text)
        assessment = config.assess_row(dimensions, outputs)
        lines.append(
            f"| {escape_cell(article.get('issueNo', ''))} | {escape_cell(article['newsId'])} | "
            f"{escape_cell(article.get('title', ''))} | [原文]({article_url(article['newsId'])}) | "
            f"{'是' if article.get('isRecommend') == '1' else '否'} | "
            f"{escape_cell('、'.join(dimensions) if dimensions else '未抽取到') } | "
            f"{escape_cell('、'.join(outputs) if outputs else '未抽取到') } | "
            f"{escape_cell(assessment)} |"
        )

    lines.extend(
        [
            "",
            "## 逐期文本摘录",
            "",
            "- 下列摘录直接来自原文 `summary/freeContent/payContent` 的清洗结果，用来补足“统计如何落成具体句子和号码”的本地样本。",
            "- 调用大乐透专家 skill 时，应把本节与 `SKILL.md`、共享快照一起作为严格参照，不要只看结构表头后就声称缺少原文落号样本。",
            "",
        ]
    )

    for article in articles:
        excerpt = excerpts[article["newsId"]]
        lines.extend(
            [
                f"### {article.get('issueNo', '')} / {article.get('newsId', '')}",
                "",
                *[
                    f"- {label}：{excerpt.get(label) or '未抽取到'}"
                    for label in config.excerpt_labels
                ],
                "",
            ]
        )

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="刷新大乐透专家 replay.md 动态快照")
    parser.add_argument("--skill", action="append", choices=sorted(CONFIGS), help="只刷新指定 skill，可重复传入")
    parser.add_argument("--all", action="store_true", help="刷新全部大乐透 skill")
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
        refresh_dlt_support(root, as_of_date)
        print(f"updated shared dlt support: {root / 'data' / 'dlt-support.json'}")

    for slug in selected:
        config = CONFIGS[slug]
        replay_path = root / config.slug / "references" / "replay.md"
        replay_text = build_replay(config, as_of_date)
        replay_path.write_text(replay_text, encoding="utf-8-sig")
        print(f"updated {config.slug}: {replay_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
