# Supser Terminology Glossary

## Purpose

This file defines the shared terms used by the `supser` data pipeline and expert skills.
It is a normalization reference for common statistics only. It does not override an expert's
house style or output phrasing unless the skill explicitly chooses to.

## DLT Core Terms

- `前区`: The 5 red balls in Super Lotto. Value range: `01-35`.
- `后区`: The 2 blue balls in Super Lotto. Value range: `01-12`.
- `奇偶比`: Ratio order is always `odd:even`.
- `大小比`: Ratio order is always `small:big`.
- `跨度`: `max(nums) - min(nums)`.
- `极距`: In the current shared data model, this is treated as the same metric as `跨度`
  unless a skill explicitly defines a different expert-specific meaning.

## DLT Shared Thresholds

- `前区大小分界`: `01-17` is `small`, `18-35` is `big`.
- `后区大小分界`: `01-06` is `small`, `07-12` is `big`.
- `前区三区`:
  - `一区`: `01-12`
  - `二区`: `13-23`
  - `三区`: `24-35`
- `前区五区`:
  - `一区`: `01-07`
  - `二区`: `08-14`
  - `三区`: `15-21`
  - `四区`: `22-28`
  - `五区`: `29-35`
- `012路`: Value modulo 3.
  - `0路`: `num % 3 == 0`
  - `1路`: `num % 3 == 1`
  - `2路`: `num % 3 == 2`

## DLT Derived Terms

- `龙头`: The smallest number in the front area.
- `凤尾`: The largest number in the front area.
- `尾数`: The last digit of a number, `num % 10`.
- `连号`: Adjacent numbers in sorted order, for example `03-04`.
- `重号`: In the shared normalized history, this means repeat numbers versus the previous draw.
  If a skill uses a different repeat window, that skill definition wins.
- `遗漏值`: Number of consecutive draws since a number last appeared.
- `热码 / 温码 / 冷码`: These labels are skill-level interpretations. The shared data layer only
  provides raw omission and frequency support; thresholds may differ by expert.

## PL3 Core Terms

- `百位 / 十位 / 个位`: The three positions in PL3.
- `奇偶比`: Ratio order is always `odd:even`.
- `大小比`: Ratio order is always `small:big`.
- `PL3 大小分界`: `0-4` is `small`, `5-9` is `big`.
- `质合`: Prime/composite over a single digit.
  - `质`: `2, 3, 5, 7`
  - `合`: `0, 1, 4, 6, 8, 9`
- `012路`: Digit modulo 3.

## Interpretation Rules

- Expert skills may reuse the same statistical terms with different emphasis. Shared terminology
  overlap is normal and should not be treated as cross-expert contamination by itself.
- Shared terms describe the data model, not a forced writing template.
- When a skill and this glossary conflict, the skill wins for expert-specific narrative behavior,
  while the shared data calculation continues to follow the pipeline definitions above.
