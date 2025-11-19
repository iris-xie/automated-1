# crawl_with_firecrawl.py 使用说明

本脚本通过本地 Firecrawl v2 服务抓取站点内容，并利用本地 Ollama 模型对标题、描述、正文进行英文翻译、分类/标签与关键词提取，最终生成带有前言（Front Matter）的 Markdown 文件并支持断点续传。

## 前置条件

- 已安装 Python 3.9+（含 `requests` 等常用依赖）
- 本地运行 Firecrawl v2 服务，默认地址 `http://localhost:3002`
- 本地运行 Ollama 服务，默认地址 `http://localhost:11434`，并已准备好模型（默认 `qwen:3b`）

支持通过 `.env` 或系统环境变量覆盖默认地址与输出目录：
- `FIRECRAWL_BASE_URL` 覆盖 Firecrawl 基址
- `OLLAMA_BASE_URL` 覆盖 Ollama 基址
- `OLLAMA_MODEL` 覆盖模型名称
- `OUTPUT_DIR` 覆盖输出目录（默认 `results`）
- `FIRECRAWL_TOKEN` 设置 Firecrawl 访问令牌（自动拼接为 `Authorization: Bearer <token>`）
- `FIRECRAWL_AUTH` 兼容旧授权变量（若不以 `Bearer ` 开头会自动拼接）
- `FIRECRAWL_EXTRA_SCRAPE_OPTIONS` 以 JSON 形式提供额外抓取选项，合并到请求的 `scrape_options` 中
- `FIRECRAWL_MAX_DISCOVERY_DEPTH` 设置发现深度（顶层参数 `maxDiscoveryDepth`）
- `FIRECRAWL_LIMIT` 设置每批抓取上限（顶层参数 `limit`）

示例 `.env`（位于仓库根目录）：

```env
FIRECRAWL_BASE_URL=http://localhost:3002
FIRECRAWL_TOKEN=
FIRECRAWL_AUTH=
FIRECRAWL_EXTRA_SCRAPE_OPTIONS={"formats":["markdown","markdownDelta","html"]}
FIRECRAWL_MAX_DISCOVERY_DEPTH=1
FIRECRAWL_LIMIT=100
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=qwen:3b
OUTPUT_DIR=results
```

## 本地默认执行命令

最简启动（使用默认本地服务地址与默认输出目录）：

```bash
python crawl_with_firecrawl.py --start-url "https://example.com"
```

如需显式指定所有关键参数（示例）：

```bash
python crawl_with_firecrawl.py \
  --start-url "https://example.com" \
  --firecrawl-base "http://localhost:3002" \
  --ollama-base "http://localhost:11434" \
  --ollama-model "qwen:3b" \
  --output-dir "results" \
  --max-pages 0 \
  --delay 0.2 \
  --firecrawl-token "<token>"
```

## 命令参数说明

- `--start-url`（必填）
  - 抓取的根 URL；脚本会从该地址开始并按 Firecrawl v2 的批次返回继续抓取。

- `--firecrawl-base`（可选）
  - Firecrawl v2 服务基址；默认使用环境变量 `FIRECRAWL_BASE_URL`，若未设置则使用 `http://localhost:3002`。

- `--max-pages`（可选，整数）
  - 最多处理的页面数量；默认 `0` 表示不限制。

- `--delay`（可选，浮点数，单位秒）
  - 每次请求批次的间隔；默认 `0.2`。

- `--ollama-base`（可选）
  - 本地 Ollama 服务基址；默认使用环境变量 `OLLAMA_BASE_URL`，若未设置则使用 `http://localhost:11434`。

- `--ollama-model`（可选）
  - 本地模型名称；默认使用环境变量 `OLLAMA_MODEL`，若未设置则为 `qwen:3b`。

- `--output-dir`（可选）
  - 输出目录；默认使用环境变量 `OUTPUT_DIR`，若未设置则为 `results`。

- `--env-file`（可选）
  - 启动前加载的 `.env` 文件路径；也可通过环境变量 `ENV_FILE` 指定。
  - 优先级：`--env-file` > `ENV_FILE` > 默认根目录 `.env`。

## 输出内容与结构

- 每个页面会生成对应的 Markdown 文件，文件名前缀来自英文标题的规范化（保持小写、去除标点、空格转 `-`、确保唯一）。
- 前言（Front Matter）包含以下字段（部分与抓取顺序相关）：
  - `publishDate`：发布时间；前 50 篇使用当前日期时间，之后每 3 篇递增 1 天。
  - `lastmod`：最后修改时间；与 `publishDate` 一致。
  - `draft`：固定为 `true`。
  - `title`：英文标题。
  - `description`：英文描述（由源页面元数据翻译得到）。
  - `summary`：与 `description` 同生成方式，使用翻译后的描述。
  - `url`：规范化生成的相对 URL（文件名）。
  - `categories`：来自本地提取与归一化的分类列表（上限 70）。
  - `tags`：来自本地提取与归一化的标签列表（上限 300）。
  - `keywords`：英文 SEO 关键词（最多 70 个，YAML 内联列表）。
  - `type`：固定为 `docs`。
  - `prev`：前一页的 URL（用于形成文档链）。
  - `sidebar.open`：固定为 `true`。

## 断点续传（manifest.json）

- 脚本会在 `output_dir` 写入 `manifest.json`，每处理一篇都会更新：
  - `start_url`、`firecrawl_base`、`ollama_base`、`ollama_model`
  - `pages_processed`：已处理的页面数
  - `files`：已写入文件的相对路径列表（POSIX 风格）
  - `used_urls`：已使用的 URL slug 集合（确保生成文件名唯一）
  - `last_prev_url`：前言里的 `prev` 字段上一个值
  - `global_categories_pool`、`global_tags_pool`：全局分类与标签池
  - `latest_next_url`：下一批抓取入口 URL
- 重启时，脚本会尝试读取 `manifest.json` 并从 `latest_next_url` 继续抓取，同时恢复相关状态（计数、已写文件、池与链路等）。
- 授权信息不会写入 `manifest.json`，重启时请继续通过参数或环境变量提供 token。

## Firecrawl 抓取选项（scrape_options）

- 默认请求体包含以下 `scrape_options`：
  - `formats`: `["markdown", "markdownDelta", "html"]`
- 顶层请求参数现在直接包含发现与分页控制：
  - `maxDiscoveryDepth`: 默认 `1`
  - `limit`: 默认 `100`
  这两个参数不再出现在 `scrape_options.crawlOptions` 中。
- 可通过环境变量提供配置：
  - `FIRECRAWL_EXTRA_SCRAPE_OPTIONS`：仅用于合并 `scrape_options` 的内容（如 `formats`、`includeSelectors`、`removeSelectors` 等）；其中不得包含 `crawlOptions`。
    - 示例：
      ```bash
      $env:FIRECRAWL_EXTRA_SCRAPE_OPTIONS='{"includeSelectors": ["article"], "removeSelectors": ["nav", "footer"]}'
      ```
    - 若包含 `crawlOptions` 将被忽略。
  - `FIRECRAWL_MAX_DISCOVERY_DEPTH` 与 `FIRECRAWL_LIMIT`：分别控制顶层的 `maxDiscoveryDepth` 与 `limit`，优先级高于默认值。

## 注意事项

- 请确保 Firecrawl v2 与 Ollama 均在本地正常运行，且模型已准备好。
- 若你希望使用不同的模型或变更参数（如温度、上下文长度），可通过脚本内的调用体进行调整。
- 输出目录默认 `results`，可通过 `--output-dir` 或环境变量修改。

## 示例

抓取某站点并输出至默认目录：

```bash
python crawl_with_firecrawl.py --start-url "https://docs.example.com"
```

中断后继续抓取（自动读取 `manifest.json`）：

```bash
python crawl_with_firecrawl.py --start-url "https://docs.example.com"
```
- `--firecrawl-token`（推荐）
  - Firecrawl 访问令牌，直接输入 token，程序会自动拼接 `Authorization: Bearer <token>`。
  - 也可通过环境变量 `FIRECRAWL_TOKEN` 设置。

- `--firecrawl-auth`（兼容）
  - 兼容旧参数：若输入不以 `Bearer ` 开头的值，程序会自动拼接；若以 `Bearer ` 开头，将按原样使用。
  - 也可通过环境变量 `FIRECRAWL_AUTH` 设置。
