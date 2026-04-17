# Smart Money Swap Monitor

这个目录现在包含一个可以 24 小时轮询的最小监控器，用来读取 `smart_money_active.csv` 里的地址：

- `evm` 地址固定监控 `ethereum`、`bnb`、`base` 上的 swap
- `sol` 地址固定监控 `solana` 上的 swap

只要命中 swap，就立刻输出告警，并可选推送到 Telegram / Webhook。默认配置已经切到低成本模式，每 `1` 小时轮询一次。

其中 `ethereum`、`base`、`bnb` 三条 EVM 链都额外带一层 decoded event fallback：如果某些协议暂时没有进入 `dex.trades`，脚本还会尝试从对应链的 `logs_decoded / traces_decoded` 中识别 swap-like 事件。

`solana` 也带了一层日志 fallback：如果某些 swap 还没进入 `dex_solana.trades`，脚本会额外从 `solana.transactions` 的交易日志里识别 `swap/route` 这类 swap-like 信号。

脚本也带了批次容错：如果某一批 Dune 查询超时或失败，不会让整轮监控直接中断，而是记录错误后继续处理其他批次。

## 文件说明

- `smart_money_monitor.py`: 主监控脚本
- `smart_money_active.csv`: 监控地址列表
- `.env.example`: 环境变量模板
- `requirements.txt`: Python 依赖

## CSV 格式

```csv
address_type,address,last_active,active_chains,tx_count_7d,tx_count_30d
evm,0x123...,2026-04-15 08:25:11.000 UTC,base|bnb|ethereum,22,192
sol,93Ny...,2026-04-15 07:29:25.000 UTC,solana,22,30
```

- `address`: 必填
- `address_type`: `evm` 会走 EVM 监控，`sol` 会走 Solana 监控
- `active_chains`: 当前只作为参考列，实际监控范围按地址类型固定：
- `evm` -> `ethereum`、`bnb`、`base`
- `sol` -> `solana`
- 如果文件里有 `label` / `name` / `alias` 列，脚本会优先拿来作为告警名称；否则自动用地址和 `last_active` 生成展示名

## 快速启动

1. 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. 配置环境变量

```bash
cp .env.example .env
```

至少要填：

- `DUNE_API_KEY`
- `SMART_MONEY_CSV`
- `POLL_INTERVAL_SECONDS` 默认是 `3600`，也就是每小时一次

默认已经带上两个可直接使用的 Dune 查询 ID：

- EVM: `7325007`
- Solana: `7325094`
- Solana fallback: `7330025`
- Ethereum fallback: `7329980`
- Base fallback: `7329981`
- BNB fallback: `7325474`

3. 填好 `smart_money_active.csv`

你现在这份导出格式可以直接用，不需要再手工改列名。脚本会自动按 `address_type` 分流到 EVM 和 Solana 两套监控。

4. 启动监控

```bash
source .venv/bin/activate
python smart_money_monitor.py
```

按当前默认配置，脚本会每小时查询一次。

## 告警方式

脚本默认会：

- 在终端打印告警
- 把告警追加到 `alerts.log`

可选推送：

- Telegram: 配 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID`
- Slack / Discord / 自定义 Webhook: 配对应的 webhook URL

## 持续运行

最简单的方式：

```bash
nohup .venv/bin/python smart_money_monitor.py >> monitor.out 2>&1 &
```

如果你不想在本地跑，推荐直接用 GitHub Actions。这个目录里已经带了工作流：

- 文件：`.github/workflows/smart-money-monitor.yml`
- 调度：每小时第 `17` 分钟执行一次
- 运行方式：`python smart_money_monitor.py --once`

你只需要把这个目录推到 GitHub 仓库，然后在仓库 `Settings -> Secrets and variables -> Actions` 里配置：

- `DUNE_API_KEY`
- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`，如果你要 Telegram 告警
- 或对应的 Slack / Discord / Webhook

GitHub Actions 方案里，状态文件存在临时目录，所以每次会按“最近 1 小时窗口”查询一次，而不是依赖本地常驻状态。

如果你仍然想在 macOS 本机常驻，这个目录里也已经带了一个 `launchd` 配置：

```bash
chmod +x run_monitor.sh
cp com.smart_money_tracker.monitor.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.smart_money_tracker.monitor.plist
launchctl start com.smart_money_tracker.monitor
```

查看状态：

```bash
launchctl list | grep smart_money
tail -f launchd.stdout.log
tail -f launchd.stderr.log
```

停止：

```bash
launchctl unload ~/Library/LaunchAgents/com.smart_money_tracker.monitor.plist
```

## 实现细节

- 通过 Dune API 调用两条查询：
- EVM -> `7325007`
- Solana -> `7325094`
- Solana fallback -> `7330025`
- Ethereum fallback -> `7329980`
- Base fallback -> `7329981`
- BNB fallback -> `7325474`
- 每次轮询只查上次检查之后的窗口
- 本地状态保存在 `monitor_state.json`
- 用 `seen_tx_hashes` 做去重，避免重复告警
- `--once` 适合放到 GitHub Actions / 云端 cron 上执行

## 当前假设

- EVM swap 来源于 Dune 的 `dex.trades`
- Solana 主查询来源于 Dune 的 `dex_solana.trades`
- Solana fallback 会把 `solana.transactions` 里带 `swap/route` 日志的交易视为 swap-like 信号，所以覆盖更广，但相对主查询更可能出现少量误报
