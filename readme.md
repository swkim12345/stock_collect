# stock_collect

A lightweight Python service to **collect**, **process**, and **store** stock price data at both minute-level and daily-level granularity, with built-in scheduling and notifications.

---

## 📦 Project Overview

`stock_collect` periodically fetches live stock price data from public endpoints, transforms it into tidy tables, and writes it into an SQLite database—making analytics and backtests easy and efficient.

---

## 🔑 Key Features

- **Scheduler & Daemonization**  
  - Run collectors as long-running processes using `nohup` or systemd services  
  - Helper scripts for background execution provided  
- **Slack Notifications**  
  - Sends a webhook alert when jobs start, succeed, or fail  
- **Modular & Configurable**  
  - YAML or ENV-based settings for API endpoints, database URL, notification hooks  
  - Separate modules for fetch, transform, and load  

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+  
- SQLite (no extra installation needed)  
- Slack workspace (for optional webhook alerts)

### Installation

1. **Clone the repo**  
   ```bash
   git clone https://github.com/swkim12345/stock_collect.git
   cd stock_collect
   ```

2. **Create & activate a virtual environment**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

### Configuration

Copy the example config and fill in your own values:

```bash
cp .env.example .env
# Then edit .env:
# DB_URL=sqlite:///./stock.db
# SLACK_WEBHOOK=https://hooks.slack.com/services/...
# API_ENDPOINT=https://api.example.com/stock
# FETCH_INTERVAL_MIN=1
```

---

## ⚙️ Usage

* **Manual run**

  ```bash
  python stock_price_multiprocess.py
  ```
* **Background / daemon**

  ```bash
  nohup python stock_price_multiprocess.py &> collector.log &
  ```

---

## 📅 Scheduling with Cron

To run every minute for intraday ticks, and once a day for daily summary:

```cron
# `crontab -e`
0 18 * * *   youruser  cd /path/to/stock_collect && /path/to/.venv/bin/python run_collector.py
```

---

## 🛠 Architecture & Tech Stack

* **Language & Libraries**

  * Python 3.8+, `requests`, `pandas`
  * Please check `requirements.txt`
* **Database**

  * SQLite (single-file database via SQLAlchemy)
* **Scheduler**

  * `nohup`, systemd timers, or cron jobs
* **Notifications**

  * Slack Incoming Webhook
