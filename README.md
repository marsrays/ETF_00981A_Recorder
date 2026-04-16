# ETF_00981A_Recorder

追蹤 **00981A ETF** 投資組合的歷史變化，提供 REST API 查詢任意日期區間的資金與成分股差異。

---

## 專案結構

```
etf_tracker/
├── main.py          # FastAPI 應用程式進入點 + 排程器
├── parser.py        # xlsx 解析邏輯（民國日期 → 西元、欄位對應）
├── store.py         # 記憶體資料庫，管理所有快照
├── diff.py          # 兩個快照之間的差異計算
├── downloader.py    # 自動下載最新 xlsx 並去重
├── requirements.txt
└── data/            # 存放 ETF_Investment_Portfolio_YYYYMMDD.xlsx 檔案
```

---

## 安裝

```bash
pip install -r requirements.txt
```

---

## 啟動

```bash
python -m uvicorn main:app --host 0.0.0.0 --port 8081 --reload
```

啟動後自動：
1. 讀取 `data/` 資料夾中所有 `ETF_Investment_Portfolio_*.xlsx`
2. 每天下午 **17:00（台北時間）** 自動下載最新版本

---

## API 端點

### `GET /dates`
列出所有已有資料的日期。

```json
{ "dates": ["2026-04-15", "2026-04-16"] }
```

---

### `GET /snapshot/{date}`
取得特定日期的完整快照（需完全匹配）。

```
GET /snapshot/2026-04-15
```

---

### `GET /diff?start=YYYY-MM-DD&end=YYYY-MM-DD`
比較兩個日期的資料差異。

| 情況 | 行為 |
|------|------|
| 起始日無資料 | 往後找最近有資料的日期 |
| 結束日無資料 | 往前找最近有資料的日期 |

```
GET /diff?start=2026-04-01&end=2026-04-15
```

回應包含：
- `start_date` / `end_date`：實際使用的日期
- `requested_start` / `requested_end`：原始輸入的日期
- `fund_assets`：淨資產、流通單位數、每單位淨值的變化
- `asset_allocation`：各類資產配置的變化
- `stocks.summary`：新增/刪除/變更/不變的股票數量
- `stocks.added`：新增成分股
- `stocks.removed`：移除成分股
- `stocks.changed`：持股數量或權重有變化的股票
- `stocks.unchanged`：無變化的股票

---

### `POST /download`
手動觸發下載最新 xlsx。

```bash
curl -X POST http://localhost:8000/download
```

---

### `GET /health`
健康檢查，顯示已載入的快照數量。

---

## 新增歷史資料

直接將 `ETF_Investment_Portfolio_YYYYMMDD.xlsx` 放入 `data/` 資料夾，
重啟服務後會自動載入。

或使用 `POST /download` 觸發下載（只有當天最新版本）。

---

## 資料來源

```
https://www.ezmoney.com.tw/ETF/Fund/AssetExcelNPOI?fundCode=49YTW
```

檔案內的資料日期為民國日期格式（例：`115/04/15` = 2026-04-15），
程式會自動轉換並以西元格式儲存與查詢。
