# comex-gap-panel

一个轻量面板，用来统计 **过去五年 COMEX 黄金（GC=F）周一开市相对上一交易日收市的价格差与百分比**。

## 当前功能

- 拉取公开日线历史数据（Yahoo Finance chart API）
- 计算过去五年每个周一交易日：
  - 上一交易日收盘价
  - 周一开盘价
  - 跳空差值
  - 跳空百分比
- 前端图表展示跳空百分比历史
- 表格展示全部样本明细
- 提供刷新接口

## 统计口径

当前版本使用 **日线历史** 做计算：

- `gap = Monday Open - Previous Trading Day Close`
- `gap_pct = gap / Previous Trading Day Close * 100`

注意：
这不是 tick 级/分钟级官方结算统计，而是一个公开数据近似看板，适合先看整体规律。

## 启动

```bash
cd comex-gap-panel
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 4391
```

打开：

- `http://localhost:4391`
- `GET /api/data`
- `POST /api/refresh`

## 后续扩展

下一步可以继续接：

- 周一开市后 5 分钟 / 10 分钟波动
- 更稳定的数据源
- 导出 CSV
- 按年份分层统计
