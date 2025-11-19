## 目标
- 提供一个`backtesting.lightweight_chart`封装，类`PriceChart`一行代码即可渲染价格图与指标，深度适配我们回测输出（净值、配对、交易标记）。
- API与用法符合你给出的示例，尽量保持简洁：`PriceChart(df, default_offset="4h", maximize=True)`；`add_indicator(fn, color, column_name)`；`add_trade_marks([...])`；`add_subchart(df, 'close', color='blue')`。

## 与开源库的关系
- 依赖`lightweight-charts`（TriangleTraders 版本，支持多面板 Subcharts、markers、legend 等）[来源: GitHub - TriangleTraders/lightweight-charts-python]。
- 我们的封装做数据标准化、接口简化与框架适配；底层仍调用其`Chart.set`/`create_line`/`marker_list`/`subchart`等能力。

## 数据与兼容
- 输入`history_data`为DataFrame，列：`candle_begin_time, open, high, low, close, volume`（邢大列名）。
- 封装内做列重命名与时间列转换：映射为库要求的`time | open | high | low | close | volume`，`time`取`candle_begin_time`，自动转`datetime`。
- `default_offset`：可选重采样（如`15T/1H/4H`）。若与原始数据相同频率则无需重采样。
- `maximize`：窗口最大化（使用库窗口API）。

## API设计
- `class PriceChart(history_data: pd.DataFrame, default_offset: str = '15T', maximize: bool = False)`：
  - 校验与标准化列；可选重采样；构造`Chart`，设置样式（浅色干净风格，禁用花哨渐变）。
- `add_indicator(fn: Callable[[pd.DataFrame], pd.Series|pd.DataFrame], color: str = 'red', column_name: str = 'indicator')`：
  - 传入函数对原始`history_data`计算出一列；内部创建`line`并`line.set({'time':..., column_name:...})`。
- `add_trade_marks(trades: List[TradeData])`：
  - 定义`TradeData(symbol: str, direction: Direction, offset: Offset, price: float, qty: float, time: datetime)`；
  - 映射为`marker_list`：多头用绿色向上箭头，空头用红色向下箭头；文本包含`price/qty`与`OPEN/CLOSE`；时间取`time`。
- `add_subchart(sub_df: pd.DataFrame, value_col: str, color: str = 'blue', name: str = 'Sub')`：
  - 要求含`candle_begin_time`用于同步；创建子面板（Subchart），将`{'time':..., name: value}`作为`create_line`或`create_histogram`数据；与主图时间轴联动。
- 其他便捷方法：`set_visible_range(start, end)`、`legend(visible=True)`、`watermark(text)`、`layout(background/text/font)`，统一中文Docstrings。

## 与我们回测框架深度适配
- 价格图主面板：来自你传入的`history_data`（例如从H5 table窗口加载的DF）。
- 指标：一行添加，如`sma/ema/布林带`；实现通用指标辅助函数（可选）。
- 交易标记：在我们回测账户的`更新订单(...)`处（BUY/SELL触网）补充记录`trade_marks`（方向、价、数量、时间），封装后即可`add_trade_marks`渲染。
- 子图表：
  - `净值曲线`：来自`results.json`的`nav_series`，映射为子图`time|nav`折线；
  - `配对收益分布`：聚合分桶后用`histogram`子图；或在子面板用垂直线标注配对时刻。
- 无模拟数据：数据来自真实回测输出与真实历史加载。

## 实现步骤
1. 新建模块`backtesting/lightweight_chart.py`：实现`PriceChart`类、`TradeData`数据结构、`Direction/Offset`枚举，中文Docstrings。
2. 适配函数：
   - `to_lwch_df(df)`：列重命名与时间转换
   - `resample(df, rule)`：可选重采样（OHLC聚合与volume汇总）
3. 指标管道：统一将`Series/DataFrame`转换为库所需格式并创建`Line`。
4. 交易标记：提供批量`marker_list`封装，默认配色与形状，可自定义。
5. 子图表：创建`Subchart`面板并绘制折线/柱状图，时间轴跟随主图。
6. 单元测试：
   - 输入列缺失与类型异常的卫语句；
   - 指标计算正确性（AAA模式）；
   - 标记与子图渲染数据映射测试（不依赖GUI事件）。
7. 使用示例：与你给出的示例一致，确保“一行创建，几行增强”。

## 验证与演示
- 在本地虚拟环境中安装：`pip install lightweight-charts`。
- 用我们已有的3天窗口数据与`results_3d.json`演示：价格主图 + SMA指标 + 交易标记 + 子图净值曲线。
- 说明如何从回测引擎导出`trade_marks`，并用封装一行渲染。

## 后续增强（可选）
- 顶部工具栏：时间框切换、搜索符号（沿用库的`topbar.switcher`等）。
- 交互回放：点击标记查看订单详情（价格、数量、费用）。
- 与现有`server.py`整合：在网页里嵌入同样的图，统一结果浏览与交互体验。

请确认以上封装与适配方案，确认后我将实现`backtesting/lightweight_chart.py`、补齐回测交易标记导出，并给出使用示例与测试。