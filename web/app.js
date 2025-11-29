const 文件选择 = document.getElementById('文件选择')
const 时间框 = document.getElementById('时间框')
const 加载按钮 = document.getElementById('加载按钮')
const m_sharpe = document.getElementById('m_sharpe')
const m_dd = document.getElementById('m_dd')
const m_win = document.getElementById('m_win')
const m_turnover = document.getElementById('m_turnover')
const m_total = document.getElementById('m_total')
const m_pairs = document.getElementById('m_pairs')
const plotImg = document.getElementById('plotImg')
const debugInfo = document.getElementById('debugInfo')
let priceChart, candleSeries, navChart, navSeriesMap
let 显示调试 = false
let 原始K线 = []
let 原始标记 = []
let 当前区间秒 = null
let 显示标记 = true
let ma5Series, ma10Series, ma20Series, volumeSeries
let 原始MA5 = [], 原始MA10 = [], 原始MA20 = [], 原始VOL = []
let ma5Map = new Map(), ma10Map = new Map(), ma20Map = new Map()
const 详情 = document.getElementById('tradeDetail')

function 调试信息(msg) {
  console.log(msg)
  debugInfo.textContent += new Date().toLocaleTimeString() + ': ' + msg + '\n'
  if(显示调试){
    debugInfo.style.display = 'block'
  } else {
    debugInfo.style.display = 'none'
  }
}

// 重新加载图表函数
window.重新加载图表 = function() {
  调试信息('图表库重新加载，重新渲染图表');
  const names = Array.from(文件选择.selectedOptions).map(x=>x.value);
  if(names.length > 0) {
    渲染价格(names[0], 时间框.value);
  }
}

async function 加载列表(){
  const r = await fetch('/api/list')
  const j = await r.json()
  文件选择.innerHTML = ''
  j.files.forEach(x=>{
    const opt = document.createElement('option')
    opt.value = x.path
    opt.textContent = x.name
    文件选择.appendChild(opt)
  })
}

async function 加载结果(){
  const names = Array.from(文件选择.selectedOptions).map(x=>x.value)
  if(names.length===0) return
  const r = await fetch('/api/result?name='+encodeURIComponent(names[0]))
  const j = await r.json()
  m_sharpe.textContent = (j.sharpe??'-').toFixed ? j.sharpe.toFixed(3) : j.sharpe
  m_dd.textContent = (j.max_drawdown??'-').toFixed ? j.max_drawdown.toFixed(4) : j.max_drawdown
  m_win.textContent = (j.win_rate??'-').toFixed ? (j.win_rate*100).toFixed(2)+'%' : j.win_rate
  m_turnover.textContent = (j.turnover??'-').toFixed ? j.turnover.toFixed(3) : j.turnover
  m_total.textContent = (j.total_pnl??'-').toFixed ? j.total_pnl.toFixed(3) : j.total_pnl
  m_pairs.textContent = (j.pairing_count??'-').toFixed ? j.pairing_count.toFixed(0) : j.pairing_count
  const plot = j.plot
  plotImg.src = plot ? '/'+plot.replace(/^\//,'') : ''
  await 渲染价格(names[0], 时间框.value)
  await 渲染交易标记(names[0])
  await 渲染净值对比(names)
}

async function 渲染价格(name, tf){
  调试信息(`开始渲染价格图: ${name}, 时间框架: ${tf}`)
  const container = document.getElementById('price')
  container.innerHTML = ''

  // 检查图表库是否加载
  if(typeof window.LightweightCharts === 'undefined'){
    调试信息('图表库未定义，等待加载...')
    container.innerHTML = '<div style="padding:20px;text-align:center;color:#666;">图表库加载中，请稍候...</div>'
    setTimeout(() => 渲染价格(name, tf), 1000)
    return
  }

  if(typeof window.LightweightCharts.createChart !== 'function'){
    调试信息('图表库创建函数不可用')
    container.innerHTML = '<div style="padding:20px;text-align:center;color:#e74c3c;">图表库加载失败</div>'
    return
  }

  调试信息('图表库已加载，开始创建图表')
  priceChart = LightweightCharts.createChart(container,{width:container.clientWidth,height:360,layout:{textColor:'#e6e8eb',background:{color:'transparent'}},grid:{vertLines:{color:'rgba(255,255,255,0.08)'},horzLines:{color:'rgba(255,255,255,0.08)'}}})
  let 系列类型 = 'candle'
  if(typeof priceChart.addCandlestickSeries==='function'){
    candleSeries = priceChart.addCandlestickSeries()
    系列类型 = 'candle'
  }else if(typeof priceChart.addLineSeries==='function'){
    candleSeries = priceChart.addLineSeries({color:'#222'})
    系列类型 = 'line'
  }else if(typeof priceChart.addAreaSeries==='function'){
    candleSeries = priceChart.addAreaSeries({lineColor:'#222',topColor:'rgba(34,34,34,0.2)',bottomColor:'rgba(34,34,34,0.0)'})
    系列类型 = 'line'
  }else if(typeof priceChart.addSeries==='function'){
    candleSeries = priceChart.addSeries({type:'line'})
    系列类型 = 'line'
  }else{
    console.error('图表系列创建方法不可用')
    return
  }
  try {
    调试信息(`请求数据: /api/ohlc?name=${encodeURIComponent(name)}&tf=${encodeURIComponent(tf)}`)
    const r = await fetch(`/api/ohlc?name=${encodeURIComponent(name)}&tf=${encodeURIComponent(tf)}`)
    调试信息(`响应状态: ${r.status}`)

    if(!r.ok){
      const txt = await r.text()
      调试信息(`获取OHLC失败: ${r.status} - ${txt}`)
      container.innerHTML = `<div style="padding:20px;text-align:center;color:#e74c3c;">获取数据失败: ${r.status}</div>`
      return
    }

    const j = await r.json()
    调试信息(`获取到数据，条数: ${j.bars ? j.bars.length : 0}`)
    console.log('OHLC数据:', j)

    if(!j.bars || j.bars.length === 0){
      调试信息('没有OHLC数据')
      container.innerHTML = '<div style="padding:20px;text-align:center;color:#e74c3c;">暂无数据</div>'
      return
    }

    调试信息(`开始设置${系列类型}数据`)
    原始K线 = j.bars
    原始VOL = 原始K线.map(b=>({time:b.time,value:b.volume||0,color:(b.close>=b.open)?'#34c759':'#ff3b30'}))
    原始MA5 = 计算SMA(原始K线,5)
    原始MA10 = 计算SMA(原始K线,10)
    原始MA20 = 计算SMA(原始K线,20)
    ma5Map = new Map(原始MA5.map(d=>[d.time,d.value]))
    ma10Map = new Map(原始MA10.map(d=>[d.time,d.value]))
    ma20Map = new Map(原始MA20.map(d=>[d.time,d.value]))
    if(当前区间秒){
      应用区间(当前区间秒)
    }else{
      if(系列类型==='candle'){
        candleSeries.setData(原始K线)
      }else{
        const line = 原始K线.map(b=>({time:b.time,value:b.close}))
        candleSeries.setData(line)
      }
      if(typeof priceChart.addLineSeries==='function'){
        ma5Series = priceChart.addLineSeries({color:'#ff2d55',lineWidth:2})
        ma10Series = priceChart.addLineSeries({color:'#ff9f0a',lineWidth:2})
        ma20Series = priceChart.addLineSeries({color:'#0a84ff',lineWidth:2})
        ma5Series.setData(原始MA5)
        ma10Series.setData(原始MA10)
        ma20Series.setData(原始MA20)
      }
      if(typeof priceChart.addHistogramSeries==='function'){
        volumeSeries = priceChart.addHistogramSeries({color:'#34c759',base:0,priceFormat:{type:'volume'},scaleMargins:{top:0.8,bottom:0}})
        volumeSeries.setData(原始VOL)
      }
    }

    调试信息('数据设置完成，调整视图')
    priceChart.timeScale().fitContent()
    调试信息('价格图渲染完成')
  } catch(error){
    调试信息(`渲染价格图错误: ${error.message}`)
    console.error('渲染价格图错误:', error)
    container.innerHTML = `<div style="padding:20px;text-align:center;color:#e74c3c;">渲染错误: ${error.message}</div>`
  }
}

async function 渲染交易标记(name){
  try {
    const r = await fetch('/api/trades?name='+encodeURIComponent(name))
    if(!r.ok){
      const txt = await r.text().catch(()=> '')
      调试信息(`获取交易标记失败: ${r.status} ${txt}`)
      return
    }
    const j = await r.json().catch(()=> ({}))
    const 标记 = Array.isArray(j.markers) ? j.markers : []
    const 列表 = Array.isArray(j.items) ? j.items : []
    原始标记 = 标记
    if(candleSeries && typeof candleSeries.setMarkers === 'function'){
      if(显示标记){
        if(当前区间秒){
          应用标记过滤()
        }else{
          candleSeries.setMarkers(标记)
        }
      }else{
        candleSeries.setMarkers([])
      }
    }
    const topN = 列表.slice(-20).reverse()
    const 行 = topN.map(x=>{
      const t = typeof x.ts==='string'? Number(x.ts) : Number(x.ts)
      const ts = isFinite(t)? new Date(t).toLocaleString() : String(x.ts)
      const side = x.side==='BUY'?'买入':'卖出'
      const price = x.price
      const qty = x.qty
      const pp = x.pair_profit
      return `<tr><td>${ts}</td><td>${side}</td><td>${price}</td><td>${qty}</td><td>${pp}</td></tr>`
    }).join('')
    详情.innerHTML = `<table style="width:100%;border-collapse:collapse"><thead><tr style="text-align:left"><th>时间</th><th>方向</th><th>价格</th><th>数量</th><th>配对盈亏</th></tr></thead><tbody>${行}</tbody></table>`
    详情.style.display = 'block'
    priceChart.subscribeCrosshairMove(param=>{
      if(!param || !param.time || !param.seriesData) return
      const p = param.seriesData.get(candleSeries)
      if(!p) return
      const ts = new Date((param.time)*1000).toLocaleString()
      const 文本 = p.open!==undefined? `时间:${ts} 开:${p.open} 高:${p.high} 低:${p.low} 收:${p.close}` : `时间:${ts} 值:${p.value}`
      const 附 = 列表.filter(x=>{
        const t = typeof x.ts==='string'? Number(x.ts) : Number(x.ts)
        return Math.abs(t/1000 - param.time) <= 60
      }).slice(-5).map(x=>{
        const side = x.side==='BUY'?'买入':'卖出'
        return `${side} ${x.qty}@${x.price} 配对盈亏:${x.pair_profit}`
      }).join(' | ')
      const 提示 = 附? `${文本} | 交易:${附}` : 文本
      详情.innerHTML = `<div>${提示}</div>`
    })
  } catch(error){
    调试信息(`渲染交易标记错误: ${error.message}`)
  }
}

function 应用区间(sec){
  当前区间秒 = sec
  if(!原始K线 || 原始K线.length===0) return
  const last = 原始K线[原始K线.length-1].time
  const start = last - sec
  const 子集 = 原始K线.filter(b=>b.time>=start)
  const line = 子集.map(b=>({time:b.time,value:b.close}))
  const ma5子 = 原始MA5.filter(b=>b.time>=start)
  const ma10子 = 原始MA10.filter(b=>b.time>=start)
  const ma20子 = 原始MA20.filter(b=>b.time>=start)
  const vol子 = 原始VOL.filter(b=>b.time>=start)
  if(candleSeries && typeof candleSeries.setData==='function'){
    if(candleSeries.setMarkers && 原始标记.length){
      应用标记过滤()
    }
    if(line.length>0 && candleSeries.setData){
      if(typeof candleSeries.setData === 'function' && candleSeries.seriesType && candleSeries.seriesType()==='Line'){
        candleSeries.setData(line)
      }else{
        candleSeries.setData(子集)
      }
    }
    if(ma5Series) ma5Series.setData(ma5子)
    if(ma10Series) ma10Series.setData(ma10子)
    if(ma20Series) ma20Series.setData(ma20子)
    if(volumeSeries) volumeSeries.setData(vol子)
  }
  if(priceChart && priceChart.timeScale){
    priceChart.timeScale().fitContent()
  }
}

function 应用标记过滤(){
  if(!原始标记 || 原始标记.length===0) return
  const last = 原始K线.length? 原始K线[原始K线.length-1].time : null
  if(!last || !当前区间秒) { candleSeries.setMarkers(原始标记); return }
  const start = last - 当前区间秒
  let 筛 = 原始标记.filter(m=> m.time>=start)
  if(筛.length>500) 筛 = 筛.slice(-500)
  candleSeries.setMarkers(显示标记? 筛 : [])
}

async function 渲染净值对比(names){
  const container = document.getElementById('nav')
  container.innerHTML = ''
  try {
    if(typeof window.LightweightCharts === 'undefined'){
      调试信息('图表库未定义，等待加载...')
      container.innerHTML = '<div style="padding:20px;text-align:center;color:#666;">图表库加载中，请稍候...</div>'
      setTimeout(() => 渲染净值对比(names), 1000)
      return
    }
    navChart = LightweightCharts.createChart(container,{width:container.clientWidth,height:360,layout:{textColor:'#222',background:{color:'#fff'}}})
    navSeriesMap = {}
    const r = await fetch('/api/nav?names='+encodeURIComponent(names.join(',')))
    if(!r.ok){
      const txt = await r.text().catch(()=> '')
      调试信息(`获取净值数据失败: ${r.status} ${txt}`)
      container.innerHTML = `<div style="padding:20px;text-align:center;color:#e74c3c;">获取净值数据失败: ${r.status}</div>`
      return
    }
    const j = await r.json().catch(()=> ({navs: []}))
    if(!j.navs || j.navs.length === 0){
      调试信息('没有净值数据')
      container.innerHTML = '<div style="padding:20px;text-align:center;color:#e74c3c;">暂无净值数据</div>'
      return
    }
    let colors = ['#222','#0a84ff','#ff3b30','#34c759','#af52de','#ff9f0a']
    j.navs.forEach((item,idx)=>{
      let s
      if(typeof navChart.addLineSeries==='function'){
        s = navChart.addLineSeries({color:colors[idx%colors.length]})
      }else if(typeof navChart.addAreaSeries==='function'){
        s = navChart.addAreaSeries({lineColor:colors[idx%colors.length],topColor:'rgba(10,132,255,0.2)',bottomColor:'rgba(10,132,255,0.0)'})
      }else if(typeof navChart.addSeries==='function'){
        s = navChart.addSeries({type:'line'})
      }else{
        return
      }
      s.setData(item.series)
      navSeriesMap[item.name]=s
    })
    navChart.timeScale().fitContent()
  } catch(error){
    调试信息(`渲染净值图错误: ${error.message}`)
    container.innerHTML = `<div style="padding:20px;text-align:center;color:#e74c3c;">渲染错误: ${error.message}</div>`
  }
}

function 渲染收益(profits){
  const ctx = document.getElementById('profitChart')
  if(profitChart) profitChart.destroy()
  const bins = 20
  const min = Math.min(...profits,0)
  const max = Math.max(...profits,0)
  const step = (max-min)/bins || 1
  const edges = Array.from({length:bins+1},(_,i)=>min+i*step)
  const counts = Array.from({length:bins},()=>0)
  profits.forEach(p=>{const idx = Math.max(0,Math.min(bins-1,Math.floor((p-min)/step)));counts[idx]++})
  const labels = counts.map((_,i)=>edges[i].toFixed(2)+'~'+edges[i+1].toFixed(2))
  profitChart = new Chart(ctx,{type:'bar',data:{labels,datasets:[{label:'Profit',data:counts,backgroundColor:'#444'}]},options:{responsive:true,plugins:{legend:{display:false}}}})
}

加载按钮.addEventListener('click',加载结果)
时间框.addEventListener('change',()=>{const names=Array.from(文件选择.selectedOptions).map(x=>x.value);if(names.length>0){渲染价格(names[0], 时间框.value)}})

// 窗口大小调整时重新渲染图表
window.addEventListener('resize', () => {
  if(priceChart) {
    const priceContainer = document.getElementById('price')
    priceChart.applyOptions({width: priceContainer.clientWidth})
  }
  if(navChart) {
    const navContainer = document.getElementById('nav')
    navChart.applyOptions({width: navContainer.clientWidth})
  }
})

// 页面加载完成后延迟执行，确保图表库已加载
window.addEventListener('load', function() {
  setTimeout(() => {
    调试信息('页面加载完成，开始加载数据');
    加载列表().then(()=>{
      if(文件选择.options.length>0){
        文件选择.options[0].selected=true;
        加载结果();
      }
    });
  }, 1000); // 延迟1秒确保图表库加载
});
document.getElementById('区间1小时').addEventListener('click',()=>{应用区间(3600)})
document.getElementById('区间4小时').addEventListener('click',()=>{应用区间(4*3600)})
document.getElementById('区间1天').addEventListener('click',()=>{应用区间(24*3600)})
document.getElementById('区间全部').addEventListener('click',()=>{当前区间秒=null; if(原始K线.length){candleSeries.setData(原始K线); 应用标记过滤()}})
document.getElementById('标记开关').addEventListener('change',(e)=>{显示标记=e.target.checked; 应用标记过滤()})

window.addEventListener('keydown', (e)=>{
  const names=Array.from(文件选择.selectedOptions).map(x=>x.value)
  if(e.key==='1' && names.length){时间框.value='1min'; 渲染价格(names[0], '1min')}
  if(e.key==='5' && names.length){时间框.value='5min'; 渲染价格(names[0], '5min')}
  if(e.key==='0' && names.length){当前区间秒=null; 渲染价格(names[0], 时间框.value)}
  if(e.key==='z'){应用区间(3600)}
  if(e.key==='x'){应用区间(24*3600)}
})

function 计算SMA(bars, n){
  const out=[]
  let sum=0
  for(let i=0;i<bars.length;i++){
    sum += Number(bars[i].close||0)
    if(i>=n) sum -= Number(bars[i-n].close||0)
    if(i>=n-1){ out.push({time: bars[i].time, value: +(sum/n).toFixed(4)}) }
  }
  return out
}