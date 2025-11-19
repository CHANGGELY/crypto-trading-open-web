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
let priceChart, candleSeries, navChart, navSeriesMap
const 详情 = document.getElementById('tradeDetail')

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
  const container = document.getElementById('price')
  container.innerHTML = ''
  if(!(window.LightweightCharts && typeof window.LightweightCharts.createChart==='function')){
    console.error('轻量图表库未加载')
    return
  }
  priceChart = LightweightCharts.createChart(container,{width:container.clientWidth,height:360,layout:{textColor:'#222'},grid:{vertLines:{color:'#eee'},horzLines:{color:'#eee'}}})
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
  const r = await fetch(`/api/ohlc?name=${encodeURIComponent(name)}&tf=${encodeURIComponent(tf)}`)
  const j = await r.json()
  if(系列类型==='candle'){
    candleSeries.setData(j.bars)
  }else{
    const line = j.bars.map(b=>({time:b.time,value:b.close}))
    candleSeries.setData(line)
  }
}

async function 渲染交易标记(name){
  const r = await fetch('/api/trades?name='+encodeURIComponent(name))
  const j = await r.json()
  candleSeries.setMarkers(j.markers||[])
  priceChart.subscribeCrosshairMove(param=>{
    if(!param || !param.time || !param.seriesData) return
    const p = param.seriesData.get(candleSeries)
    if(!p) return
    详情.style.display='block'
    if(p.open!==undefined){
      详情.textContent = `时间:${new Date((param.time)*1000).toLocaleString()} 开:${p.open} 高:${p.high} 低:${p.low} 收:${p.close}`
    }else{
      详情.textContent = `时间:${new Date((param.time)*1000).toLocaleString()} 值:${p.value}`
    }
  })
}

async function 渲染净值对比(names){
  const container = document.getElementById('nav')
  container.innerHTML = ''
  navChart = LightweightCharts.createChart(container,{width:container.clientWidth,height:360,layout:{textColor:'#222'}})
  navSeriesMap = {}
  const r = await fetch('/api/nav?names='+encodeURIComponent(names.join(',')))
  const j = await r.json()
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
加载列表().then(()=>{if(文件选择.options.length>0){文件选择.options[0].selected=true;加载结果()}})