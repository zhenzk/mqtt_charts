/* ═══════════════════════════════════════════════════════
   MQTT Charts — Commercial Edition
   bezier curves · gradient fills · auto-demo · BI dashboard
   ═══════════════════════════════════════════════════════ */
const PALETTE = ['#3b82f6','#10b981','#f59e0b','#ef4444','#a855f7','#06b6d4','#fb923c','#ec4899','#84cc16','#e879f9'];
const STORE_KEY = 'mqtt-charts-v4';
const DEMO_BROKER = 'mqtt://broker.hivemq.com:1883';
const DEMO_TOPIC = 'mqtt-charts/demo';

/* ─── Time ranges (ms) ─── */
const TIME_RANGES = {
  '30s': 30000, '1m': 60000, '2m': 120000, '5m': 300000,
  '15m': 900000, '30m': 1800000, '1h': 3600000, '2h': 7200000, 'all': 0,
};
const DEFAULT_TIME_RANGE = '5m';

/* ─── i18n ─── */
const I18N = {
  zh:{appTitle:'MQTT 实时图表',connection:'连接',brokerUrl:'Broker 地址',username:'用户',password:'密码',clientId:'客户端 ID',connect:'连接',disconnect:'断开',testConnection:'⚡ 测试连接 & 演示',charts:'图表',spacing:'间距',clearData:'清除数据',startDemo:'▶ 启动演示数据',addChart:'添加图表',editChart:'编辑图表',chartTitle:'图表标题',mqttTopic:'MQTT 主题',chartMode:'模式',autoMode:'自动多线',manualMode:'自定义表达式',expression:'值表达式',expressionHint:'用 data 访问 JSON',fieldsFilter:'字段过滤',fieldsFilterHint:'逗号分隔。留空 = 显示所有数字字段。',maxPoints:'最大点数',timeRange:'时间范围',allTime:'全部',lineColor:'颜色',yAxisLabel:'Y 轴标签',cancel:'取消',save:'保存',export:'导出 CSV',noChartsTitle:'暂无图表',noChartsDesc:'连接 MQTT 后创建图表。',orTest:'或点击 <strong>测试连接 & 演示</strong> 立即体验。',disconnected:'未连接',connecting:'连接中',connected:'已连接',connectionFailed:'连接失败',offline:'离线重连中',fields:'字段',points:'点',msgRate:'消息/秒',current:'当前',min:'最小',max:'最大',avg:'平均',count:'数量',auto:'自动',manual:'手动',paused:'已暂停',lastUpdate:'最后更新',demoRunning:'演示运行中',jsonPreview:'JSON 预览',jsonWaiting:'等待消息...'},
  en:{appTitle:'MQTT Charts',connection:'Connection',brokerUrl:'Broker URL',username:'User',password:'Pass',clientId:'Client ID',connect:'Connect',disconnect:'Disconnect',testConnection:'⚡ Test Connection & Demo',charts:'Charts',spacing:'Spacing',clearData:'Clear Data',startDemo:'▶ Start Demo Data',addChart:'Add Chart',editChart:'Edit Chart',chartTitle:'Chart Title',mqttTopic:'MQTT Topic',chartMode:'Mode',autoMode:'Auto Multi-Line',manualMode:'Custom Expression',expression:'Value Expression',expressionHint:'Use data for the JSON',fieldsFilter:'Fields Filter',fieldsFilterHint:'Comma-separated. Empty = show all numeric fields.',maxPoints:'Max Points',timeRange:'Time Range',allTime:'All',lineColor:'Color',yAxisLabel:'Y-axis Label',cancel:'Cancel',save:'Save',export:'Export CSV',noChartsTitle:'No Charts Yet',noChartsDesc:'Connect to MQTT, then create a chart.',orTest:'Or click <strong>Test Connection & Demo</strong> to see it live.',disconnected:'Disconnected',connecting:'Connecting',connected:'Connected',connectionFailed:'Connection failed',offline:'Offline, reconnecting',fields:'Fields',points:'Points',current:'Current',min:'Min',max:'Max',avg:'Avg',count:'Count',auto:'Auto',manual:'Manual',paused:'Paused',lastUpdate:'Last update',demoRunning:'Demo running',jsonPreview:'JSON Preview',jsonWaiting:'Waiting for messages...'}
};
let lang='en';
const t=k=>(I18N[lang]||{})[k]||k;
function applyLang(){document.querySelectorAll('[data-i18n]').forEach(e=>{const v=t(e.dataset.i18n);if(v.includes('<'))e.innerHTML=v;else e.textContent=v;});$('lang-toggle').textContent=lang==='zh'?'EN':'中';renderList();}

/* ─── State ─── */
const S={connected:false,connecting:false,charts:[],data:{},inst:{},maxId:null,maxInst:null,nextId:1,editId:null,demoOn:false,lastJson:null,jsonCount:0,selectedId:null,lastPayloadByTopic:{},gridGap:12,panelH:{connection:null,charts:null,json:null},msgRate:0,msgRateTimer:0,totalPoints:0};
let dragSrc=null;

/* ─── Toast system ─── */
function toast(msg,type='info',ms=3000){
  let c=document.querySelector('.toast-container');if(!c){c=document.createElement('div');c.className='toast-container';document.body.appendChild(c);}
  const el=document.createElement('div');el.className=`toast ${type}`;
  el.innerHTML=`<span class="toast-icon"></span><span>${esc(msg)}</span>`;
  c.appendChild(el);
  setTimeout(()=>{el.classList.add('out');setTimeout(()=>el.remove(),250);},ms);
}

/* ─── Object pool for data points (reduce GC pressure) ─── */
const ptPool=[];
function pt(x,y){const p=ptPool.pop()||{};p.x=x;p.y=y;return p;}
function recyclePt(p){p.x=null;p.y=null;if(ptPool.length<5000)ptPool.push(p);}

/* ─── Batched message queue ─── */
const msgQueue=[];
let msgProcessing=false;
function enqueueMsg(ch,payload){
  msgQueue.push({ch,payload});
  if(!msgProcessing){msgProcessing=true;requestAnimationFrame(flushMsgs);}
}
function flushMsgs(){
  const batch=msgQueue.splice(0,Math.min(msgQueue.length,200));
  msgProcessing=false;
  // Skip heavy chart updates if window hidden (save CPU)
  const hidden=document.hidden;
  for(const{ch,payload}of batch){
    if(hidden){S.data[ch.id]&&Object.values(S.data[ch.id].fields).forEach(a=>{if(a.length>ch.maxPoints)a.splice(0,a.length-ch.maxPoints);});continue;}
    processMsg(ch,payload);
  }
  if(msgQueue.length)requestAnimationFrame(flushMsgs);
}

/* ─── Throttled update queue ─── */
const uq=new Set();let rafId=null;

/* ─── JSON preview throttle (500ms) ─── */
let jsonPreviewTimer=null;
function queueUp(id){if(document.hidden)return;uq.add(id);if(!rafId){rafId=requestAnimationFrame(()=>{uq.forEach(id=>_upd(id));uq.clear();rafId=null;if(S.maxId!==null)updBI();});}}

/* ─── Storage ─── */
function save(){try{const c=S.charts.map(c=>({id:c.id,title:c.title,topic:c.topic,mode:c.mode,expression:c.expression,maxPoints:c.maxPoints,color:c.color,yLabel:c.yLabel,paused:c.paused,fieldsFilter:c.fieldsFilter||[],timeRange:c.timeRange||DEFAULT_TIME_RANGE,h:c.h}));localStorage.setItem(STORE_KEY,JSON.stringify({charts:c,nextId:S.nextId,broker:$('broker-url').value,lang,gridGap:S.gridGap,panelH:S.panelH}));}catch{}}
function load(){try{const r=localStorage.getItem(STORE_KEY);if(!r)return;const c=JSON.parse(r);if(c.lang)lang=c.lang;if(c.broker)$('broker-url').value=c.broker;if(c.gridGap)S.gridGap=c.gridGap;if(c.panelH)S.panelH={...{connection:null,charts:null,json:null},...c.panelH};if(c.charts)c.charts.forEach(ch=>{S.charts.push({...ch,paused:ch.paused||false,fieldsFilter:ch.fieldsFilter||[],timeRange:ch.timeRange||DEFAULT_TIME_RANGE,h:ch.h});S.data[ch.id]={fields:{}};if(ch.id>=S.nextId)S.nextId=ch.id+1;});if(c.nextId)S.nextId=Math.max(S.nextId,c.nextId);}catch{}}

function applyGridGap(){const c=$('charts-container');if(c)c.style.gap=S.gridGap+'px';}
function setGridGap(v){S.gridGap=Math.max(0,Math.min(40,v));applyGridGap();save();}

/* ─── Init ─── */
document.addEventListener('DOMContentLoaded',()=>{
  load();setupEv();applyLang();renderGrid();setupMQTT();setupPanelResizers();
  $('gap-slider').value=S.gridGap;$('gap-val').textContent=S.gridGap;applyGridGap();
  applyPanelHeights();
  setInterval(pruneAll,5000);
});

/* ─── Sidebar panel resizers (drag to resize) ─── */
function setupPanelResizers(){
  document.querySelectorAll('.panel-resizer').forEach(rz=>{
    rz.addEventListener('mousedown',e=>startPanelResize(e,rz));
  });
}

let panelResize=null;
function startPanelResize(e,rz){
  e.preventDefault();
  const above=$(`panel-${rz.dataset.above}`);
  const below=$(`panel-${rz.dataset.below}`);
  if(!above||!below)return;
  const aboveH=above.offsetHeight;
  const belowH=below.offsetHeight;
  const startY=e.clientY;
  rz.classList.add('dragging');
  document.body.style.cursor='ns-resize';
  document.body.style.userSelect='none';
  panelResize={rz,startY,above,below,aboveH,belowH};

  const onMove=ev=>{
    if(!panelResize)return;
    const dy=ev.clientY-panelResize.startY;
    const newAboveH=Math.max(40,panelResize.aboveH+dy);
    const newBelowH=Math.max(40,panelResize.belowH-dy);
    // Apply flex-basis to control height
    panelResize.above.style.flex=`0 0 ${newAboveH}px`;
    panelResize.below.style.flex=`0 0 ${newBelowH}px`;
  };
  const onUp=()=>{
    if(!panelResize)return;
    panelResize.rz.classList.remove('dragging');
    document.body.style.cursor='';document.body.style.userSelect='';
    // Save heights
    const aKey=panelResize.above.dataset.panel;
    const bKey=panelResize.below.dataset.panel;
    S.panelH[aKey]=panelResize.above.offsetHeight;
    S.panelH[bKey]=panelResize.below.offsetHeight;
    save();
    panelResize=null;
    document.removeEventListener('mousemove',onMove);
    document.removeEventListener('mouseup',onUp);
  };
  document.addEventListener('mousemove',onMove);
  document.addEventListener('mouseup',onUp);
}

function applyPanelHeights(){
  for(const[key,h]of Object.entries(S.panelH)){
    if(h!=null){
      const el=$(`panel-${key}`);
      if(el)el.style.flex=`0 0 ${h}px`;
    }
  }
}

/* ─── Test Connection: one-click connect + demo + JSON preview ─── */
async function testConnection(){
  if(S.connected){
    // Already connected — just toggle demo
    toggleDemo();return;
  }
  const url=$('broker-url').value.trim()||DEMO_BROKER;
  const st=$('status-text');
  st.textContent=t('connecting');st.style.color='var(--warning)';
  $('connect-btn').disabled=true;S.connecting=true;
  $('status-dot').className='status-dot wait';
  $('test-btn').disabled=true;
  const r=await window.mqttAPI.connect({url,username:$('mqtt-user').value.trim()||undefined,password:$('mqtt-pass').value.trim()||undefined,clientId:$('mqtt-cid').value.trim()||undefined});
  $('test-btn').disabled=false;
  if(!r.success){
    S.connecting=false;$('connect-btn').disabled=false;
    $('status-dot').className='status-dot err';
    st.textContent=t('connectionFailed')+(r.error?': '+r.error:'');st.style.color='var(--error)';
    toast(t('connectionFailed'),'error');
    return;
  }
  // Connected! Now create demo chart + start publisher
  if(!S.charts.some(c=>c.topic===DEMO_TOPIC)){
    addChart({title:lang==='zh'?'传感器演示':'Sensor Demo',topic:DEMO_TOPIC,mode:'auto'});
  }
  await window.mqttAPI.demoStart();
  S.demoOn=true;updateDemoBtn();showDemoBadge();
  toast(lang==='zh'?'已连接，演示数据运行中':'Connected, demo running','success');
}

/* ─── MQTT ─── */
function setupMQTT(){
  window.mqttAPI.onMessage(({topic,payload})=>{
    if(typeof payload==='string'){try{payload=JSON.parse(payload)}catch{return}}
    S.lastJson={topic,payload};
    S.jsonCount++;
    if(!jsonPreviewTimer){jsonPreviewTimer=setTimeout(()=>{updateJsonPreview(S.lastJson.topic,S.lastJson.payload);jsonPreviewTimer=null;},500);}
    // Batch processing for performance
    S.charts.forEach(ch=>{if(ch.topic===topic&&!ch.paused)enqueueMsg(ch,payload);});
  });
  window.mqttAPI.onStatus(({status,msg})=>onStatus(status,msg));
}

function updateJsonPreview(topic,payload){
  const el=$('json-preview');
  if(!el)return;
  const json=JSON.stringify(payload,null,2);
  // Syntax-highlight
  const highlighted=json
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"([^"]+)":/g,'<span class="json-key">"$1"</span>:')
    .replace(/: (-?\d+\.?\d*)/g,': <span class="json-num">$1</span>')
    .replace(/: "([^"]*)"/g,': <span class="json-str">"$1"</span>')
    .replace(/: (true|false)/g,': <span class="json-bool">$1</span>')
    .replace(/: null/g,': <span class="json-null">null</span>');
  // Build field chips with + button
  let chips='';
  if(payload&&typeof payload==='object'&&!Array.isArray(payload)){
    const fields=Object.entries(payload).filter(([,v])=>!isNaN(+v)).map(([k])=>k);
    if(fields.length){
      if(S.selectedId!==null){
        // Route to selected chart: show toggle state
        const selCh=S.charts.find(c=>c.id===S.selectedId);
        const selData=S.data[S.selectedId];
        chips=`<div class="json-fields"><span class="json-hint">${lang==='zh'?'→ 添加到已选图表':'→ Add to selected chart'}: </span>${fields.map(f=>{
          const hasData=!!selData?.fields[f];
          const onChart=selCh&&selCh.topic===topic;
          if(!onChart)return`<span class="json-field-chip ft-no-topic" onclick="quickAddField('${esc(topic)}','${esc(f)}')" title="Topic mismatch - creates new chart"><span class="plus">+</span>${esc(f)}</span>`;
          const hidden=hasData&&S.inst[S.selectedId]?S.inst[S.selectedId].data.datasets[Object.keys(selData.fields).indexOf(f)]?.hidden:false;
          const active=hasData&&!hidden;
          return`<span class="json-field-chip ${active?'jfc-active':hasData?'jfc-hidden':''}" onclick="toggleField(${S.selectedId},'${esc(f)}')" title="${active?'Hide':'Show'} on chart">${active?'●':hasData?'⊘':'+'} ${esc(f)}</span>`;
        }).join('')}</div>`;
      }else{
        chips=`<div class="json-fields"><span class="json-hint">${lang==='zh'?'点击 + 创建单字段图表，或先点击图表选中':'Click + for new chart, or click a chart first'}: </span>${fields.map(f=>`<span class="json-field-chip" onclick="quickAddField('${esc(topic)}','${esc(f)}')" title="Add chart for this field"><span class="plus">+</span>${esc(f)}</span>`).join('')}</div>`;
      }
    }
  }
  el.innerHTML=`<div class="json-topic">topic: ${esc(topic)} · #${S.jsonCount}</div>${chips}<pre style="margin:6px 0 0;white-space:pre-wrap">${highlighted}</pre>`;
}

function onStatus(status,msg){
  S.connecting=false;$('connect-btn').disabled=false;
  const dot=$('status-dot'),st=$('status-text');
  switch(status){
    case 'connected':
      S.connected=true;$('disconnect-btn').disabled=false;
      dot.className='status-dot on';st.textContent=t('connected');st.style.color='var(--success)';
      subAll();
      break;
    case 'disconnected':
      S.connected=false;$('disconnect-btn').disabled=true;
      dot.className='status-dot off';st.textContent=t('disconnected');st.style.color='var(--text-3)';
      S.demoOn=false;updateDemoBtn();hideDemoBadge();
      break;
    case 'offline':st.textContent=t('offline');st.style.color='var(--warning)';dot.className='status-dot wait';break;
    case 'error':st.textContent=t('connectionFailed')+(msg?': '+msg:'');st.style.color='var(--error)';dot.className='status-dot err';break;
  }
}

/* ─── Events ─── */
function setupEv(){
  // Window controls
  $('btn-close').onclick=()=>window.winAPI.close();
  $('btn-min').onclick=()=>window.winAPI.minimize();
  $('btn-max').onclick=()=>window.winAPI.toggleMaximize();
  $('lang-toggle').onclick=()=>{lang=lang==='zh'?'en':'zh';applyLang();save();};
  $('connect-btn').onclick=doConnect;
  $('disconnect-btn').onclick=async()=>{await window.mqttAPI.demoStop();S.demoOn=false;updateDemoBtn();hideDemoBadge();window.mqttAPI.disconnect();};
  $('test-btn').onclick=testConnection;
  $('add-chart-btn').onclick=()=>openModal();
  $('modal-save').onclick=saveChart;
  $('modal-cancel').onclick=closeModal;
  $('modal-close').onclick=closeModal;
  $('clear-data-btn').onclick=clearAll;
  $('demo-btn').onclick=toggleDemo;
  $('sidebar-hide').onclick=()=>{document.getElementById('app').classList.add('sb-hidden');};
  $('sidebar-show').onclick=()=>{document.getElementById('app').classList.remove('sb-hidden');};
  $('gap-slider').oninput=e=>{const v=+e.target.value;$('gap-val').textContent=v;setGridGap(v);};
  $('bi-close').onclick=closeBI;
  $('bi-back').onclick=closeBI;
  $('bi-export').onclick=exportCSV;
  $('modal-overlay').onclick=e=>{if(e.target===e.currentTarget)closeModal();};
  $('chart-mode').onchange=e=>{const m=e.target.value==='manual';$('expression-group').style.display=m?'':'none';$('color-group').style.display=m?'':'none';$('fields-filter-group').style.display=m?'none':'';};
  document.addEventListener('keydown',e=>{
    if(e.target.tagName==='INPUT'||e.target.tagName==='SELECT'||e.target.isContentEditable)return;
    if(e.key==='Escape'){if(!$('modal-overlay').classList.contains('hidden'))closeModal();else if(S.maxId!==null)closeBI();}
    if(e.ctrlKey&&e.key==='n'){e.preventDefault();openModal();}
    if(e.ctrlKey&&e.key==='w'){e.preventDefault();if(S.selectedId!==null)removeChart(S.selectedId);}
    if(e.code==='Space'){e.preventDefault();if(S.selectedId!==null)togglePause(S.selectedId);}
  });
  // Status bar updater every second
  setInterval(updateStatusbar,1000);
}

function updateStatusbar(){
  const dot=$('sb-dot'),conn=$('sb-conn');
  if(S.connected){dot.style.background='var(--success)';conn.textContent=lang==='zh'?'已连接':'Connected';}
  else if(S.connecting){dot.style.background='var(--warning)';conn.textContent=lang==='zh'?'连接中':'Connecting';}
  else{dot.style.background='var(--text-4)';conn.textContent=lang==='zh'?'未连接':'Disconnected';}
  $('sb-charts').textContent=S.charts.length;
  // Total points
  let tot=0;for(const id in S.data){for(const f in S.data[id].fields)tot+=S.data[id].fields[f].length;}
  $('sb-points').textContent=tot.toLocaleString();
  // Msg rate
  $('sb-rate').textContent=S.msgRateTimer;
  S.msgRate=S.msgRateTimer;S.msgRateTimer=0;
  // Memory estimate
  const memKB=Math.round((tot*16+ptPool.length*8)/1024);
  $('sb-mem').textContent=memKB<1024?memKB+' KB':(memKB/1024).toFixed(1)+' MB';
}

async function doConnect(){
  const url=$('broker-url').value.trim();if(!url)return;
  const st=$('status-text');st.textContent=t('connecting');st.style.color='var(--warning)';
  $('connect-btn').disabled=true;S.connecting=true;$('status-dot').className='status-dot wait';save();
  const r=await window.mqttAPI.connect({url,username:$('mqtt-user').value.trim()||undefined,password:$('mqtt-pass').value.trim()||undefined,clientId:$('mqtt-cid').value.trim()||undefined});
  if(!r.success){S.connecting=false;$('connect-btn').disabled=false;$('status-dot').className='status-dot err';st.textContent=t('connectionFailed')+(r.error?': '+r.error:'');st.style.color='var(--error)';toast(t('connectionFailed'),'error');}
  else toast(t('connected'),'success');
}
function subAll(){const ts=new Set();S.charts.forEach(c=>{if(c.topic)ts.add(c.topic);});ts.forEach(tp=>window.mqttAPI.subscribe(tp));}

/* ─── Demo Mode (real MQTT publisher in main process) ─── */
async function toggleDemo(){
  if(S.demoOn){
    await window.mqttAPI.demoStop();
    S.demoOn=false;updateDemoBtn();hideDemoBadge();
  }else{
    if(!S.connected){toast(lang==='zh'?'请先连接 MQTT broker':'Connect to MQTT broker first','warn');return;}
    if(!S.charts.some(c=>c.topic===DEMO_TOPIC)){
      addChart({title:lang==='zh'?'传感器演示':'Sensor Demo',topic:DEMO_TOPIC,mode:'auto'});
    }
    const r=await window.mqttAPI.demoStart();
    if(r.success){S.demoOn=true;updateDemoBtn();showDemoBadge();}
  }
}

function updateDemoBtn(){
  const b=$('demo-btn');
  if(S.demoOn){b.textContent=lang==='zh'?'⏹ 停止演示':'⏹ Stop Demo';b.classList.add('btn-primary');b.classList.remove('btn-ghost');}
  else{b.textContent=t('startDemo');b.classList.remove('btn-primary');b.classList.add('btn-ghost');}
}

function showDemoBadge(){
  let b=$('demo-badge');if(!b){b=document.createElement('div');b.id='demo-badge';b.className='demo-badge';b.innerHTML=`<span class="ddot"></span><span>${t('demoRunning')}</span>`;document.body.appendChild(b);}
}
function hideDemoBadge(){const b=$('demo-badge');if(b)b.remove();}

/* ─── Data Processing ─── */
function processMsg(ch,payload){
  if(!S.data[ch.id])S.data[ch.id]={fields:{}};
  S.lastPayloadByTopic[ch.topic]=payload;
  const d=S.data[ch.id],now=new Date();
  if(ch.mode==='manual'){
    const v=evalExpr(payload,ch.expression);
    if(v==null||isNaN(v))return;
    if(!d.fields._v)d.fields._v=[];
    const a=d.fields._v;a.push(pt(now,+v));
    if(a.length>ch.maxPoints){const old=a.shift();if(old)recyclePt(old);}
    S.msgRateTimer++;queueUp(ch.id);return;
  }
  let nf=false;
  if(payload&&typeof payload==='object'&&!Array.isArray(payload)){
    for(const[k,v]of Object.entries(payload)){
      const n=+v;if(isNaN(n))continue;
      if(ch.fieldsFilter.length&&!ch.fieldsFilter.includes(k))continue;
      if(!d.fields[k]){d.fields[k]=[];nf=true;}
      const a=d.fields[k];a.push(pt(now,n));
      if(a.length>ch.maxPoints){const old=a.shift();if(old)recyclePt(old);}
    }
  }
  if(nf){renderGrid();renderList();}
  pruneData(ch.id);
  queueUp(ch.id);
}
function evalExpr(d,e){try{return new Function('data',`return (${e});`)(d)}catch{return null}}

/* ─── Memory management: prune data older than timeRange ─── */
function pruneData(id){
  const ch=S.charts.find(c=>c.id===id),d=S.data[id];
  if(!ch||!d)return;
  const rangeMs=TIME_RANGES[ch.timeRange]||0;
  if(rangeMs===0)return; // 'all' = keep everything (capped by maxPoints)
  const cutoff=Date.now()-rangeMs;
  let pruned=false;
  for(const f in d.fields){
    const a=d.fields[f];
    let i=0;
    while(i<a.length&&a[i].x.getTime()<cutoff)i++;
    if(i>0){a.splice(0,i);pruned=true;}
  }
  return pruned;
}

function pruneAll(){
  S.charts.forEach(ch=>{if(pruneData(ch.id))queueUp(ch.id);});
}

function setTimeRange(id,range){
  const ch=S.charts.find(c=>c.id===id);
  if(!ch)return;
  ch.timeRange=range;
  pruneData(id);
  if(S.inst[id])S.inst[id].update('none');
  queueUp(id);
  save();
}

/* ─── Stats ─── */
function stats(pts){
  if(!pts||!pts.length)return null;
  let mn=Infinity,mx=-Infinity,sm=0;
  for(const p of pts){if(p.y<mn)mn=p.y;if(p.y>mx)mx=p.y;sm+=p.y;}
  const cur=pts[pts.length-1].y,first=pts[0].y,ch=cur-first,pct=first!==0?(ch/Math.abs(first))*100:0;
  return{cur,mn,mx,avg:sm/pts.length,count:pts.length,change:ch,pct,firstTime:pts[0].x,lastTime:pts[pts.length-1].x};
}

/* ─── CRUD ─── */
function addChart(cfg){
  const id=S.nextId++;
  const ff=(cfg.fieldsFilter||'').split(',').map(s=>s.trim()).filter(Boolean);
  const ch={id,paused:false,title:cfg.title||`Chart ${id}`,topic:cfg.topic||'',mode:cfg.mode||'auto',expression:cfg.expression||'data.value',maxPoints:+cfg.maxPoints||10000,color:cfg.color||PALETTE[(S.charts.length)%PALETTE.length],yLabel:cfg.yLabel||'',fieldsFilter:ff,timeRange:cfg.timeRange||DEFAULT_TIME_RANGE,h:null};
  S.charts.push(ch);S.data[id]={fields:{}};
  renderList();renderGrid();
  if(S.connected&&ch.topic)window.mqttAPI.subscribe(ch.topic);
  save();return ch;
}
function recycleData(id){const d=S.data[id];if(!d)return;for(const f in d.fields){const a=d.fields[f];while(a.length){const p=a.pop();if(p)recyclePt(p);}}}
function removeChart(id){
  const card=document.querySelector(`.card[data-cid="${id}"]`);
  if(card){card.classList.add('removing');setTimeout(()=>{
    S.charts=S.charts.filter(c=>c.id!==id);recycleData(id);delete S.data[id];
    if(S.inst[id]){S.inst[id].destroy();delete S.inst[id];}
    if(S.maxId===id)closeBI();if(S.selectedId===id)S.selectedId=null;
    renderList();renderGrid();save();
  },180);}else{
    S.charts=S.charts.filter(c=>c.id!==id);recycleData(id);delete S.data[id];
    if(S.inst[id]){S.inst[id].destroy();delete S.inst[id];}
    if(S.maxId===id)closeBI();if(S.selectedId===id)S.selectedId=null;
    renderList();renderGrid();save();
  }
}
function togglePause(id){const ch=S.charts.find(c=>c.id===id);if(ch){ch.paused=!ch.paused;renderList();save();}}

/* ─── Render Grid ─── */
function renderGrid(){
  const c=$('charts-container'),empty=$('empty-state');
  if(!S.charts.length){c.innerHTML='';c.className='grid';empty.style.display='';return;}
  empty.style.display='none';
  const n=S.charts.length;c.className=`grid has-${Math.min(n,2)}`;
  applyGridGap();
  const ex=new Map();
  c.querySelectorAll('.card').forEach(el=>{const id=+el.dataset.cid;if(S.charts.some(ch=>ch.id===id))ex.set(id,el);});
  const f=document.createDocumentFragment();
  S.charts.forEach(ch=>{let el=ex.get(ch.id);if(el){f.appendChild(el);ex.delete(ch.id);return;}el=createCard(ch);f.appendChild(el);});
  ex.forEach(el=>{const id=+el.dataset.cid;if(S.inst[id]){S.inst[id].destroy();delete S.inst[id];}});
  c.innerHTML='';c.appendChild(f);
  requestAnimationFrame(()=>{
    S.charts.forEach(ch=>{
      const cv=$(`cv-${ch.id}`);
      if(cv&&!S.inst[ch.id])initChart(ch,cv);
      // Apply saved size
      const card=document.querySelector(`.card[data-cid="${ch.id}"]`);
      if(card&&ch.h)card.style.height=ch.h+'px';
    });
    S.charts.forEach(ch=>_upd(ch.id));
    if(S.selectedId!==null){const c2=document.querySelector(`.card[data-cid="${S.selectedId}"]`);if(c2)c2.classList.add('selected');const ft=$(`cf-${S.selectedId}`);if(ft){ft.style.display='';renderFieldToolbar(S.selectedId);}}
    // Observe card resize to save heights
    S.charts.forEach(ch=>{
      const card=document.querySelector(`.card[data-cid="${ch.id}"]`);
      if(card&&!card._ro){
        card._roTimer=null;
        card._ro=new ResizeObserver(()=>{
          ch.h=card.offsetHeight;
          if(card._roTimer)clearTimeout(card._roTimer);
          card._roTimer=setTimeout(save,300);
        });
        card._ro.observe(card);
      }
    });
  });
}

function createCard(ch){
  const card=document.createElement('div');
  card.className='card';card.dataset.cid=ch.id;card.draggable=true;
  card.innerHTML=`<div class="card-head"><span class="card-title" id="ct-${ch.id}" ondblclick="renameChart(${ch.id})" title="Double-click to rename">${esc(ch.title)}</span><span class="card-topic">${esc(ch.topic)}</span><select class="time-sel" onchange="setTimeRange(${ch.id},this.value)" title="Time range">${Object.keys(TIME_RANGES).map(r=>`<option value="${r}"${ch.timeRange===r?' selected':''}>${r}</option>`).join('')}</select><div class="card-acts"><button class="act pause" onclick="togglePause(${ch.id})" title="Pause">${ch.paused?'▶':'⏸'}</button><button class="act max" onclick="openBI(${ch.id})" title="Maximize">⛶</button><button class="act del" onclick="removeChart(${ch.id})" title="Delete">✕</button></div></div><div class="card-body"><canvas id="cv-${ch.id}"></canvas></div><div class="card-legend" id="lg-${ch.id}"></div><div class="card-fields" id="cf-${ch.id}"></div><div class="card-resize"></div>`;
  // Click to select chart (not when clicking buttons/selects/legend)
  card.addEventListener('click',e=>{if(e.target.closest('button')||e.target.closest('select')||e.target.closest('.legend-item')||e.target.closest('.ft-chip')||e.target.isContentEditable)return;selectChart(ch.id);});
  card.addEventListener('dragstart',e=>{if(e.target.closest('.card-resize')||e.target.isContentEditable){e.preventDefault();return;}dragSrc=ch.id;card.classList.add('dragging');e.dataTransfer.effectAllowed='move';const g=card.cloneNode(true);g.className='card drag-ghost';g.style.width=card.offsetWidth+'px';document.body.appendChild(g);e.dataTransfer.setDragImage(g,0,0);setTimeout(()=>document.body.removeChild(g),0);});
  card.addEventListener('dragend',()=>{card.classList.remove('dragging');document.querySelectorAll('.card').forEach(c=>c.classList.remove('drag-over'));dragSrc=null;});
  card.addEventListener('dragover',e=>{e.preventDefault();if(dragSrc!==ch.id)card.classList.add('drag-over');});
  card.addEventListener('dragleave',()=>card.classList.remove('drag-over'));
  card.addEventListener('drop',()=>{card.classList.remove('drag-over');if(dragSrc&&dragSrc!==ch.id)reorder(dragSrc,ch.id);});
  return card;
}

function renameChart(id){
  const el=$(`ct-${id}`);if(!el)return;
  el.contentEditable=true;el.focus();
  const range=document.createRange();range.selectNodeContents(el);const sel=window.getSelection();sel.removeAllRanges();sel.addRange(range);
  el.addEventListener('blur',()=>{el.contentEditable=false;const ch=S.charts.find(c=>c.id===id);if(ch){ch.title=el.textContent.trim()||`Chart ${id}`;el.textContent=ch.title;renderList();save();}},{once:true});
  el.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();el.blur();}if(e.key==='Escape'){el.textContent=S.charts.find(c=>c.id===id)?.title||'';el.blur();}});
}
function reorder(f,t){const fi=S.charts.findIndex(c=>c.id===f),ti=S.charts.findIndex(c=>c.id===t);if(fi===-1||ti===-1)return;const[m]=S.charts.splice(fi,1);S.charts.splice(ti,0,m);renderList();renderGrid();save();}

/* ─── Chart.js with gradient fills & bezier ─── */
function makeGradient(ctx,color){
  const g=ctx.createLinearGradient(0,0,0,300);
  g.addColorStop(0,color+'40');g.addColorStop(1,color+'00');return g;
}

const chartOpts=(ch,large)=>({
  responsive:true,maintainAspectRatio:false,animation:{duration:300,easing:'easeOutQuart'},
  interaction:{mode:'nearest',intersect:false},
  plugins:{
    legend:{display:false},
    tooltip:{backgroundColor:'#111827',titleColor:'#94a3b8',bodyColor:'#e5e7eb',borderColor:'#1f2937',borderWidth:1,padding:10,cornerRadius:8,titleFont:{size:10},bodyFont:{size:11},usePointStyle:true,boxPadding:4},
  },
  scales:{
    x:{type:'time',time:{unit:'second',displayFormats:{second:'HH:mm:ss',minute:'HH:mm',hour:'HH:mm'}},grid:{color:'rgba(255,255,255,.03)'},ticks:{color:'#64748b',maxTicksLimit:large?12:8,font:{size:large?11:9}}},
    y:{beginAtZero:false,grid:{color:'rgba(255,255,255,.04)'},ticks:{color:'#64748b',font:{size:large?11:9}},title:{display:!!ch.yLabel,text:ch.yLabel||'',color:'#94a3b8',font:{size:large?11:10}}},
  },
});

function initChart(ch,canvas){
  const ctx=canvas.getContext('2d');
  const data=S.data[ch.id];const fields=data?Object.keys(data.fields):[];
  S.inst[ch.id]=new Chart(ctx,{type:'line',data:{datasets:buildDS(ch,fields,data,ctx)},options:chartOpts(ch,false)});
}

function buildDS(ch,fields,data,ctx){
  const ds=[];
  if(ch.mode==='manual'){
    const c=ch.color;
    ds.push({label:ch.title,data:data?.fields._v||[],borderColor:c,backgroundColor:ctx?makeGradient(ctx,c):c+'15',borderWidth:2.5,fill:true,tension:0.4,pointRadius:0,pointHoverRadius:5,pointHoverBackgroundColor:c,pointHoverBorderColor:'#fff',pointHoverBorderWidth:2});
  }else{
    fields.forEach((f,i)=>{const ci=i%PALETTE.length,c=PALETTE[ci];
      ds.push({label:f,data:data?.fields[f]||[],borderColor:c,backgroundColor:ctx?makeGradient(ctx,c):c+'12',borderWidth:2,fill:false,tension:0.4,pointRadius:0,pointHoverRadius:5,pointHoverBackgroundColor:c,pointHoverBorderColor:'#fff',pointHoverBorderWidth:2});
    });
    if(!ds.length)ds.push({label:ch.title,data:[],borderColor:PALETTE[0],backgroundColor:PALETTE[0]+'12',borderWidth:2,fill:false,tension:0.4,pointRadius:0});
  }
  return ds;
}

function _upd(id){
  const inst=S.inst[id],data=S.data[id],ch=S.charts.find(c=>c.id===id);
  if(!inst||!data||!ch)return;
  const fields=Object.keys(data.fields);
  if(ch.mode==='manual'){inst.data.datasets[0].data=data.fields._v||[];}
  else{
    fields.forEach((f,i)=>{
      if(i<inst.data.datasets.length){inst.data.datasets[i].label=f;inst.data.datasets[i].data=data.fields[f]||[];}
      else{const ci=i%PALETTE.length,c=PALETTE[ci];inst.data.datasets.push({label:f,data:data.fields[f]||[],borderColor:c,backgroundColor:c+'12',borderWidth:2,fill:false,tension:0.4,pointRadius:0,pointHoverRadius:5,pointHoverBackgroundColor:c,pointHoverBorderColor:'#fff',pointHoverBorderWidth:2});}
    });
    while(inst.data.datasets.length>fields.length)inst.data.datasets.pop();
  }
  inst.update('none');updLegend(id);
}

function updLegend(id){
  const el=$(`lg-${id}`);if(!el)return;
  const ch=S.charts.find(c=>c.id===id),data=S.data[id];if(!ch||!data)return;
  const fields=ch.mode==='manual'?['_v']:Object.keys(data.fields);
  const inst=S.inst[id];
  el.innerHTML=fields.map(f=>{
    const a=data.fields[f]||[],last=a.length?a[a.length-1].y:null;
    const nm=f==='_v'?ch.title:f;
    const ci=(ch.mode==='manual'?0:fields.indexOf(f))%PALETTE.length;
    const color=ch.mode==='manual'?ch.color:PALETTE[ci];
    // Check if this dataset is hidden
    const dsIdx=ch.mode==='manual'?0:fields.indexOf(f);
    const hidden=inst&&inst.data.datasets[dsIdx]?inst.data.datasets[dsIdx].hidden:false;
    return `<span class="legend-item${hidden?' hidden-line':''}" onclick="toggleLine(${id},${dsIdx})"><span class="legend-dot" style="background:${color}"></span>${esc(nm)}${last!=null?` <span class="legend-val">${fmt(last)}</span>`:''}</span>`;
  }).join('');
}

function toggleLine(id,dsIdx){
  const inst=S.inst[id];if(!inst||!inst.data.datasets[dsIdx])return;
  inst.data.datasets[dsIdx].hidden=!inst.data.datasets[dsIdx].hidden;
  inst.update('none');
  updLegend(id);
  if(S.selectedId===id)renderFieldToolbar(id);
}

/* ─── Interactive field management ─── */
function selectChart(id){
  S.selectedId=S.selectedId===id?null:id;
  document.querySelectorAll('.card').forEach(c=>{c.classList.toggle('selected',+c.dataset.cid===S.selectedId);});
  // Show/hide field toolbar
  document.querySelectorAll('.card-fields').forEach(el=>{el.style.display='none';});
  if(S.selectedId!==null){
    const ft=$(`cf-${S.selectedId}`);
    if(ft){ft.style.display='';renderFieldToolbar(S.selectedId);}
  }
  // Update JSON chips to reflect selected chart
  if(S.lastJson)updateJsonPreview(S.lastJson.topic,S.lastJson.payload);
}

function getAvailableFields(ch){
  const data=S.data[ch.id];
  const collected=data?Object.keys(data.fields):[];
  const last=S.lastPayloadByTopic[ch.topic];
  const jsonFields=last&&typeof last==='object'&&!Array.isArray(last)?Object.entries(last).filter(([,v])=>!isNaN(+v)).map(([k])=>k):[];
  return [...new Set([...collected,...jsonFields])];
}

function renderFieldToolbar(id){
  const ch=S.charts.find(c=>c.id===id);if(!ch)return;
  const el=$(`cf-${id}`);if(!el)return;
  const available=getAvailableFields(ch);
  if(!available.length){el.innerHTML=`<span class="ft-empty">${lang==='zh'?'暂无字段，等待数据...':'No fields yet, waiting for data...'}</span>`;return;}
  const data=S.data[id],inst=S.inst[id];
  const fields=ch.mode==='manual'?['_v']:Object.keys(data?.fields||{});
  el.innerHTML=available.map(f=>{
    const hasData=!!data?.fields[f];
    const dsIdx=fields.indexOf(f);
    const hidden=inst&&inst.data.datasets[dsIdx]?inst.data.datasets[dsIdx].hidden:false;
    const ci=(ch.mode==='manual'?0:Math.max(0,fields.indexOf(f)))%PALETTE.length;
    const color=ch.mode==='manual'?ch.color:PALETTE[ci];
    let cls='ft-chip';
    if(hasData&&!hidden)cls+=' ft-active';
    else if(hasData&&hidden)cls+=' ft-hidden';
    else cls+=' ft-available';
    const icon=hasData&&!hidden?'●':hasData&&hidden?'⊘':'+';
    const nm=f==='_v'?ch.title:f;
    return `<span class="${cls}" onclick="toggleField(${id},'${esc(f)}')" style="--cc:${color}"><span class="ft-icon">${icon}</span>${esc(nm)}</span>`;
  }).join('');
}

function toggleField(chartId,field){
  const ch=S.charts.find(c=>c.id===chartId),data=S.data[chartId];
  if(!ch||!data)return;
  if(data.fields[field]){
    // Has data → toggle visibility
    const inst=S.inst[chartId];
    const fields=ch.mode==='manual'?['_v']:Object.keys(data.fields);
    const dsIdx=fields.indexOf(field);
    if(inst&&inst.data.datasets[dsIdx]!==undefined){
      inst.data.datasets[dsIdx].hidden=!inst.data.datasets[dsIdx].hidden;
      inst.update('none');
    }
  }else{
    // No data → add to fieldsFilter to start collecting
    if(!ch.fieldsFilter.includes(field))ch.fieldsFilter.push(field);
  }
  updLegend(chartId);
  renderFieldToolbar(chartId);
  save();
}

/* ─── Sidebar List ─── */
function renderList(){
  const el=$('chart-list');
  if(!S.charts.length){el.innerHTML=`<div style="padding:8px;font-size:11px;color:var(--text-4);text-align:center">—</div>`;return;}
  el.innerHTML=S.charts.map(ch=>{
    const d=S.data[ch.id],n=d?Object.values(d.fields).reduce((s,a)=>s+a.length,0):0;
    const color=ch.mode==='manual'?ch.color:PALETTE[0];
    const badge=ch.paused?t('paused'):(ch.mode==='auto'?t('auto'):t('manual'));
    return `<div class="chart-item" draggable="true" data-id="${ch.id}"><span class="cbar" style="background:${color}"></span><div class="info"><div class="nm">${esc(ch.title)}</div><div class="tp">${esc(ch.topic)}</div></div><span class="badge">${badge}</span><span style="font-size:10px;color:var(--text-4)">${n}</span><button class="del" onclick="event.stopPropagation();removeChart(${ch.id})">✕</button></div>`;
  }).join('');
  el.querySelectorAll('.chart-item').forEach(item=>{
    item.addEventListener('dragstart',e=>{dragSrc=+item.dataset.id;item.classList.add('dragging');e.dataTransfer.effectAllowed='move';});
    item.addEventListener('dragend',()=>{item.classList.remove('dragging');el.querySelectorAll('.chart-item').forEach(c=>c.classList.remove('drag-over'));});
    item.addEventListener('dragover',e=>{e.preventDefault();if(dragSrc!==+item.dataset.id)item.classList.add('drag-over');});
    item.addEventListener('dragleave',()=>item.classList.remove('drag-over'));
    item.addEventListener('drop',()=>{item.classList.remove('drag-over');if(dragSrc&&dragSrc!==+item.dataset.id)reorder(dragSrc,+item.dataset.id);});
  });
}

/* ─── BI Dashboard ─── */
function openBI(id){S.maxId=id;const ch=S.charts.find(c=>c.id===id);if(!ch)return;$('bi-title').textContent=ch.title;$('bi-sub').textContent=ch.topic;$('bi-view').classList.remove('hidden');pruneData(id);renderBI();}
function renderBI(){
  if(S.maxId==null)return;
  const ch=S.charts.find(c=>c.id===S.maxId),data=S.data[S.maxId];if(!ch||!data)return;
  const fields=ch.mode==='manual'?['_v']:Object.keys(data.fields);
  renderBIKPI(fields,ch,data);renderBIChart(fields,ch,data);renderBITable(fields,ch,data);
}
function renderBIKPI(fields,ch,data){
  $('bi-kpis').innerHTML=fields.map(f=>{
    const a=data.fields[f]||[],st=stats(a);if(!st)return'';
    const nm=f==='_v'?ch.title:f;const ci=(ch.mode==='manual'?0:fields.indexOf(f))%PALETTE.length;
    const color=ch.mode==='manual'?ch.color:PALETTE[ci];
    const tc=st.pct>0.1?'up':st.pct<-0.1?'down':'flat';
    const ar=st.pct>0.1?'▲':st.pct<-0.1?'▼':'—';
    return `<div class="kpi"><div class="kpi-label"><span class="ld" style="background:${color}"></span>${esc(nm)}</div><div class="kpi-value">${fmt(st.cur)}</div><div class="kpi-sub"><span>${t('min')} ${fmt(st.mn)}</span><span>${t('max')} ${fmt(st.mx)}</span><span>${t('avg')} ${fmt(st.avg)}</span></div><div class="kpi-trend ${tc}">${ar} ${Math.abs(st.pct).toFixed(1)}% · ${st.count} ${t('points')}</div></div>`;
  }).join('');
  let tot=0,lt=null;fields.forEach(f=>{const a=data.fields[f]||[];tot+=a.length;if(a.length)lt=a[a.length-1].x;});
  $('bi-meta').textContent=`${tot} ${t('points')}${lt?' · '+t('lastUpdate')+' '+new Date(lt).toLocaleTimeString():''}`;
}
function renderBIChart(fields,ch,data){
  if(S.maxInst){S.maxInst.destroy();S.maxInst=null;}
  const cv=$('bi-canvas'),ctx=cv.getContext('2d');
  const ds=buildDS(ch,fields,data,ctx).map(d=>({...d,borderWidth:2.8,pointRadius:1,pointHitRadius:14}));
  const o=chartOpts(ch,true);
  o.plugins.legend={display:true,labels:{color:'#94a3b8',font:{size:11},padding:14,usePointStyle:true,pointStyle:'circle'}};
  o.scales.x.grid.color='rgba(255,255,255,.04)';o.scales.y.grid.color='rgba(255,255,255,.05)';
  S.maxInst=new Chart(ctx,{type:'line',data:{datasets:ds},options:o});
}
function renderBITable(fields,ch,data){
  $('bi-tbody').innerHTML=fields.map(f=>{
    const a=data.fields[f]||[],st=stats(a)||{cur:'—',mn:'—',mx:'—',avg:'—',count:0};
    const nm=f==='_v'?ch.title:f;const ci=(ch.mode==='manual'?0:fields.indexOf(f))%PALETTE.length;
    const color=ch.mode==='manual'?ch.color:PALETTE[ci];
    return `<tr><td><span class="ld" style="background:${color}"></span>${esc(nm)}</td><td>${fmt(st.cur)}</td><td>${fmt(st.mn)}</td><td>${fmt(st.mx)}</td><td>${fmt(st.avg)}</td><td>${st.count}</td></tr>`;
  }).join('');
}
function updBI(){
  if(S.maxId==null||!S.maxInst)return;
  const ch=S.charts.find(c=>c.id===S.maxId),data=S.data[S.maxId];if(!ch||!data)return;
  const fields=ch.mode==='manual'?['_v']:Object.keys(data.fields);
  S.maxInst.data.datasets=buildDS(ch,fields,data).map(d=>({...d,borderWidth:2.8,pointRadius:1}));
  S.maxInst.update('none');
  renderBIKPI(fields,ch,data);renderBITable(fields,ch,data);
}
function closeBI(){S.maxId=null;$('bi-view').classList.add('hidden');if(S.maxInst){S.maxInst.destroy();S.maxInst=null;}}

/* ─── Export CSV ─── */
function exportCSV(){
  if(S.maxId==null)return;
  const ch=S.charts.find(c=>c.id===S.maxId),data=S.data[S.maxId];if(!ch||!data)return;
  const fields=ch.mode==='manual'?['_v']:Object.keys(data.fields);if(!fields.length)return;
  const times=new Set();fields.forEach(f=>(data.fields[f]||[]).forEach(p=>times.add(p.x.getTime())));
  const ts=[...times].sort((a,b)=>a-b);
  const rows=[['timestamp',...fields.map(f=>f==='_v'?ch.title:f)]];
  ts.forEach(t=>{const r=[new Date(t).toISOString()];fields.forEach(f=>{const p=(data.fields[f]||[]).find(p=>p.x.getTime()===t);r.push(p?p.y:'');});rows.push(r);});
  const csv=rows.map(r=>r.map(c=>`"${c}"`).join(',')).join('\n');
  const b=new Blob([csv],{type:'text/csv'});const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download=`${ch.title.replace(/[^a-zA-Z0-9_\-]/g,'_')}_${Date.now()}.csv`;a.click();URL.revokeObjectURL(a.href);
}

/* ─── Modal ─── */
function openModal(editId){
  S.editId=editId||null;
  $('modal-title').textContent=editId?t('editChart'):t('addChart');
  const isM=editId?(S.charts.find(c=>c.id===editId)?.mode==='manual'):false;
  if(editId){const c=S.charts.find(c=>c.id===editId);if(c){$('chart-title').value=c.title;$('chart-topic').value=c.topic;$('chart-mode').value=c.mode;$('chart-expression').value=c.expression;$('chart-maxpoints').value=c.maxPoints;$('chart-color').value=c.color;$('chart-ylabel').value=c.yLabel||'';$('chart-fields-filter').value=(c.fieldsFilter||[]).join(', ');$('chart-timerange').value=c.timeRange||DEFAULT_TIME_RANGE;}}
  else{$('chart-title').value='';$('chart-topic').value='';$('chart-mode').value='auto';$('chart-expression').value='data.value';$('chart-maxpoints').value='10000';$('chart-color').value=PALETTE[S.charts.length%PALETTE.length];$('chart-ylabel').value='';$('chart-fields-filter').value='';$('chart-timerange').value=DEFAULT_TIME_RANGE;}
  $('expression-group').style.display=isM?'':'none';$('color-group').style.display=isM?'':'none';$('fields-filter-group').style.display=isM?'none':'';
  $('modal-overlay').classList.remove('hidden');setTimeout(()=>$('chart-title').focus(),80);
}
function closeModal(){$('modal-overlay').classList.add('hidden');S.editId=null;}
function saveChart(){
  const cfg={title:$('chart-title').value.trim()||'Untitled',topic:$('chart-topic').value.trim(),mode:$('chart-mode').value,expression:$('chart-expression').value.trim()||'data.value',maxPoints:+$('chart-maxpoints').value||10000,color:$('chart-color').value,yLabel:$('chart-ylabel').value.trim(),fieldsFilter:$('chart-fields-filter').value.trim(),timeRange:$('chart-timerange').value};
  if(!cfg.topic){toast(t('mqttTopic')+' required','error');return;}
  if(S.editId){const i=S.charts.findIndex(c=>c.id===S.editId);if(i!==-1){Object.assign(S.charts[i],cfg);if(S.inst[S.editId]){S.inst[S.editId].destroy();delete S.inst[S.editId];}renderList();renderGrid();}}
  else addChart(cfg);
  closeModal();save();
}

/* ─── Utils ─── */
function $(id){return document.getElementById(id);}
function esc(s){const d=document.createElement('div');d.textContent=s==null?'':s;return d.innerHTML;}
function fmt(n){if(n==null||n==='—')return'—';if(Number.isInteger(n))return n.toLocaleString();return(+n).toFixed(2);}
function clearAll(){Object.keys(S.data).forEach(id=>{recycleData(+id);S.data[id]={fields:{}};_upd(+id);});if(S.maxId!=null)renderBI();}
function togglePanel(el){el.classList.toggle('collapsed');}
window.removeChart=removeChart;window.togglePause=togglePause;window.openBI=openBI;window.togglePanel=togglePanel;window.toggleLine=toggleLine;window.quickAddField=quickAddField;window.setTimeRange=setTimeRange;window.toggleField=toggleField;window.selectChart=selectChart;window.renameChart=renameChart;

/* Quick-add chart for a single field from JSON preview */
function quickAddField(topic,field){
  if(!S.connected){toast(lang==='zh'?'请先连接 MQTT':'Connect first','warn');return;}
  addChart({title:field,topic,mode:'auto',fieldsFilter:field});
}
