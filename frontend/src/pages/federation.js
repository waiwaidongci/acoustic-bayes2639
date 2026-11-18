import {domain} from '../constants/domain.js';import {research} from '../stores/research.js';import {session} from '../stores/session.js';import {federationApi} from '../api/research.js';import {mergeStates} from '../constants/enums.js';import {status} from '../components/common/status.js';import {table} from '../components/common/table.js';import {metrics} from '../components/common/business.js';import {esc,formObject,fmt,localTime} from '../utils/format.js';import {action,toast,bind} from '../hooks/actions.js';
const sourceName=id=>{const s=research.sources.find(x=>x.id==id);return s?esc(s.name):'#'+id};
const stationLedger=round=>table(round.station_status.map(s=>({station:s.station,state:s.reported?'已上报':'缺站',shards:s.shards.map(x=>x.shard+'@v'+x.version+' ['+x.coverage[0]+','+x.coverage[1]+')').join(' · ')||'—',samples:s.samples,received:s.shards.length?localTime(s.shards[s.shards.length-1].received_at):'—'})),[{key:'station',label:'检修站'},{key:'state',label:'状态'},{key:'shards',label:'已接收分片@版本 · 覆盖区间'},{key:'samples',label:'样本数'},{key:'received',label:'最近接收'}]);
const manifestTable=manifest=>table(manifest.map(m=>({...m,coverage:'['+m.coverage[0]+','+m.coverage[1]+')'})),[{key:'station',label:'检修站'},{key:'shard',label:'分片'},{key:'version',label:'版本'},{key:'coverage',label:'覆盖区间'},{key:'samples',label:'样本数'}]);
const interpretationBlock=interpretations=>Object.values(interpretations).map(b=>'<h3>判读批次 · '+esc(b.name)+' <small>'+b.summary.samples+' 条 · 一致率 '+fmt(b.summary.accuracy)+'</small></h3>'+table(b.rows,[{key:'reference',label:domain.column_labels.reference},{key:'actual',label:domain.column_labels.actual},{key:'predicted',label:domain.column_labels.predicted},{key:'posterior',label:domain.column_labels.posterior}],{limit:30})).join('');
export async function federationPage(){
 const[rounds,model]=await Promise.all([federationApi.rounds(),federationApi.model().catch(()=>null)]);
 if(!research.fedRoundId&&rounds.length)research.fedRoundId=rounds[0].id;
 const round=research.fedRoundId?await federationApi.round(research.fedRoundId).catch(()=>null):null;
 research.fedBusy=!!(round&&round.merges.some(m=>['queued','running'].includes(m.state)));
 const adhoc=research.fedAdhoc;
 let html='<section class="page-heading"><p class="eyebrow">05 / FEDERATED SUMMARY</p><h1>各站只报汇总，原始样本不出站。</h1><p>各检修站按故障类型上报样本数、均值与离均差平方和；中心合并后的判读后验，与合并全部原始样本训练一致。</p></section>';
 html+=model?'<div class="method-sheet published-model"><p class="eyebrow">CURRENT PUBLISHED MODEL</p><h2>'+esc(model.payload.round_name)+' · 已发布</h2><p>轮次 #'+model.round_id+' · epoch '+model.epoch+' · '+model.payload.labels.length+' 类 · '+model.payload.total_samples+' 条汇总样本 · 采用 '+model.payload.manifest.length+' 个分片</p><small>清单 SHA256<br><code>'+esc(model.payload.checksum)+'</code></small></div>':'<div class="failure"><h2>尚无已发布联邦模型</h2><p>全部站点贡献及判读结果齐备并发布后，这里会显示当前判读口径；此前各轮缺站不影响既有研究。</p></div>';
 if(!rounds.length)return html+'<div class="empty"><strong>还没有联邦轮次。</strong><p>新建一轮，冻结参与站点与特征口径后开始接收分片。</p></div>'+createRoundSheet();
 html+='<label class="compact">联邦轮次<select id="fed-round-select">'+rounds.map(r=>'<option value="'+r.id+'" '+(r.id==research.fedRoundId?'selected':'')+'>#'+r.id+' · '+esc(r.name)+(r.missing_stations.length?' · 缺'+r.missing_stations.length+'站':'')+'</option>').join('')+'</label>';
 if(round){
  html+='<div class="run-title"><h2>'+esc(round.name)+'</h2>'+status(round.state)+'<small>#'+round.id+' · epoch '+round.epoch+'</small></div>';
  html+='<p class="fine">冻结特征口径：'+esc(round.features.join(' / '))+' · 平滑系数 '+round.smoothing+' · 判读批次：'+round.interpret_source_ids.map(sourceName).join('、')+' · 冻结站点 '+round.stations.length+' 个'+(round.published_at?' · 发布于 '+esc(localTime(round.published_at)):'')+'</p>';
  html+='<h2>站点台账 <small>缺站可追查；全部站点齐备才能发布</small></h2>'+stationLedger(round);
  if(round.missing_stations.length)html+='<p class="fine">缺站：'+esc(round.missing_stations.join('、'))+' —— 发布前必须补齐；期间判读继续使用已发布模型。</p>';
  html+='<div class="fed-actions">'+(session.writable&&round.state==='collecting'?'<button id="fed-seal">封轮并合并 →</button>':'')+(session.user.role==='admin'&&round.state==='collecting'?'<button id="fed-publish" class="secondary">发布本轮模型 ✓</button>':'')+'</div>';
  if(round.merges.length)html+='<h2>合并记录 <small>封轮快照逐次留痕</small></h2>'+table(round.merges.map(m=>({...m,created_at:localTime(m.created_at),state:mergeStates[m.state]||m.state,current:m.epoch===round.epoch?'采用':'已过期'})),[{key:'id',label:'#'},{key:'epoch',label:'epoch'},{key:'state',label:'状态'},{key:'current',label:'与当前贡献'},{key:'error',label:'错误'},{key:'created_at',label:'封轮时间'}]);
  if(round.candidate&&round.state==='collecting')html+='<h2>候选合并 <small>epoch '+round.candidate.epoch+(round.candidate.current?' · 与当前贡献一致，可发布':' · 已有更新贡献，需重新封轮')+'</small></h2>'+manifestTable(round.candidate.manifest);
  if(round.model){
   const p=round.model.payload;
   html+='<h2>本轮发布模型 <small>后验与合并原始样本训练一致</small></h2>'+metrics([{label:'故障类型',value:p.labels.length},{label:'汇总样本',value:p.total_samples},{label:'采用分片',value:p.manifest.length},{label:'epoch',value:p.epoch}]);
   html+='<h3>类先验</h3>'+table(p.labels.map(l=>({label:l,count:p.classes[l].count,prior:p.classes[l].prior})),[{key:'label',label:'故障类型'},{key:'count',label:'样本数'},{key:'prior',label:'类先验'}]);
   html+='<h3>本轮采用的贡献 <small>每轮可追查</small></h3>'+manifestTable(p.manifest);
   html+='<h2>判读结果 <small>选定批次 · 预览前30行</small></h2>'+interpretationBlock(p.interpretations);
  }
  if(session.writable&&round.state==='collecting')html+=contributionSheet(round);
 }
 html+=adhoc?'<h2>临时判读 <small>使用当前已发布模型 · '+esc(adhoc.round_name)+'</small></h2><p class="fine">'+esc(adhoc.source_name)+' · '+adhoc.summary.samples+' 条 · 一致率 '+fmt(adhoc.summary.accuracy)+'</p>'+table(adhoc.rows,[{key:'reference',label:domain.column_labels.reference},{key:'actual',label:domain.column_labels.actual},{key:'predicted',label:domain.column_labels.predicted},{key:'posterior',label:domain.column_labels.posterior}],{limit:30}):'';
 html+='<details class="adhoc-sheet"><summary>用已发布模型临时判读一个批次</summary>'+(model?'<form id="fed-adhoc"><label>判读批次<select name="source_id">'+research.sources.map(s=>'<option value="'+s.id+'">'+esc(s.name)+' · '+s.record_count+'条</option>').join('')+'</select></label><button type="submit">判读 →</button></form>':'<p>尚无已发布模型，暂不能临时判读。</p>')+'</details>';
 return html+createRoundSheet();
}
function contributionSheet(round){
 return '<details class="entry-sheet" open><summary>上报站点分片（带版本）</summary><form id="fed-contribute"><div class="parameter-fields"><label>检修站<select name="station">'+round.stations.map(s=>'<option>'+esc(s)+'</option>').join('')+'</select></label><label>分片标识<input name="shard" required value="part-1"></label><label>版本<input name="version" type="number" min="1" required value="1"></label><label>覆盖起<input name="start" type="number" min="0" required value="0"></label><label>覆盖止<input name="end" type="number" min="1" required value="1"></label></div><label>汇总统计 JSON（按故障类型 → 特征 → n/mean/m2）<textarea name="stats" rows="8" required placeholder=\'{"正常":{"rms":{"n":80,"mean":1.2,"m2":9.8}}}\'></textarea></label><p class="fine">同一分片重报同版同内容不重复计权；更正请提升版本号替换旧贡献；覆盖区间与其他分片重叠将被拒收。</p><button type="submit">上报分片 →</button></form><form id="fed-preview" class="inline-form"><label>站点侧计算工具 · 以既有批次充当站点本地数据<select name="source_id">'+research.sources.map(s=>'<option value="'+s.id+'">'+esc(s.name)+'</option>').join('')+'</select></label><button type="submit" class="secondary">计算汇总并填入</button></form></details>';
}
function createRoundSheet(){
 if(!session.writable)return '<aside class="method-sheet"><p>阅读者可查看轮次、缺站与发布模型；建轮、上报与封轮由分析师或负责人执行，发布需负责人。</p></aside>';
 return '<details class="entry-sheet"><summary>新建联邦轮次</summary><form id="fed-create"><label>轮次名称<input name="name" required value="十二月三站联合建模"></label><label>参与站点（逗号分隔，创建后冻结）<input name="stations" required value="北郊检修站,临港检修站,山南检修站"></label><label>判读批次（按住Ctrl多选）<select name="interpret_source_ids" multiple size="3">'+research.sources.map(s=>'<option value="'+s.id+'" selected>'+esc(s.name)+'</option>').join('')+'</select></label><label>方差平滑系数<input name="smoothing" type="number" step="any" value="1e-9"></label><p class="fine">创建即冻结参与站点与特征口径（'+esc(domain.fields.filter(f=>f[2]==='number').map(f=>f[1]).join(' / '))+'）。</p><button type="submit">创建轮次 →</button></form></details>';
}
export function bindFederation(refresh){
 bind('#fed-round-select','change',async e=>{research.fedRoundId=Number(e.target.value);research.fedAdhoc=null;await refresh()});
 action('#fed-create',async form=>{const data=formObject(form);const ids=[...form.querySelector('[name=interpret_source_ids]').selectedOptions].map(o=>Number(o.value));const r=await federationApi.create({name:data.name,stations:data.stations.split(/[,，]/).map(s=>s.trim()).filter(Boolean),interpret_source_ids:ids,smoothing:Number(data.smoothing)});research.fedRoundId=r.id;await refresh();toast('联邦轮次已创建，站点与特征口径已冻结')});
 action('#fed-contribute',async form=>{const data=formObject(form);let stats;try{stats=JSON.parse(data.stats)}catch{toast('汇总统计不是合法JSON',true);return}await federationApi.contribute(research.fedRoundId,{station:data.station,shard:data.shard,version:Number(data.version),coverage:[Number(data.start),Number(data.end)],stats});await refresh();toast('分片已接收并计入本轮')});
 action('#fed-preview',async form=>{const data=formObject(form);const p=await federationApi.preview(Number(data.source_id));const ta=document.querySelector('#fed-contribute [name=stats]');ta.value=JSON.stringify(p.stats);document.querySelector('#fed-contribute [name=start]').value=0;document.querySelector('#fed-contribute [name=end]').value=p.samples;toast('已按 '+p.source_name+' 计算站点汇总，请核对后上报')});
 bind('#fed-seal','click',async()=>{try{await federationApi.seal(research.fedRoundId);await refresh();toast('已封轮，合并进入队列')}catch(e){toast(e.message,true)}});
 bind('#fed-publish','click',async()=>{try{await federationApi.publish(research.fedRoundId);await refresh();toast('本轮模型已发布')}catch(e){toast(e.message,true)}});
 action('#fed-adhoc',async form=>{const data=formObject(form);research.fedAdhoc=await federationApi.interpret(Number(data.source_id));await refresh();toast('已用当前发布模型完成判读')});
}
