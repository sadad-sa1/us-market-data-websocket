import WebSocket from 'ws';
import http from 'node:http';

const token = process.env.BENZINGA_API_TOKEN;
const botToken = process.env.TELEGRAM_BOT_TOKEN;
const chatId = process.env.TELEGRAM_CHAT_ID;
const tickers = process.env.TICKERS || '';
const port = Number(process.env.PORT || 10000);
let connected = false;
let received = 0;
let lastEventAt = null;
let lastError = null;
const seen = new Set();

http.createServer((req,res) => {
  if (req.url !== '/health') { res.writeHead(404); res.end('Not found'); return; }
  res.setHeader('content-type','application/json');
  res.end(JSON.stringify({ running:true, configured:!!token, connected, received, lastEventAt, lastError }));
}).listen(port,'0.0.0.0',()=>console.log('Health server listening on',port));

async function notify(message) {
  if (!botToken || !chatId) return;
  try {
    const response = await fetch('https://api.telegram.org/bot'+botToken+'/sendMessage',{
      method:'POST',headers:{'content-type':'application/json'},
      body:JSON.stringify({chat_id:chatId,text:message.slice(0,3900),disable_web_page_preview:true})
    });
    if (!response.ok) console.error('Telegram HTTP',response.status);
  } catch(e) { console.error('Telegram error',e.message); }
}

async function stream() {
  let delay = 2000;
  while (true) {
    if (!token) { lastError='BENZINGA_API_TOKEN not configured'; await new Promise(r=>setTimeout(r,60000)); continue; }
    const url=new URL('wss://api.benzinga.com/api/v1/news/stream');
    url.searchParams.set('token',token);
    if (tickers) url.searchParams.set('tickers',tickers);
    await new Promise(resolve=>{
      const ws=new WebSocket(url,{handshakeTimeout:15000});
      let heartbeat;
      ws.on('open',()=>{
        connected=true;lastError=null;delay=2000;
        console.log('Benzinga websocket connected');
        heartbeat=setInterval(()=>{ if(ws.readyState===WebSocket.OPEN) ws.ping(); },30000);
      });
      ws.on('message',async buffer=>{
        const raw=buffer.toString();
        if(raw==='pong') return;
        let event;try{event=JSON.parse(raw)}catch{return;}
        received++;lastEventAt=new Date().toISOString();
        const content=event?.data?.content;
        const id=event?.id;
        if(id && seen.has(id)) return;
        if(id){seen.add(id);if(seen.size>3000)seen.delete(seen.values().next().value);}
        if(event?.kind!=='news'||event?.data?.action?.toLowerCase()!=='created'||!content) return;
        const names=(content.stocks||[]).map(s=>s.name).filter(Boolean).join(', ');
        console.log('News:',JSON.stringify({id,title:content.title,tickers:names}));
        await notify('📰 Benzinga News\n'+(content.title||'')+'\nSymbols: '+(names||'N/A')+'\n'+(content.url||''));
      });
      ws.on('error',e=>{lastError=e.message;console.error('Websocket error:',e.message);});
      ws.on('close',code=>{connected=false;clearInterval(heartbeat);console.log('Websocket closed:',code);resolve();});
    });
    await new Promise(r=>setTimeout(r,delay+Math.floor(Math.random()*1000)));
    delay=Math.min(delay*2,30000);
  }
}
stream().catch(e=>{console.error(e);process.exitCode=1;});
