'use strict';
const http = require('http');
const crypto = require('crypto');
const {WebSocketServer, WebSocket} = require('ws');
const PORT = Number(process.env.PORT || 3001);
const rooms = new Map();
const clients = new WeakMap();
const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function code(){let c;do{c=Array.from(crypto.randomBytes(6),v=>alphabet[v%alphabet.length]).join('')}while(rooms.has(c));return c;}
function send(ws,obj){if(ws?.readyState===WebSocket.OPEN){if(obj.type==='snapshot'&&ws.bufferedAmount>90000)return;ws.send(JSON.stringify(obj));}}
function cleanup(ws){const info=clients.get(ws);if(!info)return;clients.delete(ws);const room=rooms.get(info.code);if(!room)return;
 if(info.role==='host'){send(room.guest,{type:'player_left'});if(room.guest)clients.delete(room.guest);rooms.delete(info.code)}
 else if(room.guest===ws){room.guest=null;room.started=false;send(room.host,{type:'player_left'});}
}
const server=http.createServer((req,res)=>{res.writeHead(200,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});res.end(JSON.stringify({name:'Square Off multiplayer relay',status:'ok',rooms:rooms.size,players:[...rooms.values()].reduce((n,r)=>n+1+(r.guest?1:0),0)}));});
const wss=new WebSocketServer({server,maxPayload:1024*1024,perMessageDeflate:false});
wss.on('connection',ws=>{
 ws.isAlive=true;ws.on('pong',()=>ws.isAlive=true);
 ws.on('message',data=>{
  if(data.length>800000)return;
  let m;try{m=JSON.parse(data.toString())}catch{return}
  if(!m||typeof m.type!=='string')return;
  const user=clients.get(ws);
  if(!user){
   if(m.type==='create'){
    const c=code();rooms.set(c,{code:c,host:ws,guest:null,started:false,created:Date.now()});clients.set(ws,{code:c,role:'host'});send(ws,{type:'created',code:c});return;
   }
   if(m.type==='join'){
    const c=String(m.code||'').trim().toUpperCase(),room=rooms.get(c);
    if(!room){send(ws,{type:'error',message:'Room not found. Check the code.'});return}
    if(room.guest){send(ws,{type:'error',message:'Room is full (2 players max).'});return}
    if(room.started){send(ws,{type:'error',message:'Match already started.'});return}
    room.guest=ws;clients.set(ws,{code:c,role:'guest'});send(ws,{type:'joined',code:c});send(room.host,{type:'player_joined'});return;
   }
   send(ws,{type:'error',message:'Create or join a room first.'});return;
  }
  const room=rooms.get(user.code);if(!room)return;
  const peer=user.role==='host'?room.guest:room.host;
  if(user.role==='host'&&m.type==='start'&&peer){room.started=true;send(peer,{type:'start'});return}
  if(!room.started||!peer)return;
  if(user.role==='host'&&['snapshot','draft','host_draft','match_end','round_prepare','round_go'].includes(m.type)){
   // Relay only expected match fields, no arbitrary client commands.
   if(m.type==='snapshot'&&m.payload&&typeof m.payload==='object')send(peer,{type:'snapshot',epoch:m.epoch,payload:m.payload});
   else if(m.type==='round_prepare'&&Number.isInteger(m.epoch)&&m.payload&&typeof m.payload==='object')send(peer,{type:'round_prepare',epoch:m.epoch,payload:m.payload});
   else if(m.type==='round_go'&&Number.isInteger(m.epoch))send(peer,{type:'round_go',epoch:m.epoch,duration:m.duration});
   else if(m.type==='host_draft')send(peer,{type:'host_draft'});
   else if(m.type==='draft'&&Array.isArray(m.options))send(peer,{type:'draft',options:m.options.slice(0,5)});
   else if(m.type==='match_end')send(peer,{type:'match_end',winner:m.winner,scores:m.scores});
  }else if(user.role==='guest'&&m.type==='input'&&m.input&&typeof m.input==='object'){
   const i=m.input;send(peer,{type:'input',input:{left:!!i.left,right:!!i.right,jump:!!i.jump,down:!!i.down,fire:!!i.fire,shield:!!i.shield,dash:!!i.dash,aim:Number.isFinite(i.aim)?i.aim:0}});
  }else if(user.role==='guest'&&m.type==='round_ready'&&Number.isInteger(m.epoch))send(peer,{type:'round_ready',epoch:m.epoch});
  else if(user.role==='guest'&&m.type==='choose'&&typeof m.card==='string')send(peer,{type:'choose',card:m.card.slice(0,32)});
 });
 ws.on('close',()=>cleanup(ws));ws.on('error',()=>cleanup(ws));
});
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}},30000);
wss.on('close',()=>clearInterval(heartbeat));
server.listen(PORT,'0.0.0.0',()=>console.log(`Square Off room server listening on ${PORT}`));
