'use strict';
const http = require('http');
const crypto = require('crypto');
const {WebSocketServer, WebSocket} = require('ws');
const PORT = Number(process.env.PORT || 3001);
const rooms = new Map();
const clients = new WeakMap();
const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function code(){let c;do{c=Array.from(crypto.randomBytes(6),v=>alphabet[v%alphabet.length]).join('')}while(rooms.has(c));return c;}
function send(ws,obj){if(ws?.readyState===WebSocket.OPEN){if(obj.type==='snapshot'&&ws.bufferedAmount>48000)return;if(obj.type==='input'&&ws.bufferedAmount>16000)return;ws.send(JSON.stringify(obj));}}
function lobbyState(room){const seats=[room.host,room.guest,...room.extra];for(const peer of seats)send(peer,{type:'lobby_state',slots:seats.map(Boolean),mode:room.mode||'classic'});}
function cleanup(ws){const info=clients.get(ws);if(!info)return;clients.delete(ws);const room=rooms.get(info.code);if(!room)return;
 if(info.role==='host'){for(const other of [room.guest,...room.extra]){send(other,{type:'player_left'});if(other)clients.delete(other)}rooms.delete(info.code)}
 else if(room.guest===ws){room.guest=null;if(room.mode==='ffa'&&room.started){for(const p of [room.host,...room.extra])send(p,{type:'ffa_left',slot:2});}else{room.started=false;send(room.host,{type:'player_left'});}lobbyState(room)}
 else {const slot=room.extra.indexOf(ws);if(slot>=0){room.extra[slot]=null;if(room.mode==='ffa'&&room.started)for(const p of [room.host,room.guest,...room.extra])send(p,{type:'ffa_left',slot:slot+3});lobbyState(room)}}
}
const server=http.createServer((req,res)=>{res.writeHead(200,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});res.end(JSON.stringify({name:'Square Off multiplayer relay',status:'ok',rooms:rooms.size,players:[...rooms.values()].reduce((n,r)=>n+1+(r.guest?1:0)+r.extra.filter(Boolean).length,0)}));});
const wss=new WebSocketServer({server,maxPayload:1024*1024,perMessageDeflate:false});
wss.on('connection',ws=>{
 ws.isAlive=true;ws.on('pong',()=>ws.isAlive=true);
 ws.on('message',data=>{
  if(data.length>800000)return;
  let m;try{m=JSON.parse(data.toString())}catch{return}
  if(!m||typeof m.type!=='string')return;
  // Server-side echo measures the actual browser-to-relay round trip.
  if(m.type==='latency_probe'&&Number.isInteger(m.seq)){
   send(ws,{type:'latency_reply',seq:m.seq});return;
  }
  const user=clients.get(ws);
  if(!user){
   if(m.type==='create'){
    const c=code();rooms.set(c,{code:c,host:ws,guest:null,extra:[null,null],mode:'classic',started:false,created:Date.now()});clients.set(ws,{code:c,role:'host',slot:1});send(ws,{type:'created',code:c,slot:1});lobbyState(rooms.get(c));return;
   }
   if(m.type==='join'){
    const c=String(m.code||'').trim().toUpperCase(),room=rooms.get(c);
    if(!room){send(ws,{type:'error',message:'Room not found. Check the code.'});return}
    if(room.started){send(ws,{type:'error',message:'Match already started.'});return}
    const slot=!room.guest?2:!room.extra[0]?3:!room.extra[1]?4:0;
    if(!slot){send(ws,{type:'error',message:'Room is full (4 players max).'});return}
    if(slot===2)room.guest=ws;else room.extra[slot-3]=ws;
    clients.set(ws,{code:c,role:slot===2?'guest':'spectator',slot});
    send(ws,{type:'joined',code:c,slot});
    send(room.host,{type:'player_joined',slot});
    lobbyState(room);return;
   }
   send(ws,{type:'error',message:'Create or join a room first.'});return;
  }
  const room=rooms.get(user.code);if(!room)return;
  const peer=user.role==='host'?room.guest:room.host;
  if(user.role==='host'&&m.type==='set_mode'&&!room.started){
   if(m.mode==='classic'||m.mode==='ffa'){room.mode=m.mode;lobbyState(room)}
   return;
  }
  if(user.role==='host'&&m.type==='start'&&peer){if(room.mode==='ffa'){const peers=[room.host,room.guest,...room.extra],count=peers.filter(Boolean).length;if(count<3)return;room.started=true;for(const p of peers)send(p,{type:'ffa_start',slots:peers.map(Boolean)});return;}room.started=true;send(peer,{type:'start'});for(const spectator of room.extra)send(spectator,{type:'spectate_wait'});return}
  if(!room.started)return;
  if(room.mode==='ffa'){
   const peers=[room.host,room.guest,...room.extra];
   if(user.role==='host'&&['ffa_snapshot','ffa_draft','ffa_round','ffa_end'].includes(m.type))for(const p of peers.slice(1))send(p,{type:m.type,payload:m.payload});
   else if(user.role!=='host'&&m.type==='ffa_input'&&m.input&&typeof m.input==='object')send(room.host,{type:'ffa_input',slot:user.slot,input:m.input});
   else if(user.role!=='host'&&m.type==='ffa_choose'&&typeof m.card==='string')send(room.host,{type:'ffa_choose',slot:user.slot,card:m.card.slice(0,48)});
   return;
  }
  if(!peer)return;
  // After a finished match, relay rematch votes both ways without closing the room.
  if(m.type==='rematch_ready'){send(peer,{type:'rematch_ready'});return}
  if(m.type==='rematch_start'&&user.role==='host'){send(peer,{type:'rematch_start'});return}
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
