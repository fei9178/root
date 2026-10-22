/* 聊天室 - 前端逻辑 (分离版) */
(function () {
"use strict";
var API = "";
var state = {
user: null, nickname: "", isAdmin: false,
curRoom: "public", view: "room",
rooms: [], evSource: null, online: []
};
var friends = {list:[], pending:[], map:{}, unread:{}};
var pmState = {to:null, toNick:"", unread:0};

function $(s, root){return (root||document).querySelector(s);}
function el(html){var t=document.createElement("template");t.innerHTML=html.trim();return t.content.firstChild;}
function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,function(c){return{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c];});}
function iniOf(n){return (n&&n.charAt(0).toUpperCase())||"?";}
function toast(m,type){var o=$("#toast");if(o)o.remove();
var d=el('<div id="toast" class="'+(type||"")+'">'+esc(m)+"</div>");
d.style.cssText="position:fixed;left:50%;bottom:80px;transform:translateX(-50%);padding:10px 18px;background:rgba(0,0,0,.85);color:#fff;border-radius:22px;font-size:13px;z-index:999;";
document.body.appendChild(d);
setTimeout(function(){d.style.opacity="0";},1500);setTimeout(function(){d.remove();},1800);}
function post(path,data,cb){fetch(API+path,{method:"POST",headers:{"Content-Type":"application/json"},
body:JSON.stringify(data||{})}).then(function(r){return r.json();})
.then(function(j){cb&&cb(j);}).catch(function(){cb&&cb({ok:false,msg:"网络错误"});});}
function get(path,cb){fetch(API+path).then(function(r){return r.json();}).then(function(j){cb&&cb(j);})
.catch(function(){cb&&cb({ok:false,msg:"网络错误"});});}

/* ---------- 登录/注册 (事件委托, 不再依赖元素何时渲染) ---------- */
function showAuthMsg(m,cls){var d=$("#auth-msg");if(d){d.textContent=m;d.className="msg-line"+(cls?" "+cls:"");}}
function tab(name){["login","register"].forEach(function(k){
var tb=$("#tab-"+k), vw=$("#view-"+k);
if(tb)tb.classList.toggle("on",k===name);
if(vw)vw.classList.toggle("hidden",k!==name);});showAuthMsg("");}
function doLogin(u,p){
if(!u||!p)return showAuthMsg("请填写账号和密码","err");
post("/api/login",{username:u,password:p},function(j){if(j.ok)enter(j.user);else showAuthMsg(j.msg,"err");});}
function doRegister(u,n,p){
if(!u||!p)return showAuthMsg("请填写账号和密码","err");
post("/api/register",{username:u,password:p,nickname:n},function(j){if(j.ok){
showAuthMsg("注册成功, 正在登录…","ok");
post("/api/login",{username:u,password:p},function(k){if(k.ok)enter(k.user);else showAuthMsg(k.msg,"err");});
}else showAuthMsg(j.msg,"err");});}
function enter(user){state.user=user.username;state.nickname=user.nickname;state.isAdmin=user.is_admin;
localStorage.setItem("chat_user",user.username);
localStorage.setItem("chat_pass",$("#login-pass").value||$("#reg-pass").value);
$("#me-avatar").textContent=iniOf(user.nickname);
$("#me-nick").textContent=user.nickname;
$("#me-user").textContent="@"+user.username+(user.is_admin?" · 管理员":"");
var ba=$("#btn-admin");if(ba)ba.textContent=user.is_admin?"👑 管理后台":"👑 管理员登录";
$("#screen-login").classList.add("hidden");
$("#screen-chat").classList.remove("hidden");
openStream();loadRooms();loadFriends();}
function tryAutoLogin(){var u=localStorage.getItem("chat_user"),p=localStorage.getItem("chat_pass");
if(u&&p)post("/api/login",{username:u,password:p},function(j){if(j.ok)enter(j.user);});}
function logout(){localStorage.removeItem("chat_pass");
if(state.evSource)state.evSource.close();location.reload();}

/* ---------- SSE ---------- */
function openStream(){if(state.evSource)state.evSource.close();
state.evSource=new EventSource("/api/stream?user="+encodeURIComponent(state.user));
state.evSource.onmessage=function(e){try{handleEvent(JSON.parse(e.data));}catch(err){}};
state.evSource.onerror=function(){setTimeout(openStream,3000);};}
function handleEvent(ev){switch(ev.t){
case "hello":state.rooms=ev.rooms;renderRoomTabs();renderOnline(ev.online||[]);break;
case "rooms":state.rooms=ev.rooms;renderRoomTabs();break;
case "online":renderOnline(ev.online||[]);break;
case "friends":renderFriendList(ev.pending||[]);break;
case "msg":if(ev.mtype==="group"){handleGroup(ev);}else if(ev.mtype==="pm"){handlePm(ev);}break;}}

/* ---------- 房间 ---------- */
function loadRooms(){get("/api/rooms",function(j){if(j.ok){state.rooms=j.rooms;renderRoomTabs();}});}
function renderRoomTabs(){var box=$("#room-tabs");if(box)box.innerHTML="";
state.rooms.forEach(function(r){var b=el('<button class="room-tab'+(r.id===state.curRoom?" on":"")+'" data-rid="'+esc(r.id)+'">'
+(r.has_password?'🔒 ':"")+esc(r.name)+"</button>");
if(box)box.appendChild(b);});
var mb=$("#menu-rooms");if(mb)mb.innerHTML="";
state.rooms.forEach(function(r){var btn=el('<button class="btn-block" data-rid="'+esc(r.id)+'" data-role="menu">'+(r.has_password?'🔒 ':"")+esc(r.name)+"</button>");
if(mb)mb.appendChild(btn);});}
function switchRoom(id){state.curRoom=id;state.view="room";
var r=state.rooms.filter(function(x){return x.id===id;})[0]||{id:id,name:"公共大厅"};
var ct=$("#cur-title");if(ct)ct.textContent=r.name;
var cs=$("#cur-sub");if(cs)cs.textContent=(r.has_password?"🔒 加密房间 · ":"")+(r.online?r.online+" 人在此 · ":"")+id;
Array.prototype.forEach.call(document.querySelectorAll("#room-tabs .room-tab"),function(b,i){
b.classList.toggle("on",state.rooms[i]&&state.rooms[i].id===id);});
closeDrawer("drawer-pm");
var m=$("#messages");if(m)m.innerHTML="";loadHistory(id);}
function joinRoom(rid){var r=state.rooms.filter(function(x){return x.id===rid;})[0];
if(r&&r.has_password){askRoomPassword(rid,function(ok){if(ok)doJoin(rid);});}else{doJoin(rid);}}
function askRoomPassword(rid,cb){var box=$("#messages");
if(box)box.innerHTML="";
var card=el('<div class="pwd-prompt"><h4>🔒 房间已加密</h4><p>输入密码以加入</p>'
+'<input type="password" id="rp-input" placeholder="房间密码" autofocus>'
+'<div class="row"><button class="ok" id="rp-ok">进入</button><button class="no" id="rp-cancel">取消</button></div></div>');
if(box)box.appendChild(card);
var inp=$("#rp-input");if(inp)inp.focus();
var rok=$("#rp-ok");if(rok)rok.onclick=function(){var v=$("#rp-input").value;
post("/api/room/join",{user:state.user,room:rid,password:v},function(j){
if(j.ok){cb(true);}else{toast(j.msg,"err");cb(false);}});};
var rcn=$("#rp-cancel");if(rcn)rcn.onclick=function(){cb(false);switchRoom("public");};
var rpi=$("#rp-input");if(rpi)rpi.addEventListener("keydown",function(e){if(e.key==="Enter"){var b=$("#rp-ok");if(b)b.click();};});}
function doJoin(rid){post("/api/room/join",{user:state.user,room:rid},function(j){
if(!j.ok)return toast(j.msg,"err");
if(state.rooms.filter(function(x){return x.id===rid;}).length===0)loadRooms();
switchRoom(rid);});}
function loadHistory(room){get("/api/history?room="+encodeURIComponent(room),function(j){
if(!j.ok)return;var m=$("#messages");if(m)m.innerHTML="";
j.messages.forEach(function(mm){appendGroup({user:mm.from,text:mm.content,time:mm.time},true);});scrollBottom();});}

/* ---------- 群消息 ---------- */
function handleGroup(ev){appendGroup(ev,ev.room===state.curRoom);
if(ev.room===state.curRoom)scrollBottom();}
function appendGroup(m,visible){if(!visible)return;
var mine=m.user===state.user;var who=mine?"我":m.user;
var d=el('<div class="msg'+(mine?" me":"")+'">'+'<div class="avatar">'+iniOf(who)+"</div>"+
'<div><div class="who">'+esc(who)+'<span class="time">'+esc(m.time||"")+"</span></div>"+
'<div class="bubble">'+esc(m.text)+"</div></div></div>");
var box=$("#messages");if(box)box.appendChild(d);}
function scrollBottom(){var m=$("#messages");if(m)m.scrollTop=m.scrollHeight;}

/* ---------- 发送 ---------- */
function send(){var inp=$("#input"),v=inp?inp.value.trim():"";if(!v)return;
post("/api/send",{user:state.user,room:state.curRoom,text:v},function(j){if(!j.ok)toast(j.msg,"err");});
if(inp)inp.value="";}

/* ---------- 在线用户 ---------- */
function renderOnline(list){state.online=list;var box=$("#user-list");if(!box)return;box.innerHTML="";
if(!list.length)box.innerHTML='<div class="empty">当前没有人在线</div>';
list.forEach(function(u){
var st=friends.map[u.username]||"";
var tag=st==="friend"?'<span class="tag friend">好友</span>':st==="pending"?'<span class="tag pending">请求中</span>':"";
var it=el('<div class="user-item" data-u="'+esc(u.username)+'">'
+'<span class="dot"></span>'+'<div class="un">'+esc(u.nickname)
+' <small style="color:var(--sub)">@'+esc(u.username)+"</small></div>"
+tag+'<button class="btn-mini" data-act="pm">私聊</button>'
+'<button class="btn-mini ok" data-act="friend">加好友</button></div>');
box.appendChild(it);});
var bu=$("#btn-users");if(bu)bu.classList.toggle("on",list.length>0);}

/* ---------- 好友 ---------- */
function loadFriends(){get("/api/friends?user="+encodeURIComponent(state.user),function(j){
if(!j.ok)return;friends.list=j.friends||[];friends.pending=j.pending||[];friends.map={};
friends.list.forEach(function(f){friends.map[f.username]=f.status;});
renderFriendDot();renderFriendList(friends.pending);});}
function renderFriendDot(){var n=friends.pending.length,b=$("#friend-dot");
if(!b)return;b.style.display=n>0?"flex":"none";b.textContent=n>9?"9+":String(n);}
function renderFriendList(pending){var box=$("#friend-list");if(!box)return;box.innerHTML="";
if(pending&&pending.length){box.innerHTML='<div class="sec-title">新的请求 <span class="badge-new">'+pending.length+"</span></div>";
pending.forEach(function(u){var it=el('<div class="user-item"><span class="dot pending"></span>'
+'<div class="un">'+esc(u.nickname)+' <small style="color:var(--sub)">@'+esc(u.username)+"</small></div>"
+'<button class="btn-mini ok" data-act="acc" data-u="'+esc(u.username)+'">同意</button>'
+'<button class="btn-mini no" data-act="rej" data-u="'+esc(u.username)+'">拒绝</button></div>');
box.appendChild(it);});}
box.innerHTML+='<div class="sec-title">我的好友 <span style="color:var(--sub);font-size:11px;">'
+(friends.list.filter(function(f){return f.status==="friend";}).length)+"</span></div>";
var fl=friends.list.filter(function(f){return f.status==="friend";});
if(!fl.length)box.innerHTML+='<div class="empty">还没有好友, 去在线列表或搜索添加吧</div>';
fl.forEach(function(f){var it=el('<div class="user-item" data-u="'+esc(f.username)+'">'
+'<span class="dot"></span>'+'<div class="un">'+esc(f.nickname)
+' <small style="color:var(--sub)">@'+esc(f.username)+"</small></div>"
+'<button class="btn-mini" data-act="pm">私聊</button>'
+'<button class="btn-mini no" data-act="del" data-u="'+esc(f.username)+'">删除</button></div>');
box.appendChild(it);});}
function openFriendPanel(){loadFriends();openDrawer("drawer-friends");}
function addFriend(u){post("/api/friend/add",{user:state.user,target:u},function(j){toast(j.msg,j.ok?"ok":"err");loadFriends();});}
function acceptFriend(u){post("/api/friend/accept",{user:state.user,target:u},function(j){toast(j.msg,j.ok?"ok":"err");loadFriends();});}
function rejectFriend(u){post("/api/friend/reject",{user:state.user,target:u},function(j){toast(j.msg,j.ok?"ok":"err");loadFriends();});}
function delFriend(u){post("/api/friend/remove",{user:state.user,target:u},function(j){toast(j.msg,j.ok?"ok":"err");loadFriends();});}

/* ---------- 私信 ---------- */
function openPm(uname,nick){pmState.to=uname;pmState.toNick=nick||uname;
var pt=$("#pm-title");if(pt)pt.textContent=nick||uname;
var pa=$("#pm-avatar");if(pa)pa.textContent=iniOf(nick||uname);
var pm=$("#pm-messages");if(pm)pm.innerHTML='<div class="msg sys"><div class="bubble">正在加载历史…</div></div>';
closeDrawer("drawer-users");closeDrawer("drawer-friends");openDrawer("drawer-pm");
loadPmHistory(uname);}
function loadPmHistory(u){get("/api/pm/history?user="+encodeURIComponent(state.user)+"&with="+encodeURIComponent(u),
function(j){var pm=$("#pm-messages");if(pm)pm.innerHTML="";
if(!j.ok||!j.messages||!j.messages.length){
if(pm)pm.innerHTML='<div class="msg sys"><div class="bubble">还没有聊天记录, 发条消息打个招呼吧</div></div>';return;}
j.messages.forEach(function(m){appendPm(m.from===state.user,false,{text:m.text,time:m.time});});
scrollPm();});}
function handlePm(ev){var me=ev.user===state.user;
var pm=$("#drawer-pm");var on=pm&&pm.classList.contains("on");
if(on&&pmState.to&&pmState.to===ev.user){appendPm(me,false,ev);scrollPm();}
else if(ev.user!==state.user){toast("新私信: "+ev.text.slice(0,24));}}
function appendPm(mine,sys,m){if(sys){var d=el('<div class="msg sys"><div class="bubble">'+esc(m.text)+"</div></div>");
var pm=$("#pm-messages");if(pm)pm.appendChild(d);return;}
var who=mine?"我":(pmState.toNick||m.user);
var d=el('<div class="msg'+(mine?" me":"")+" pm\">"+'<div class="avatar">'+iniOf(who)+"</div>"+
'<div><div class="who">'+esc(who)+'<span class="time">'+esc(m.time||"")+"</span></div>"+
'<div class="bubble">'+esc(m.text)+"</div></div></div>");
var pm=$("#pm-messages");if(pm)pm.appendChild(d);}
function scrollPm(){var p=$("#pm-messages");if(p)p.scrollTop=p.scrollHeight;}
function sendPm(){var inp=$("#pm-input"),v=inp?inp.value.trim():"";if(!v||!pmState.to)return;
post("/api/pm",{user:state.user,to:pmState.to,text:v});
appendPm(true,false,{text:v,time:new Date().toISOString().slice(0,16).replace("T"," ")});
if(inp)inp.value="";scrollPm();}

/* ---------- 抽屉 ---------- */
function openDrawer(id){var el=$("#"+id);if(el)el.classList.add("on");}
function closeDrawer(id){var el=$("#"+id);if(el)el.classList.remove("on");}
function openAdmin(){
closeDrawer("drawer-menu");
if(state.isAdmin){
window.open("admin.html","_blank");
}else{
var p=prompt("请输入管理员密码\n(管理员账号默认为 admin)");
if(p===null||p==="")return;
post("/api/login",{username:"admin",password:p},function(j){
if(j.ok&&j.user&&j.user.is_admin){
state.isAdmin=true;var ba=$("#btn-admin");if(ba)ba.textContent="👑 管理后台";
window.open("admin.html","_blank");}
else if(j.ok){toast("该账号没有管理员权限","err");}
else {toast(j.msg,"err");}});
}
}

/* ---------- 个人设置 ---------- */
function saveNick(){var inp=$("#nick-input"),n=inp?inp.value.trim():"";if(!n)return toast("昵称不能为空","err");
post("/api/nickname",{user:state.user,nickname:n},function(j){if(j.ok){state.nickname=n;
var mn=$("#me-nick");if(mn)mn.textContent=n;toast("昵称已更新","ok");}else toast(j.msg,"err");});}
function savePass(){var o=$("#pass-old"),nw=$("#pass-new"),ov=o?o.value:"",nv=nw?nw.value:"";
if(!ov||!nv)return toast("请填写完整","err");if(nv.length<4)return toast("新密码至少4位","err");
post("/api/password",{user:state.user,old_password:ov,new_password:nv},function(j){if(j.ok){
var po=$("#pass-old"),pn=$("#pass-new");if(po)po.value="";if(pn)pn.value="";
toast("密码已修改, 请重新登录","ok");setTimeout(function(){location.reload();},1200);}else toast(j.msg,"err");});}

/* ---------- 搜索 / 建房间 ---------- */
function doSearch(){var inp=$("#search-input"),k=inp?inp.value.trim():"";if(!k)return;
get("/api/users/search?user="+encodeURIComponent(state.user)+"&q="+encodeURIComponent(k),
function(j){var box=$("#search-result");if(box)box.innerHTML="";
if(!j.ok||!j.users||!j.users.length){if(box)box.innerHTML='<div class="empty">没有找到相关用户</div>';return;}
j.users.forEach(function(u){if(u.username===state.user)return;
var st=friends.map[u.username];var tag=st==="friend"?'<span class="tag friend">好友</span>'
:st==="pending"?'<span class="tag pending">请求中</span>':"";
var btn=st==="friend"?'<button class="btn-mini" data-act="pm">私聊</button>'
:st==="pending"?'<span class="tag pending">请求中</span>':'<button class="btn-mini ok" data-act="add">加好友</button>';
var it=el('<div class="user-item"><span class="dot off"></span>'
+'<div class="un">'+esc(u.nickname)+' <small style="color:var(--sub)">@'+esc(u.username)+"</small></div>"
+tag+btn+"</div>");
box.appendChild(it);});});}
function createRoom(){var nm=$("#new-room"),n=nm?nm.value.trim():"";if(!n)return;
var pw=$("#new-room-pwdval");var pv=$("#new-room-pwd");
post("/api/room/create",{user:state.user,name:n,password:(pv&&pv.checked?pw.value:"")},function(j){
if(j.ok){var r=$("#new-room");if(r)r.value="";var rv=$("#new-room-pwdval");if(rv)rv.value="";
var ck=$("#new-room-pwd");if(ck)ck.checked=false;if(rv)rv.style.display="none";
toast("房间已创建","ok");loadRooms();switchRoom(j.id);}
else toast(j.msg,"err");});}

/* ============ 全局事件委托: 无论元素何时渲染, 都能响应 ============ */
document.addEventListener("click", function (e) {
var t = e.target;
if(!t || !t.getAttribute) return;
var c = t.getAttribute("data-close");
if(c){closeDrawer(c);return;}
/* 登录/注册 tab 切换 */
if(t.id === "tab-login"){tab("login");return;}
if(t.id === "tab-register"){tab("register");return;}
/* 房间标签点击 */
var rid=t.getAttribute&&t.getAttribute("data-rid");
if(rid){var role=t.getAttribute("data-role");
if(role==="menu"){joinRoom(rid);closeDrawer("drawer-menu");}else{switchRoom(rid);}
return;}
/* 登录注册按钮 */
if(t.id === "btn-login"){var lu=$("#login-user"),lp=$("#login-pass");doLogin(lu?lu.value:"",lp?lp.value:"");return;}
if(t.id === "btn-register"){var ru=$("#reg-user"),rn=$("#reg-nick"),rp=$("#reg-pass");doRegister(ru?ru.value:"",rn?rn.value:"",rp?rp.value:"");return;}
/* 发送 */
if(t.id === "send"){send();return;}
if(t.id === "pm-send"){sendPm();return;}
/* 顶部按钮 */
if(t.id === "btn-users"){openDrawer("drawer-users");return;}
if(t.id === "btn-friends"){openFriendPanel();return;}
if(t.id === "btn-menu"){openDrawer("drawer-menu");return;}
if(t.id === "btn-back"){
if($("#drawer-pm")&&$("#drawer-pm").classList.contains("on")){closeDrawer("drawer-pm");pmState.to=null;}
else if(state.view!=="room"){state.view="room";}else{switchRoom("public");}
return;}
if(t.id === "btn-logout"){logout();return;}
if(t.id === "btn-admin"){openAdmin();return;}
/* 个人设置 */
if(t.id === "btn-nick"){saveNick();return;}
if(t.id === "btn-pass"){savePass();return;}
/* 搜索 / 建房间 */
if(t.id === "btn-search"){doSearch();return;}
if(t.id === "btn-new-room"){createRoom();return;}
if(t.id === "new-room-pwd"){
var pv=$("#new-room-pwdval");if(pv)pv.style.display=(t.checked?"block":"none");
return;}
/* 好友相关: 同意/拒绝/删除/加好友/私聊 */
var act = t.getAttribute("data-act"), u = t.getAttribute("data-u");
if(act === "acc"){acceptFriend(u);return;}
if(act === "rej"){rejectFriend(u);return;}
if(act === "del"){if(confirm("删除好友 @"+u+"?"))delFriend(u);return;}
if(act === "add"){addFriend(u);return;}
if(act === "pm"){openPm(u, $(".un", t.closest(".user-item")||t).textContent.trim());return;}
/* 在线列表整行点击 -> 私聊 */
if(t.classList && t.classList.contains("user-item") && u && u !== state.user && !t.querySelector("[data-act]")){
openPm(u, $(".un", t).textContent.trim());return;}
});

document.addEventListener("keydown", function (e) {
if(e.key === "Enter"){
if(e.target && e.target.id === "input" && !e.shiftKey){e.preventDefault();send();}
else if(e.target && e.target.id === "pm-input"){e.preventDefault();sendPm();}
else if(e.target && e.target.id === "search-input"){doSearch();}
}
});

tryAutoLogin();
})();
