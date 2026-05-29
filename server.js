// 関門海峡横断ゲーム（ZM連打クロール） サーバー
// Express + Socket.io によるオンライン最大8人対戦（ホストがスタート操作）
const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// 静的ファイル配信（public ディレクトリ / ルートでは kanmon.html を返す）
app.use(express.static(path.join(__dirname, 'public'), { index: 'kanmon.html' }));

// ゲーム定数
const MAX_PLAYERS = 8;
const MIN_PLAYERS_TO_START = 2; // ホストがスタートできる最小人数
const GOAL_DISTANCE = 1100; // m
const STROKE_DISTANCE = 2;  // 1ストロークで前進する距離(m)
const STAMINA_MAX = 100;
const STAMINA_DECAY_PER_SEC = 10;     // 連打が遅いと毎秒10%減
const STROKE_INTERVAL_LIMIT = 800;    // 0.8秒以上空くとスタミナ減少
const COUNTDOWN_SECONDS = 3;
const BROADCAST_INTERVAL = 100;       // 100ms毎にgame_state送信
// 最大8人分のレーン色
const PLAYER_COLORS = ['blue', 'red', 'yellow', 'green', 'purple', 'orange', 'cyan', 'pink'];

// ルーム管理: { [roomId]: roomObj }
const rooms = {};
// グローバルランキング（上位10件をメモリ保持）
const ranking = [];

// 空きルームを探す or 新規作成
function findOrCreateRoom() {
  for (const id in rooms) {
    const r = rooms[id];
    if (!r.started && Object.keys(r.players).length < MAX_PLAYERS) {
      return r;
    }
  }
  const id = 'room_' + Math.random().toString(36).slice(2, 10);
  const room = {
    id,
    players: {},       // socketId -> player
    hostId: null,      // ホストのsocketId（最初に参加したプレイヤー、退出時は次の参加者に自動移譲）
    started: false,
    finished: false,
    countdownStarted: false,
    startTime: 0,
    loopTimer: null,
  };
  rooms[id] = room;
  return room;
}

// 現在のプレイヤー情報を整形
function getPlayersInfo(room) {
  return Object.values(room.players).map(p => ({
    id: p.id,
    name: p.name,
    color: p.color,
    distance: p.distance,
    stamina: p.stamina,
    sunk: p.sunk,
    finished: p.finished,
    time: p.time,
    rank: p.rank,
  }));
}

// ゲームループ（サーバー権威）
function startGameLoop(room) {
  room.started = true;
  room.startTime = Date.now();
  // 全プレイヤーに開始合図
  io.to(room.id).emit('game_start', { startTime: room.startTime });

  room.loopTimer = setInterval(() => {
    const now = Date.now();
    for (const p of Object.values(room.players)) {
      if (p.sunk || p.finished) continue;
      // スタミナ減少判定（最後のストロークから0.8秒以上経過していたら減少）
      const elapsed = now - p.lastStrokeAt;
      if (elapsed > STROKE_INTERVAL_LIMIT) {
        const overSec = (elapsed - STROKE_INTERVAL_LIMIT) / 1000;
        // 直近tick分のみ減らす（100ms分）
        const decay = STAMINA_DECAY_PER_SEC * (BROADCAST_INTERVAL / 1000);
        p.stamina = Math.max(0, p.stamina - decay);
        if (p.stamina <= 0 && !p.sunk) {
          p.sunk = true;
          p.stamina = 0;
        }
      }
    }
    io.to(room.id).emit('game_state', { players: getPlayersInfo(room) });

    // 終了判定: 全員ゴール or 沈没
    const all = Object.values(room.players);
    const done = all.every(p => p.finished || p.sunk);
    if (done && all.length > 0) {
      endGame(room);
    }
  }, BROADCAST_INTERVAL);
}

// ゲーム終了処理
function endGame(room) {
  if (room.finished) return;
  room.finished = true;
  if (room.loopTimer) {
    clearInterval(room.loopTimer);
    room.loopTimer = null;
  }
  // ランキング順位付け（ゴール者をタイム昇順、沈没者は最下位）
  const finishers = Object.values(room.players).filter(p => p.finished)
    .sort((a, b) => a.time - b.time);
  const sunkers = Object.values(room.players).filter(p => !p.finished);
  const ordered = [...finishers, ...sunkers];
  ordered.forEach((p, i) => { p.rank = i + 1; });

  // グローバルランキングへ反映（ゴール者のみ）
  for (const p of finishers) {
    ranking.push({ name: p.name, time: p.time, at: Date.now() });
  }
  ranking.sort((a, b) => a.time - b.time);
  if (ranking.length > 10) ranking.length = 10;

  io.to(room.id).emit('game_result', {
    players: ordered.map(p => ({
      name: p.name, color: p.color, distance: p.distance,
      time: p.time, sunk: p.sunk, finished: p.finished, rank: p.rank,
    })),
    ranking: ranking.slice(0, 10),
  });

  // ルームは少し残してから破棄（再戦希望者の通信用）
  setTimeout(() => { delete rooms[room.id]; }, 60 * 1000);
}

// カウントダウン開始
function startCountdown(room) {
  if (room.countdownStarted) return;
  room.countdownStarted = true;
  let n = COUNTDOWN_SECONDS;
  io.to(room.id).emit('countdown', { count: n });
  const t = setInterval(() => {
    n--;
    if (n > 0) {
      io.to(room.id).emit('countdown', { count: n });
    } else {
      clearInterval(t);
      io.to(room.id).emit('countdown', { count: 0 });
      startGameLoop(room);
    }
  }, 1000);
}

io.on('connection', (socket) => {
  let currentRoom = null;
  let currentPlayer = null;

  // ルーム参加
  socket.on('join_room', ({ name }) => {
    if (currentRoom) return;
    const safeName = (typeof name === 'string' && name.trim()) ? name.trim().slice(0, 16) : '名無し';
    const room = findOrCreateRoom();
    const idx = Object.keys(room.players).length;
    const player = {
      id: socket.id,
      name: safeName,
      color: PLAYER_COLORS[idx],
      distance: 0,
      stamina: STAMINA_MAX,
      sunk: false,
      finished: false,
      time: 0,
      rank: 0,
      lastKey: null,         // 直前に押したキー（'Z' or 'M'）
      lastStrokeAt: Date.now(),
    };
    room.players[socket.id] = player;
    socket.join(room.id);
    currentRoom = room;
    currentPlayer = player;

    // 最初の参加者をホストに設定
    if (!room.hostId) {
      room.hostId = socket.id;
    }

    io.to(room.id).emit('room_update', {
      roomId: room.id,
      count: Object.keys(room.players).length,
      max: MAX_PLAYERS,
      minToStart: MIN_PLAYERS_TO_START,
      hostId: room.hostId,
      players: getPlayersInfo(room),
    });
  });

  // ホストによるゲーム開始要求
  socket.on('start_game', () => {
    if (!currentRoom) return;
    const room = currentRoom;
    // ホストのみ開始可能
    if (room.hostId !== socket.id) return;
    // すでに開始済み・カウントダウン中は無視
    if (room.started || room.countdownStarted) return;
    // 最小人数チェック
    if (Object.keys(room.players).length < MIN_PLAYERS_TO_START) return;
    startCountdown(room);
  });

  // ストローク入力（Z/Mキー）
  socket.on('stroke', ({ key }) => {
    if (!currentRoom || !currentPlayer) return;
    if (!currentRoom.started || currentRoom.finished) return;
    const p = currentPlayer;
    if (p.sunk || p.finished) return;
    const k = (key === 'Z' || key === 'M') ? key : null;
    if (!k) return;
    // 交互入力検証
    if (p.lastKey === k) {
      // 同じキー連続は無効
      return;
    }
    p.lastKey = k;
    p.lastStrokeAt = Date.now();
    p.distance += STROKE_DISTANCE;
    // ストローク成功時はスタミナを少し回復（連打維持で全快を保てる程度）
    p.stamina = Math.min(STAMINA_MAX, p.stamina + 1);
    if (p.distance >= GOAL_DISTANCE && !p.finished) {
      p.distance = GOAL_DISTANCE;
      p.finished = true;
      p.time = (Date.now() - currentRoom.startTime) / 1000;
      socket.emit('finish', { time: p.time });
    }
  });

  // クライアントからのゴール通知（保険: サーバー側でも判定済）
  socket.on('finish', () => {
    if (!currentRoom || !currentPlayer) return;
    const p = currentPlayer;
    if (!p.finished && p.distance >= GOAL_DISTANCE) {
      p.finished = true;
      p.time = (Date.now() - currentRoom.startTime) / 1000;
    }
  });

  // 切断時
  socket.on('disconnect', () => {
    if (!currentRoom) return;
    const room = currentRoom;
    if (room.players[socket.id]) {
      // 進行中なら沈没扱い（プレイヤー情報は残す）
      if (room.started && !room.finished) {
        room.players[socket.id].sunk = true;
      } else {
        delete room.players[socket.id];
      }
      // ホストが退出した場合、次の参加者にホストを自動移譲
      if (room.hostId === socket.id) {
        const nextIds = Object.keys(room.players).filter(id => id !== socket.id);
        room.hostId = nextIds.length > 0 ? nextIds[0] : null;
      }
      io.to(room.id).emit('room_update', {
        roomId: room.id,
        count: Object.keys(room.players).length,
        max: MAX_PLAYERS,
        minToStart: MIN_PLAYERS_TO_START,
        hostId: room.hostId,
        players: getPlayersInfo(room),
      });
      // 全員いなくなればルーム削除
      if (Object.keys(room.players).length === 0) {
        if (room.loopTimer) clearInterval(room.loopTimer);
        delete rooms[room.id];
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`関門海峡横断ゲーム サーバー起動: ポート ${PORT}`);
});
