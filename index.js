// Robô ouvinte 24h v2: escuta as 5 mesas (2 conexões por mesa) e repassa cada resultado ao painel.
const TABLES = {
  leqhceumaq6qfoug: "Speed Baccarat A",
  lv2kzclunt2qnxo5: "Speed Baccarat B",
  ndgvwvgthfuaad3q: "Speed Baccarat C",
  ndgvz5mlhfuaad6e: "Speed Baccarat D",
  ndgv45bghfuaaebf: "Speed Baccarat E",
};
const SSE = "https://minigame.evolgame.net/minigame/play?gameId=";
const PANEL = process.env.PANEL_URL || "https://nomad-api.lovable.app/api/public/listener";
const SECRET = process.env.LISTENER_SECRET;
if (!SECRET) { console.error("Falta LISTENER_SECRET"); process.exit(1); }

// Eventos que provam que a mesa está realmente transmitindo o jogo.
const GAME_EVENTS = new Set(["bets-open", "bets-closed", "card-reveal", "bets-resolved", "betting-stats", "new-deck"]);
const STALL_MS = 70000;     // sem evento de jogo por 70s -> reconecta
const STREAMS_PER_TABLE = 2;

const side = (v) => {
  const s = String(v ?? "").toLowerCase();
  if (s.startsWith("p")) return "player";
  if (s.startsWith("b")) return "banker";
  if (s.startsWith("t") || s === "draw") return "tie";
  return null;
};
const num = (v) => (Number.isFinite(Number(v)) && v !== null && v !== "" ? Number(v) : null);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function extract(p) {
  if (!p || p.event !== "bets-resolved") return null;
  const data = p.data && typeof p.data === "object" ? p.data : {};
  const r = data.result && typeof data.result === "object" ? data.result : data;
  const winner = side(r.winner);
  const round = num(p.round_number) ?? num(data.roundNumber);
  if (!winner || round === null) return null;
  const roundId = typeof p.round_id === "string" ? p.round_id : `${p.deck_id ?? "d"}-${round}`;
  return { roundId, round, winner };
}

// Deduplicação: as 2 conexões da mesma mesa recebem a mesma rodada; envia só uma vez.
const seen = new Map(); // gameId -> Map(roundId -> timestamp)
const lastGameEvent = {}; // gameId -> timestamp do último evento de jogo (qualquer conexão)
// A provedora REUTILIZA o mesmo round_id em outro sapato ~30 min depois: só é repetido se for recente.
const REUSE_MS = 10 * 60 * 1000;
function firstTime(gameId, roundId) {
  let s = seen.get(gameId);
  if (!s) seen.set(gameId, (s = new Map()));
  const now = Date.now();
  for (const [k, t] of s) if (now - t > REUSE_MS) s.delete(k);
  if (s.has(roundId)) return false;
  s.set(roundId, now);
  return true;
}

async function post(body) {
  const res = await fetch(PANEL, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-listener-secret": SECRET },
    body: JSON.stringify(body),
  });
  return { status: res.status, ok: res.ok, txt: await res.text() };
}

async function send(gameId, live) {
  const name = TABLES[gameId];
  // Reenvio insistente por ~30s (o painel ignora rodada repetida, então é seguro).
  for (let i = 1; i <= 8; i++) {
    try {
      const r = await post({ gameId, ...live });
      if (r.ok) { console.log(`[${name}] #${live.round} ${live.winner} -> ${r.txt}`); return; }
      console.warn(`[${name}] #${live.round} painel ${r.status}: ${r.txt}`);
      if (r.status === 401 || r.status === 400) return;
    } catch (e) { console.warn(`[${name}] #${live.round} envio falhou (${i}/8): ${e.message}`); }
    await sleep(Math.min(8000, 500 * 2 ** (i - 1)));
  }
  console.error(`[${name}] #${live.round} NÃO ENTREGUE após 8 tentativas`);
}

async function listen(gameId, n) {
  const tag = `${TABLES[gameId]}/${n}`;
  let attempts = 0;
  for (;;) {
    const ctrl = new AbortController();
    let lastEvent = Date.now();
    const watchdog = setInterval(() => {
      if (Date.now() - lastEvent > STALL_MS) { console.warn(`[${tag}] sem eventos de jogo há 40s`); ctrl.abort(); }
    }, 5000);
    try {
      const res = await fetch(`${SSE}${gameId}&_=${Date.now()}-${n}`, {
        headers: { Accept: "text/event-stream", "Cache-Control": "no-cache" }, signal: ctrl.signal,
      });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      console.log(`[${tag}] conectado`);
      const dec = new TextDecoder();
      let buf = "";
      for await (const chunk of res.body) {
        buf += dec.decode(chunk, { stream: true }).replace(/\r\n/g, "\n");
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const block = buf.slice(0, i); buf = buf.slice(i + 2);
          const dataStr = block.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).replace(/^ /, "")).join("\n");
          if (!dataStr) continue;
          let p;
          try { p = JSON.parse(dataStr); } catch { continue; }
          if (p && GAME_EVENTS.has(p.event)) { lastEvent = Date.now(); lastGameEvent[gameId] = lastEvent; attempts = 0; }
          const live = extract(p);
          if (live && firstTime(gameId, live.roundId)) send(gameId, live);
        }
      }
      throw new Error("conexão encerrada");
    } catch (e) {
      console.warn(`[${tag}] reconectando: ${e.message}`);
    } finally { clearInterval(watchdog); }
    await sleep(Math.min(10000, 1000 * 2 ** attempts++));
  }
}

// Sinal de vida a cada 30s: só mesas com evento de jogo recente.
setInterval(async () => {
  const alive = Object.keys(TABLES).filter((id) => Date.now() - (lastGameEvent[id] ?? 0) < 60000);
  if (!alive.length) return;
  try { await post({ heartbeat: alive }); } catch (e) { console.warn("heartbeat falhou:", e.message); }
}, 30000);

console.log("Robô ouvinte v3 iniciado:", Object.values(TABLES).join(", "));
for (const id of Object.keys(TABLES)) for (let n = 1; n <= STREAMS_PER_TABLE; n++) setTimeout(() => listen(id, n), (n - 1) * 3000);
