// Robô ouvinte 24h v6: escuta as 5 mesas (2 conexões por mesa).
// Novidades v6 (auditoria de 23/10):
//  1. A provedora às vezes NÃO manda "bets-resolved" de uma rodada jogada. O vencedor vem
//     também na última carta ("card-reveal" com winner) -> usamos o que chegar primeiro.
//  2. A provedora às vezes PULA o número da rodada (ex.: 9 -> 11 sem rodada 10 jogada).
//     Enviamos "prevRound": a rodada vista logo antes na MESMA conexão, sem queda e sem
//     nenhuma rodada intermediária -> o painel sabe que nada foi perdido.
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
if (!SECRET) {
  console.error("Falta LISTENER_SECRET");
  process.exit(1);
}

const GAME_EVENTS = new Set(["bets-open", "bets-closed", "card-reveal", "bets-resolved", "betting-stats", "new-deck"]);

// Watchdog: fluxo saudável manda betting-stats várias vezes por segundo; 20s em branco = conexão
// morta. Na troca de sapato a paciência dobra (20s -> 120s) e volta a 20s quando chega rodada.
const STALL_MIN = 20000;
const STALL_MAX = 120000;
const STREAMS_PER_TABLE = 2;
// Espera curta para juntar a prova de continuidade das 2 conexões antes de enviar.
const MERGE_MS = 800;

const patience = new Map();
const getPatience = (id) => patience.get(id) ?? STALL_MIN;

const side = (v) => {
  const s = String(v ?? "").toLowerCase();
  if (s.startsWith("p")) return "player";
  if (s.startsWith("b")) return "banker";
  if (s.startsWith("t") || s === "draw") return "tie";
  return null;
};
const num = (v) => (v !== null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const roundOf = (p) => num(p.round_number) ?? num(p.data && p.data.roundNumber);
const deckOf = (p) => (typeof p.deck_id === "string" ? p.deck_id : null);

/** Resultado de bets-resolved OU da última carta (card-reveal com winner). */
function extract(p) {
  if (!p) return null;
  const data = p.data && typeof p.data === "object" ? p.data : {};
  let raw = null;
  if (p.event === "bets-resolved") {
    const r = data.result && typeof data.result === "object" ? data.result : data;
    raw = r.winner;
  } else if (p.event === "card-reveal") {
    raw = p.winner ?? data.winner; // só existe na carta final da mão
  } else return null;
  const winner = side(raw);
  const round = roundOf(p);
  if (!winner || round === null) return null;
  const deck = deckOf(p);
  const roundId = typeof p.round_id === "string" ? p.round_id : `${deck ?? "d"}-${round}`;
  return { roundId, round, winner, deck, via: p.event };
}

// Envio único por rodada, juntando a prova de continuidade das 2 conexões.
const pending = new Map(); // gameId|roundId -> {live, prev}
const sent = new Map(); // gameId -> Map(roundId -> ts)
const REUSE_MS = 90 * 1000;
function alreadySent(gameId, roundId) {
  let s = sent.get(gameId);
  if (!s) sent.set(gameId, (s = new Map()));
  const now = Date.now();
  for (const [k, t] of s) if (now - t > REUSE_MS) s.delete(k);
  return s.has(roundId);
}
function offer(gameId, live, prev) {
  if (alreadySent(gameId, live.roundId)) return;
  const key = `${gameId}|${live.roundId}`;
  const cur = pending.get(key);
  if (cur) {
    if (cur.prev === null && prev !== null) cur.prev = prev;
    return;
  }
  pending.set(key, { live, prev });
  setTimeout(() => {
    const item = pending.get(key);
    pending.delete(key);
    if (!item || alreadySent(gameId, live.roundId)) return;
    sent.get(gameId).set(live.roundId, Date.now());
    send(gameId, item.live, item.prev);
  }, MERGE_MS);
}

// Auditoria: rodadas recebidas e perdas REAIS (número pulado sem prova de continuidade).
const stats = new Map();
const lastRound = new Map();
const statOf = (id) => {
  let s = stats.get(id);
  if (!s) stats.set(id, (s = { rounds: 0, gaps: 0, skips: 0 }));
  return s;
};
function noteRound(gameId, live, prev) {
  const st = statOf(gameId);
  st.rounds++;
  patience.set(gameId, STALL_MIN);
  const last = lastRound.get(gameId);
  if (last && last.deck === live.deck && live.round > last.n + 1) {
    if (prev === last.n) {
      st.skips++;
      console.log(`[${TABLES[gameId]}] provedora pulou o número ${last.n + 1}-${live.round - 1} (nenhuma rodada jogada) -> sequência mantida`);
    } else {
      st.gaps += live.round - last.n - 1;
      console.warn(`[${TABLES[gameId]}] BURACO: rodadas ${last.n + 1}-${live.round - 1} não recebidas (vi ${last.n} -> ${live.round})`);
    }
  }
  lastRound.set(gameId, { deck: live.deck, n: live.round });
}

async function post(body) {
  const res = await fetch(PANEL, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-listener-secret": SECRET },
    body: JSON.stringify(body),
  });
  return { status: res.status, ok: res.ok, txt: await res.text() };
}

async function send(gameId, live, prev) {
  const name = TABLES[gameId];
  noteRound(gameId, live, prev);
  for (let i = 1; i <= 8; i++) {
    try {
      const r = await post({ gameId, roundId: live.roundId, round: live.round, winner: live.winner, prevRound: prev });
      if (r.ok) {
        console.log(`[${name}] #${live.round} ${live.winner} (${live.via}, ant=${prev ?? "-"}) -> ${r.txt}`);
        return;
      }
      console.warn(`[${name}] #${live.round} painel ${r.status}: ${r.txt}`);
      if (r.status === 401 || r.status === 400) return;
    } catch (e) {
      console.warn(`[${name}] #${live.round} envio falhou (${i}/8): ${e.message}`);
    }
    await sleep(Math.min(8000, 500 * 2 ** (i - 1)));
  }
  console.error(`[${name}] #${live.round} NÃO ENTREGUE após 8 tentativas`);
}

const lastGameEvent = {};

async function listen(gameId, n) {
  const tag = `${TABLES[gameId]}/${n}`;
  let attempts = 0;
  for (;;) {
    const ctrl = new AbortController();
    let lastEvent = Date.now();
    let gotRound = false;
    const limit = getPatience(gameId);
    // Continuidade desta conexão (zera a cada reconexão).
    let connPrev = null; // {deck, n} última rodada com resultado nesta conexão
    const seenSince = new Set(); // números de rodada vistos depois de connPrev
    const doneHere = new Set(); // rodadas já com resultado nesta conexão
    const watchdog = setInterval(() => {
      const quiet = Date.now() - lastEvent;
      if (quiet > getPatience(gameId)) {
        console.warn(`[${tag}] fluxo em branco há ${Math.round(quiet / 1000)}s (limite ${Math.round(getPatience(gameId) / 1000)}s) -> reconecta`);
        ctrl.abort();
      }
    }, 3000);
    try {
      const res = await fetch(`${SSE}${gameId}&_=${Date.now()}-${n}`, {
        headers: { Accept: "text/event-stream", "Cache-Control": "no-cache" },
        signal: ctrl.signal,
      });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      console.log(`[${tag}] conectado`);
      const dec = new TextDecoder();
      let buf = "";
      for await (const chunk of res.body) {
        buf += dec.decode(chunk, { stream: true }).replace(/\r\n/g, "\n");
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const block = buf.slice(0, i);
          buf = buf.slice(i + 2);
          const dataStr = block
            .split("\n")
            .filter((l) => l.startsWith("data:"))
            .map((l) => l.slice(5).replace(/^ /, ""))
            .join("\n");
          if (!dataStr) continue;
          let p;
          try {
            p = JSON.parse(dataStr);
          } catch {
            continue;
          }
          if (!p || !GAME_EVENTS.has(p.event)) continue;
          lastEvent = Date.now();
          lastGameEvent[gameId] = lastEvent;
          attempts = 0;

          const r = roundOf(p);
          const d = deckOf(p);
          if (p.event === "new-deck") { connPrev = null; seenSince.clear(); continue; }
          if (r !== null && connPrev && d === connPrev.deck && r > connPrev.n) seenSince.add(r);

          const live = extract(p);
          if (!live || doneHere.has(live.roundId)) continue;
          doneHere.add(live.roundId);
          if (doneHere.size > 200) doneHere.clear();
          gotRound = true;

          // Prova: mesma conexão, mesmo sapato, nenhuma rodada entre connPrev e esta.
          let prev = null;
          if (connPrev && connPrev.deck === live.deck && live.round > connPrev.n) {
            const between = [...seenSince].some((x) => x > connPrev.n && x < live.round);
            if (!between) prev = connPrev.n;
          }
          connPrev = { deck: live.deck, n: live.round };
          seenSince.clear();
          offer(gameId, live, prev);
        }
      }
      throw new Error("conexão encerrada");
    } catch (e) {
      console.warn(`[${tag}] reconectando: ${e.message}`);
      if (!gotRound) patience.set(gameId, Math.min(STALL_MAX, limit * 2));
    } finally {
      clearInterval(watchdog);
    }
    await sleep(Math.min(10000, 1000 * 2 ** attempts++));
  }
}

setInterval(async () => {
  const alive = Object.keys(TABLES).filter((id) => Date.now() - (lastGameEvent[id] ?? 0) < 60000);
  if (!alive.length) return;
  try {
    await post({ heartbeat: alive });
  } catch (e) {
    console.warn("heartbeat falhou:", e.message);
  }
}, 30000);

setInterval(() => {
  const line = Object.keys(TABLES)
    .map((id) => { const s = statOf(id); return `${TABLES[id].replace("Speed Baccarat ", "")}: ${s.rounds}r/${s.skips}pulo/${s.gaps}perda`; })
    .join(" | ");
  console.log(`[resumo] ${line}`);
}, 60000);

console.log(`Robô ouvinte v6 iniciado: ${Object.values(TABLES).join(", ")}`);
for (const id of Object.keys(TABLES))
  for (let k = 1; k <= STREAMS_PER_TABLE; k++) setTimeout(() => listen(id, k), (k - 1) * 3000);
