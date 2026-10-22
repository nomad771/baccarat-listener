// Robô ouvinte 24h: escuta as 5 mesas e repassa cada resultado ao painel.
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

const side = (v) => {
  const s = String(v ?? "").toLowerCase();
  if (s.startsWith("p")) return "player";
  if (s.startsWith("b")) return "banker";
  if (s.startsWith("t") || s === "draw") return "tie";
  return null;
};
const num = (v) => (Number.isFinite(Number(v)) && v !== null && v !== "" ? Number(v) : null);

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

async function send(gameId, live) {
  for (let i = 1; i <= 5; i++) {
    try {
      const res = await fetch(PANEL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-listener-secret": SECRET },
        body: JSON.stringify({ gameId, ...live }),
      });
      const txt = await res.text();
      if (res.ok) { console.log(`[${TABLES[gameId]}] #${live.round} ${live.winner} -> ${txt}`); return; }
      console.warn(`[${TABLES[gameId]}] painel ${res.status}: ${txt}`);
      if (res.status === 401 || res.status === 400) return;
    } catch (e) { console.warn(`[${TABLES[gameId]}] envio falhou: ${e.message}`); }
    await new Promise((r) => setTimeout(r, 500 * i));
  }
}

async function listen(gameId) {
  let attempts = 0;
  for (;;) {
    const ctrl = new AbortController();
    let last = Date.now();
    const watchdog = setInterval(() => { if (Date.now() - last > 30000) ctrl.abort(); }, 5000);
    try {
      const res = await fetch(SSE + gameId, { headers: { Accept: "text/event-stream" }, signal: ctrl.signal });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      console.log(`[${TABLES[gameId]}] conectado`);
      attempts = 0;
      const dec = new TextDecoder();
      let buf = "";
      for await (const chunk of res.body) {
        last = Date.now();
        buf += dec.decode(chunk, { stream: true }).replace(/\r\n/g, "\n");
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const block = buf.slice(0, i); buf = buf.slice(i + 2);
          const dataStr = block.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).replace(/^ /, "")).join("\n");
          if (!dataStr) continue;
          try { const live = extract(JSON.parse(dataStr)); if (live) send(gameId, live); } catch {}
        }
      }
      throw new Error("conexão encerrada");
    } catch (e) {
      console.warn(`[${TABLES[gameId]}] reconectando: ${e.message}`);
    } finally { clearInterval(watchdog); }
    await new Promise((r) => setTimeout(r, Math.min(15000, 1000 * 2 ** attempts++)));
  }
}

console.log("Robô ouvinte iniciado:", Object.values(TABLES).join(", "));
for (const id of Object.keys(TABLES)) listen(id);
