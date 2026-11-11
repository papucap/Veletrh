"""TERMINÁL (server B) – obrazovka pro jedno patro.

Spuštění:  uvicorn main:app --host 0.0.0.0 --port 3000
Konfigurace (.env): CENTRAL_URL, FLOOR, MY_URL
"""
import asyncio
import hashlib
import hmac
import logging
import os
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Literal

import httpx
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ValidationError

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("terminal")

CENTRAL_URL = os.getenv("CENTRAL_URL", "http://localhost:8000").rstrip("/")
FLOOR = int(os.getenv("FLOOR", "2"))
MY_URL = os.getenv("MY_URL", "http://localhost:3000").rstrip("/")
MAX_SEEN = 10_000


class State:
    stands: dict[str, dict] = {}                 # stand_id -> {title, votes, rating_sum, favorites}
    seen: OrderedDict[str, None] = OrderedDict() # id už zpracovaných událostí (omezená paměť)
    snapshot_at: str = ""                        # generated_at snapshotu
    sub_id: str | None = None
    secret: str | None = None


S = State()


async def retry(fn, what: str):
    while True:
        try:
            return await fn()
        except httpx.HTTPError as exc:
            log.warning("%s selhalo (%r), zkouším znovu za 2 s", what, exc)
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient(base_url=CENTRAL_URL, timeout=5) as client:
        # 1) NEJDŘÍV odběr, 2) POTOM snapshot – jinak by se hlas mezi tím ztratil.
        async def register():
            r = await client.post("/api/v1/subscriptions",
                                  json={"callback_url": f"{MY_URL}/webhooks/votes", "floor": FLOOR})
            r.raise_for_status()
            return r.json()

        sub = await retry(register, "Registrace odběru")
        S.sub_id, S.secret = sub["id"], sub["secret"]
        log.info("Přihlášen k odběru %s (patro %s)", S.sub_id, FLOOR)

        async def snapshot():
            r = await client.get("/api/v1/stats", params={"floor": FLOOR})
            r.raise_for_status()
            return r.json()

        snap = await retry(snapshot, "Snapshot")
        S.stands = {s["stand_id"]: {"title": s["title"], "votes": s["votes"],
                                    "rating_sum": s["rating_sum"], "favorites": s["favorites"]}
                    for s in snap["stands"]}
        S.snapshot_at = snap["generated_at"]
        log.info("Snapshot načten: %d stánků", len(S.stands))

        yield   # --- aplikace běží ---

        try:
            await client.delete(f"/api/v1/subscriptions/{S.sub_id}")
            log.info("Odběr zrušen")
        except httpx.HTTPError as exc:
            log.warning("Odběr se nepodařilo zrušit: %r", exc)


app = FastAPI(title=f"Veletrh – terminál (patro {FLOOR})", lifespan=lifespan)


# ---------------------------------------------------------------- webhook
class EventData(BaseModel):
    stand_id: str
    floor: int
    rating: int
    favorite: bool


class VoteEvent(BaseModel):
    id: str
    type: Literal["vote.created"]
    created_at: str
    data: EventData


async def verify_signature(request: Request) -> bytes:
    """Middleware-like závislost: podpis se ověřuje nad SUROVÝMI bajty, JSON se parsuje až potom."""
    raw = await request.body()
    if not S.secret:
        raise HTTPException(503, "Terminál ještě není přihlášen")
    expected = "sha256=" + hmac.new(S.secret.encode(), raw, hashlib.sha256).hexdigest()
    header = request.headers.get("X-Signature", "")
    if not hmac.compare_digest(header.encode(), expected.encode()):
        raise HTTPException(401, "Chybějící nebo neplatný podpis")
    return raw


@app.post("/webhooks/votes", summary="Příjem události vote.created od centrály")
async def webhook_votes(raw: bytes = Depends(verify_signature)):
    try:
        event = VoteEvent.model_validate_json(raw)
    except ValidationError as exc:
        raise HTTPException(422, exc.errors(include_url=False, include_context=False))

    if event.id in S.seen:                       # idempotence: duplicita => 200, beze změny
        return {"status": "duplicate"}
    S.seen[event.id] = None
    if len(S.seen) > MAX_SEEN:
        S.seen.popitem(last=False)

    if event.data.floor != FLOOR:
        return {"status": "ignored"}
    if event.created_at <= S.snapshot_at:        # hlas už je v snapshotu (přišel opakovaně po startu)
        return {"status": "already_in_snapshot"}
    stand = S.stands.get(event.data.stand_id)
    if stand is None:
        return {"status": "ignored"}
    stand["votes"] += 1
    stand["rating_sum"] += event.data.rating
    stand["favorites"] += int(event.data.favorite)
    return {"status": "counted"}


# ---------------------------------------------------------------- zobrazení
@app.get("/stats", summary="Co terminál právě zobrazuje (seřazeno podle průměru)")
def stats():
    rows = []
    for sid, s in S.stands.items():
        avg = round(s["rating_sum"] / s["votes"], 2) if s["votes"] else 0.0
        rows.append({"stand_id": sid, "title": s["title"], "votes": s["votes"],
                     "avg_rating": avg, "favorites": s["favorites"]})
    rows.sort(key=lambda r: (-r["avg_rating"], -r["votes"], r["stand_id"]))
    return {"floor": FLOOR, "total_votes": sum(r["votes"] for r in rows), "stands": rows}


PAGE = """<!doctype html><html lang="cs"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Patro __FLOOR__</title><style>body{font:22px system-ui;margin:2rem;background:#111;color:#eee}
h1 small{font-size:.5em;color:#999}table{width:100%;border-collapse:collapse}th,td{padding:.6rem;text-align:left;border-bottom:1px solid #333}
td:nth-child(n+3),th:nth-child(n+3){text-align:right}</style></head><body>
<h1>Veletrh – patro __FLOOR__ <small id="tot"></small></h1>
<table><thead><tr><th>#</th><th>Stánek</th><th>Hlasů</th><th>Průměr</th><th>Favorit</th></tr></thead><tbody id="rows"></tbody></table>
<script>
async function tick(){try{const d=await (await fetch('/stats')).json();
 document.getElementById('tot').textContent='· celkem hlasů: '+d.total_votes;
 const tb=document.getElementById('rows');tb.replaceChildren();
 d.stands.forEach((s,i)=>{const tr=document.createElement('tr');
  [i+1,s.stand_id+' – '+s.title,s.votes,s.avg_rating.toFixed(2),s.favorites].forEach(v=>{const td=document.createElement('td');td.textContent=v;tr.appendChild(td);});
  tb.appendChild(tr);});}catch(e){}}
tick();setInterval(tick,2000);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index():
    return PAGE.replace("__FLOOR__", str(FLOOR))
