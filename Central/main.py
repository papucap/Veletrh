"""CENTRÁLA (server A) – katalog stánků, QR kódy, dotazník, hlasy, statistika, webhooky.

Spuštění:  uvicorn main:app --host 0.0.0.0 --port 8000
Dokumentace: /docs, /openapi.json
"""
import asyncio
import hashlib
import hmac
import logging
import os
import secrets
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from io import BytesIO
from typing import Literal

import httpx
import segno
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, HttpUrl

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("centrala")

PUBLIC_URL = os.getenv("PUBLIC_URL", "http://localhost:8000").rstrip("/")
DELIVERY_TIMEOUT = 3.0          # doručení je úspěšné jen při 2xx do 3 s
RETRY_DELAYS = (1, 2, 4)        # opakování po 1 s, 2 s, 4 s
MAX_FAILED_EVENTS = 5           # tolik nedoručených událostí v řadě => odběr se zruší


# ---------------------------------------------------------------- modely
class Stand(BaseModel):
    id: str = Field(examples=["S201"])
    title: str = Field(examples=["Rezervační systém pro školní učebny"])
    authors: list[str] = Field(examples=[["Marek Horák"]])
    class_name: str = Field(examples=["C4a"])
    floor: int = Field(examples=[2])
    room: str = Field(examples=["201"])


USERNAME_RE = r"^[A-Za-zÀ-ž0-9._-]{3,32}$"


class VoteIn(BaseModel):
    """Hlas žáka. Jméno se porovnává bez ohledu na velikost písmen a diakritiku."""
    username: str = Field(pattern=USERNAME_RE, examples=["novak.jan"],
                          description="3–32 znaků: písmena, číslice, tečka, pomlčka, podtržítko")
    rating: int = Field(ge=1, le=5, strict=True, examples=[5])
    favorite: bool = Field(strict=True, examples=[True])


class VoteOut(BaseModel):
    stand_id: str
    username: str
    rating: int
    favorite: bool
    created_at: str


class StandStats(BaseModel):
    stand_id: str
    title: str
    floor: int
    votes: int
    rating_sum: int
    avg_rating: float
    favorites: int


class Stats(BaseModel):
    generated_at: str = Field(description="UTC, ISO 8601")
    total_votes: int
    stands: list[StandStats]


class SubscriptionIn(BaseModel):
    callback_url: HttpUrl = Field(examples=["http://10.0.0.21:3000/webhooks/votes"],
                                  description="Kam má centrála události posílat")
    floor: int | None = Field(default=None, examples=[2],
                              description="Patro, které terminál zajímá; null = všechna patra")


class SubscriptionOut(BaseModel):
    id: str
    callback_url: str
    floor: int | None
    created_at: str


class SubscriptionCreated(SubscriptionOut):
    secret: str = Field(description="Podpisový klíč. Vrací se POUZE jednou, při vytvoření.")


class VoteEventData(BaseModel):
    stand_id: str
    floor: int
    rating: int
    favorite: bool


class VoteEvent(BaseModel):
    id: str = Field(description="Unikátní ID události (stejné i při opakovaném doručení)")
    type: Literal["vote.created"]
    created_at: str = Field(description="UTC, ISO 8601; okamžik uložení hlasu")
    data: VoteEventData


@dataclass
class Sub:
    id: str
    callback_url: str
    floor: int | None
    secret: str
    created_at: str
    failed_events: int = 0


# ---------------------------------------------------------------- data
STANDS = [
    Stand(id="S101", title="Chytré zalévání pokojových rostlin", authors=["Petra Svobodová"], class_name="C2a", floor=1, room="101"),
    Stand(id="S102", title="Hra Labyrint v Unity", authors=["Tomáš Dvořák", "Adam Král"], class_name="C3b", floor=1, room="102"),
    Stand(id="S103", title="Webová aplikace pro školní jídelnu", authors=["Eva Marešová"], class_name="C4b", floor=1, room="103"),
    Stand(id="S201", title="Rezervační systém pro školní učebny", authors=["Marek Horák"], class_name="C4a", floor=2, room="201"),
    Stand(id="S202", title="Monitoring domácí sítě v Grafaně", authors=["Lukáš Veselý"], class_name="C4a", floor=2, room="202"),
    Stand(id="S203", title="Textová multiplayerová hra", authors=["Jakub Novotný", "Dan Pokorný"], class_name="C3a", floor=2, room="203"),
    Stand(id="S301", title="Evidence techniky v databázi", authors=["Karolína Bílá"], class_name="C4c", floor=3, room="301"),
    Stand(id="S302", title="Robotické rameno na Arduinu", authors=["Ondřej Beneš", "Filip Urban"], class_name="C3c", floor=3, room="302"),
]
STAND_BY_ID = {s.id: s for s in STANDS}
AGG: dict[str, list[int]] = {s.id: [0, 0, 0] for s in STANDS}   # votes, rating_sum, favorites
VOTED: set[tuple[str, str]] = set()                              # (stand_id, normalizované jméno)
SUBS: dict[str, Sub] = {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize(username: str) -> str:
    """Bez ohledu na velká/malá písmena (a diakritiku): 'Novák.Jan' == 'novak.jan'."""
    decomposed = unicodedata.normalize("NFKD", username.casefold())
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def stand_stats(s: Stand) -> StandStats:
    votes, total, favs = AGG[s.id]
    return StandStats(stand_id=s.id, title=s.title, floor=s.floor, votes=votes, rating_sum=total,
                      avg_rating=round(total / votes, 2) if votes else 0.0, favorites=favs)


# ---------------------------------------------------------------- aplikace
app = FastAPI(
    title="Veletrh – centrála",
    version="1.0.0",
    description="Katalog stánků, příjem hlasů, statistika a rozesílání událostí terminálům.\n\n"
                "**Webhook `vote.created`** – popis toho, co centrála posílá terminálům, je v sekci "
                "*Webhooks* (a u `POST /api/v1/subscriptions`).",
)


@app.webhooks.post("vote.created")
def vote_created_webhook(event: VoteEvent):
    """Centrála pošle `POST` na `callback_url` odběru po každém uloženém hlasu.

    * Hlavička `X-Signature: sha256=<hex>` = HMAC-SHA256(`secret`, **surové bajty těla**).
    * Hlavička `X-Event-Id` = `id` události (při opakování stejné).
    * Úspěch = odpověď `2xx` do 3 s. Jinak se doručení opakuje po 1 s, 2 s a 4 s
      (se stejným tělem i podpisem). Příjemce proto musí být idempotentní podle `id`.
    """


# -------- stánky
@app.get("/api/v1/stands", response_model=list[Stand], tags=["Stánky"])
def list_stands(floor: int | None = Query(default=None, description="Filtr na patro")):
    return [s for s in STANDS if floor is None or s.floor == floor]


@app.get("/api/v1/stands/{stand_id}", response_model=Stand, tags=["Stánky"],
         responses={404: {"description": "Stánek neexistuje"}})
def get_stand(stand_id: str):
    if stand_id not in STAND_BY_ID:
        raise HTTPException(404, "Stánek neexistuje")
    return STAND_BY_ID[stand_id]


# -------- QR kódy
def qr_bytes(stand_id: str, kind: str) -> bytes:
    if stand_id not in STAND_BY_ID:
        raise HTTPException(404, "Stánek neexistuje")
    buf = BytesIO()
    segno.make(f"{PUBLIC_URL}/s/{stand_id}", error="m").save(buf, kind=kind, scale=10, border=2)
    return buf.getvalue()


@app.get("/qr", response_class=HTMLResponse, tags=["QR kódy"], summary="Všechny QR kódy k tisku")
def qr_page():
    cards = "".join(
        f'<figure><img src="/qr/{s.id}.svg" alt="QR {s.id}"><figcaption>'
        f"<b>{s.id}</b> · učebna {escape(s.room)}<br>{escape(s.title)}</figcaption></figure>"
        for s in STANDS
    )
    return (
        '<!doctype html><meta charset="utf-8"><title>QR kódy</title>'
        "<style>body{font-family:system-ui;display:flex;flex-wrap:wrap;gap:1rem}"
        "figure{width:260px;margin:0;text-align:center;break-inside:avoid}img{width:100%}</style>" + cards
    )


@app.get("/qr/{stand_id}.svg", tags=["QR kódy"])
def qr_svg(stand_id: str):
    return Response(qr_bytes(stand_id, "svg"), media_type="image/svg+xml")


@app.get("/qr/{stand_id}.png", tags=["QR kódy"])
def qr_png(stand_id: str):
    return Response(qr_bytes(stand_id, "png"), media_type="image/png")


# -------- dotazník (HTML pro mobil)
FORM_HTML = """<!doctype html><html lang="cs"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Hodnocení __ID__</title>
<style>body{font-family:system-ui;max-width:30rem;margin:auto;padding:1rem}label{display:block;margin:1rem 0 .3rem}
input[type=text]{width:100%;padding:.6rem;font-size:1rem;box-sizing:border-box}.r label{display:inline-block;margin-right:.8rem}
button{padding:.8rem 1.2rem;font-size:1rem;margin-top:1rem}#msg{margin-top:1rem;font-weight:600}.meta{color:#555}</style></head><body>
<h1>__TITLE__</h1><p class="meta">__AUTHORS__ · třída __CLASS__ · učebna __ROOM__</p>
<form id="f"><label for="u">Uživatelské jméno</label><input id="u" type="text" required minlength="3" maxlength="32" autocomplete="off" placeholder="novak.jan">
<label>Hodnocení</label><div class="r">
<label><input type="radio" name="r" value="1" required> 1</label><label><input type="radio" name="r" value="2"> 2</label>
<label><input type="radio" name="r" value="3"> 3</label><label><input type="radio" name="r" value="4"> 4</label>
<label><input type="radio" name="r" value="5"> 5</label></div>
<label><input type="checkbox" id="fav"> Můj favorit</label><button>Odeslat</button></form><div id="msg"></div>
<script>
const msg=document.getElementById('msg');
document.getElementById('f').onsubmit=async e=>{e.preventDefault();
 const body={username:document.getElementById('u').value.trim(),
  rating:Number(document.querySelector('input[name=r]:checked').value),favorite:document.getElementById('fav').checked};
 try{const r=await fetch('/api/v1/stands/__ID__/votes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(r.status===201){msg.textContent='Díky, hlas byl uložen.';document.getElementById('f').style.display='none';}
  else if(r.status===409){msg.textContent='Pro tento stánek jsi už hlasoval(a).';}
  else if(r.status===422){const d=await r.json();msg.textContent='Neplatné údaje: '+d.detail.map(x=>x.loc.slice(1).join('.')+': '+x.msg).join('; ');}
  else{msg.textContent='Chyba ('+r.status+').';}
 }catch(err){msg.textContent='Centrála není dostupná.';}};
</script></body></html>"""


@app.get("/s/{stand_id}", response_class=HTMLResponse, tags=["Hlasy"], summary="Dotazník (HTML)",
         responses={404: {"description": "Stánek neexistuje"}})
def questionnaire(stand_id: str):
    s = STAND_BY_ID.get(stand_id)
    if not s:
        raise HTTPException(404, "Stánek neexistuje")
    html = FORM_HTML
    for key, val in {"__ID__": s.id, "__TITLE__": escape(s.title), "__AUTHORS__": escape(", ".join(s.authors)),
                     "__CLASS__": escape(s.class_name), "__ROOM__": escape(s.room)}.items():
        html = html.replace(key, val)
    return html


# -------- hlasy
@app.post(
    "/api/v1/stands/{stand_id}/votes", status_code=201, response_model=VoteOut, tags=["Hlasy"],
    summary="Odeslat hlas",
    responses={
        201: {"description": "Hlas uložen"},
        404: {"description": "Stánek neexistuje"},
        409: {"description": "Tento žák už pro stánek hlasoval (jméno bez ohledu na velikost písmen)"},
        422: {"description": "Neplatné tělo – odpověď říká, co je špatně"},
    },
)
async def create_vote(stand_id: str, vote: VoteIn, background: BackgroundTasks):
    stand = STAND_BY_ID.get(stand_id)
    if not stand:
        raise HTTPException(404, "Stánek neexistuje")
    key = (stand_id, normalize(vote.username))
    if key in VOTED:
        raise HTTPException(409, "Pro tento stánek už hlasoval(a).")

    # Bez await => uložení a výpočet času jsou atomické vůči ostatním požadavkům.
    VOTED.add(key)
    agg = AGG[stand_id]
    agg[0] += 1
    agg[1] += vote.rating
    agg[2] += int(vote.favorite)
    event = VoteEvent(
        id=str(uuid.uuid4()), type="vote.created", created_at=utc_now(),
        data=VoteEventData(stand_id=stand_id, floor=stand.floor, rating=vote.rating, favorite=vote.favorite),
    )
    body = event.model_dump_json().encode()      # serializace JEDNOU; podepisují se tytéž bajty, které se odešlou
    for sub in list(SUBS.values()):
        if sub.floor is None or sub.floor == stand.floor:
            background.add_task(deliver, sub, body, event.id)   # běží až po odeslání odpovědi žákovi
    return VoteOut(stand_id=stand_id, username=vote.username, rating=vote.rating,
                   favorite=vote.favorite, created_at=event.created_at)


async def deliver(sub: Sub, body: bytes, event_id: str) -> None:
    signature = "sha256=" + hmac.new(sub.secret.encode(), body, hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json", "X-Event-Id": event_id, "X-Signature": signature}
    async with httpx.AsyncClient() as client:
        for attempt, delay in enumerate((0, *RETRY_DELAYS), start=1):
            if delay:
                await asyncio.sleep(delay)
            try:
                r = await asyncio.wait_for(client.post(sub.callback_url, content=body, headers=headers),
                                           DELIVERY_TIMEOUT)
                if r.is_success:
                    sub.failed_events = 0
                    return
                log.warning("Doručení %s -> %s: HTTP %s (pokus %d)", event_id, sub.callback_url, r.status_code, attempt)
            except (httpx.HTTPError, asyncio.TimeoutError) as exc:
                log.warning("Doručení %s -> %s selhalo: %r (pokus %d)", event_id, sub.callback_url, exc, attempt)
    log.error("NEDORUČENO: událost %s pro odběr %s (%s)", event_id, sub.id, sub.callback_url)
    sub.failed_events += 1
    if sub.failed_events >= MAX_FAILED_EVENTS and SUBS.pop(sub.id, None):
        log.error("Odběr %s zrušen: %d nedoručených událostí v řadě", sub.id, sub.failed_events)


# -------- statistika
@app.get("/api/v1/stats", response_model=Stats, tags=["Statistika"], summary="Statistika (volitelně pro patro)")
def get_stats(floor: int | None = Query(default=None, description="Filtr na patro")):
    items = [stand_stats(s) for s in STANDS if floor is None or s.floor == floor]
    return Stats(generated_at=utc_now(), total_votes=sum(i.votes for i in items), stands=items)


@app.get("/api/v1/stats/{stand_id}", response_model=StandStats, tags=["Statistika"],
         summary="Statistika jednoho stánku", responses={404: {"description": "Stánek neexistuje"}})
def get_stand_stats(stand_id: str):
    if stand_id not in STAND_BY_ID:
        raise HTTPException(404, "Stánek neexistuje")
    return stand_stats(STAND_BY_ID[stand_id])


# -------- odběry terminálů
@app.post("/api/v1/subscriptions", status_code=201, response_model=SubscriptionCreated, tags=["Odběry"],
          summary="Přihlásit terminál k odběru událostí",
          responses={422: {"description": "Neplatné tělo"}})
def create_subscription(sub_in: SubscriptionIn):
    """Terminál řekne, kam a pro které patro chce události. Vrací se `id` a `secret`
    (**jen teď**). Na `callback_url` pak chodí událost `vote.created` – tvar je v sekci *Webhooks*:
    tělo `VoteEvent`, hlavičky `X-Event-Id` a `X-Signature: sha256=<hex HMAC-SHA256 surového těla>`.
    """
    sub = Sub(id=str(uuid.uuid4()), callback_url=str(sub_in.callback_url), floor=sub_in.floor,
              secret=secrets.token_hex(32), created_at=utc_now())     # 32 náhodných bajtů
    SUBS[sub.id] = sub
    log.info("Nový odběr %s: %s (patro %s)", sub.id, sub.callback_url, sub.floor)
    return SubscriptionCreated(**{k: getattr(sub, k) for k in ("id", "callback_url", "floor", "created_at", "secret")})


@app.get("/api/v1/subscriptions", response_model=list[SubscriptionOut], tags=["Odběry"],
         summary="Seznam odběrů (bez secret)")
def list_subscriptions():
    return [SubscriptionOut(id=s.id, callback_url=s.callback_url, floor=s.floor, created_at=s.created_at)
            for s in SUBS.values()]


@app.delete("/api/v1/subscriptions/{sub_id}", status_code=204, tags=["Odběry"], summary="Zrušit odběr",
            responses={404: {"description": "Odběr neexistuje"}})
def delete_subscription(sub_id: str):
    if SUBS.pop(sub_id, None) is None:
        raise HTTPException(404, "Odběr neexistuje")
    log.info("Odběr %s zrušen", sub_id)
    return Response(status_code=204)
