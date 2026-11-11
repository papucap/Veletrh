# Veletrh – centrála + terminál

Vše je ve FastAPI (OpenAPI se generuje z kódu → `/docs`, `/openapi.json`).

## Spuštění
```bash
# centrála (server A)
cd central && python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # PUBLIC_URL = IP, kterou vidí telefon
uvicorn main:app --host 0.0.0.0 --port 8000

# terminál (server B) – v dalším terminálu; pro další terminál jiný port + FLOOR + MY_URL
cd terminal && pip install -r requirements.txt
cp .env.example .env        # CENTRAL_URL, FLOOR, MY_URL
uvicorn main:app --host 0.0.0.0 --port 3000
```
Pozor na povolené porty ve škole (spsejecna.cz/pc). Terminál se při startu přihlašuje k centrále
a dokud neuspěje, zkouší to každé 2 s.

## Kde co je
| Krok | Kde |
|---|---|
| Katalog, filtr na patro, `/docs` | `central/main.py` – `/api/v1/stands?floor=` |
| QR kódy | `/qr/{id}.svg`, `/qr/{id}.png`, `/qr` (stránka k tisku) |
| Dotazník + hlas | `/s/{id}` (HTML), `POST /api/v1/stands/{id}/votes` (201/404/409/422) |
| Statistika | `/api/v1/stats?floor=`, `/api/v1/stats/{id}` |
| Odběry | `POST/GET /api/v1/subscriptions`, `DELETE /api/v1/subscriptions/{id}` |
| Webhooky, podpis, retry | `deliver()` v centrále; popis ve `/docs` → *Webhooks* |
| Terminál | `/webhooks/votes`, `/stats`, `/` (obrazovka) |

Polling (krok 4) je ve finální verzi nahrazen webhooky + snapshotem při startu.

## Testy
```bash
# 201, potom 409 (Novák.Jan == novak.jan)
curl -i -X POST http://localhost:8000/api/v1/stands/S201/votes -H "Content-Type: application/json" -d '{"username":"novak.jan","rating":5,"favorite":true}'
curl -i -X POST http://localhost:8000/api/v1/stands/S201/votes -H "Content-Type: application/json" -d '{"username":"Novák.Jan","rating":4,"favorite":false}'

# 422
curl -i -X POST http://localhost:8000/api/v1/stands/S201/votes -H "Content-Type: application/json" --data-binary @bad.json

# falešná událost bez podpisu -> 401
curl -i -X POST http://localhost:3000/webhooks/votes -H "Content-Type: application/json" --data-binary @fake.json

# správný podpis (secret zná jen terminál – pro test si ho dočasně vypiš do logu v lifespan)
openssl dgst -sha256 -hmac "<secret>" fake.json      # hex patří do hlavičky X-Signature: sha256=<hex>
curl -i -X POST http://localhost:3000/webhooks/votes -H "Content-Type: application/json" -H "X-Signature: sha256=<hex>" --data-binary @fake.json
# 3× po sobě -> pokaždé 200, započítáno jednou (idempotence podle "id")
```
Další kontroly: vypnutý terminál → hlas se uloží a centrála zaloguje `NEDORUČENO`;
přestěhování (vypnout, `FLOOR=3`, zapnout) → obrazovka ukáže hlasy nového patra.
`fake.json` má `created_at` v roce 2099, aby prošel filtrem „už je ve snapshotu“.

## Odpovědi na otázky ze zadání
* **Jak centrála pozná, že terminál zanikl?** Terminál při vypnutí pošle `DELETE`. Při pádu nepošle,
  proto centrála po 5 nedoručených událostech v řadě (každá po 3 opakováních) odběr smaže
  (`MAX_FAILED_EVENTS`). Restartovaný terminál se zaregistruje znovu s novým `id` i `secret`.
* **Proč u duplicity `200`, ne `409`?** Centrále jde o to, že událost dorazila. `409` není `2xx`, takže by
  doručení opakovala a nakonec by odběr po chybách zrušila.
* **Kdy terminál hlas započítá?** Až po ověření podpisu a kontrole duplicity (`id`), v handleru `/webhooks/votes`.
* **Podpis:** centrála serializuje tělo jednou, podepíše a odešle tytéž bajty; terminál počítá HMAC ze
  surových bajtů (`request.body()`) před parsováním JSON (závislost `verify_signature`, funguje jako middleware).
* **Pořadí při startu:** nejdřív odběr, potom snapshot. Událost, která už je ve snapshotu a přijde znovu
  (retry), se pozná podle `created_at <= generated_at` a nezapočítá se podruhé.
* **Data jsou jen v paměti** – po restartu centrály hlasy zmizí. Pro trvalost stačí přidat SQLite.
