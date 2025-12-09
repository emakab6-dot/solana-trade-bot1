# =========================================================
#  MEMES DEX TRADES BOT – HELIUS ONLY (FULL WHITELIST)
#  - Tous les tokens de pools-raydium-orca.json en whitelist
#  - 1 seule pool par token : la plus liquide / pertinente
#  - 1 WebSocket Helius (max 50 pools / WS → 24 pools ok)
#  - Dexscreener pour ne garder que les pools actives (5mn)
#  - Worker throttlé : max 5 getTransaction/sec
#  - Filtre : trades >= 1000$ (whales)
# =========================================================

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import os
import requests
import websockets
from telegram import Bot

# ---------------------------------------------------------
# CONFIG UTILISATEUR
# ---------------------------------------------------------

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN environment variable not set")
bot = Bot(token=TELEGRAM_TOKEN)

# 🔥 HELIUS ENDPOINTS (HTTP + WS)
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
if not HELIUS_API_KEY:
    raise RuntimeError("HELIUS_API_KEY environment variable not set")
HELIUS_HTTP_ENDPOINT = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_WS_ENDPOINT = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# Tous les endpoints RPC HTTP utilisés en round-robin (ici : Helius only)
RPC_HTTP_ENDPOINTS: List[Tuple[str, str]] = [
    ("Helius", HELIUS_HTTP_ENDPOINT),
]

# Tous les endpoints WebSocket (ici : Helius only)
WS_ENDPOINTS: List[Tuple[str, str]] = [
    ("Helius", HELIUS_WS_ENDPOINT),
]

# Index interne pour faire tourner les endpoints HTTP
_RPC_HTTP_INDEX = 0

# Fichier pools
POOLS_FILE = "pools-raydium-orca.json"

# Programmes DEX (pour logs uniquement)
RAYDIUM_AMM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM = "CPMMoo8L3F4NbTegBCKvNunggL7H1ZpdTHKxQbSqKP1C"
RAYDIUM_CLMM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
PUMPSWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
ORCA_SWAP_V2_PROGRAM_ID = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP"
ORCA_WHIRLPOOL_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"

PROGRAMS_TO_WATCH: Dict[str, str] = {
    RAYDIUM_AMM_V4: "Raydium AMM v4",
    RAYDIUM_CPMM: "Raydium CPMM",
    RAYDIUM_CLMM: "Raydium CLMM",
    PUMPSWAP_PROGRAM_ID: "PumpSwap",
    ORCA_SWAP_V2_PROGRAM_ID: "Orca Swap V2",
    ORCA_WHIRLPOOL_PROGRAM_ID: "Orca Whirlpools",
}

# 💥 On garde TOUS les tokens (whitelist complète)
TARGET_SYMBOLS: Set[str] = set()

# Mints base
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJFrF4HkXHYQG6K7UT2HaqN1gSdZ8rtd"
WSOL_MINT = "So11111111111111111111111111111111111111112"

BASE_MINTS = {USDC_MINT, USDT_MINT, WSOL_MINT}
MINT_NAMES = {
    USDC_MINT: "USD Coin (USDC)",
    USDT_MINT: "Tether USD (USDT)",
    WSOL_MINT: "Wrapped SOL (WSOL)",
}

# Prix SOL via Binance
BINANCE_SOL_PRICE = "https://api.binance.com/api/v3/ticker/price"
SOL_PRICE_USD = 100.0  # fallback

# Filtre trade
MIN_USD_TRADE = 1000.0

# Whitelist & pools
TOKENS_WHITELIST: Dict[str, Dict[str, str]] = {}
POOLS_BY_ADDRESS: Dict[str, Dict[str, Any]] = {}
WATCHLIST_POOLS: List[str] = []

# Multi-WebSocket
MAX_POOLS_PER_WS = 50  # 24 pools → 1 seul WS Helius

# Limite RPC parallèle
RPC_SEMAPHORE = asyncio.Semaphore(8)

# Throttling worker
MAX_TX_PER_SECOND = 5
TX_QUEUE: "asyncio.Queue[Tuple[str, str, int]]" = asyncio.Queue(maxsize=300)

# Déduplication signatures
SEEN_SIGNATURES: Set[str] = set()

# Pools actives Dexscreener
DEXSCREENER_PAIRS_BASE = "https://api.dexscreener.com/latest/dex/pairs/solana/"
MAX_POOLS_PER_DEX_REQUEST = 30
ACTIVE_MIN_M5_TRADES = 1
ACTIVE_REFRESH_INTERVAL = 30
ACTIVE_POOLS: Set[str] = set()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def short(s: str, n: int = 6) -> str:
    s = s or ""
    return s[:n] + "…" if len(s) > n else s


# ---------------- POOLS & WHITELIST ----------------

def load_pools_and_tokens():
    global TOKENS_WHITELIST, POOLS_BY_ADDRESS, WATCHLIST_POOLS

    TOKENS_WHITELIST = {}
    POOLS_BY_ADDRESS = {}
    WATCHLIST_POOLS = []

    try:
        with open(POOLS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"[POOLS] Erreur chargement {POOLS_FILE} : {e}")
        return

    target_upper = {s.upper() for s in TARGET_SYMBOLS} if TARGET_SYMBOLS else set()

    for mint, info in data.items():
        symbol = info.get("symbol") or short(mint)
        name = info.get("name") or symbol

        if target_upper and symbol.upper() not in target_upper:
            continue

        TOKENS_WHITELIST[mint] = {"symbol": symbol, "name": name}

        pools = info.get("pools") or []
        if not pools:
            continue

        best_addr = None
        best_pool = None
        best_score = -1.0

        for pool in pools:
            addr = pool.get("pairAddress")
            if not addr:
                continue

            liq = float(pool.get("liquidityUsd") or 0.0)
            quote_info = pool.get("quoteToken") or {}
            quote_mint = quote_info.get("address")

            score = liq
            if quote_mint in BASE_MINTS:
                score *= 1.5

            if score > best_score:
                best_score = score
                best_addr = addr
                best_pool = pool

        if not best_addr or not best_pool:
            continue

        POOLS_BY_ADDRESS[best_addr] = {
            "mint": mint,
            "symbol": symbol,
            "dexId": best_pool.get("dexId"),
            "quoteSymbol": (best_pool.get("quoteToken") or {}).get("symbol"),
            "liquidityUsd": float(best_pool.get("liquidityUsd") or 0.0),
            "url": best_pool.get("url"),
        }

    WATCHLIST_POOLS = list(POOLS_BY_ADDRESS.keys())

    logging.info(
        f"[POOLS] Tokens whitelist: {len(TOKENS_WHITELIST)} | "
        f"Pools surveillées (1 par token): {len(WATCHLIST_POOLS)}"
    )


# ---------------- POOLS ACTIVES (DEXSCREENER) ----------------

def fetch_active_pools_once() -> Set[str]:
    active: Set[str] = set()
    if not WATCHLIST_POOLS:
        return active

    chunks = [
        WATCHLIST_POOLS[i:i + MAX_POOLS_PER_DEX_REQUEST]
        for i in range(0, len(WATCHLIST_POOLS), MAX_POOLS_PER_DEX_REQUEST)
    ]

    for group in chunks:
        try:
            url = DEXSCREENER_PAIRS_BASE + ",".join(group)
            r = requests.get(url, timeout=10)
            data = r.json()
            pairs = data.get("pairs") or []
            for p in pairs:
                addr = p.get("pairAddress")
                txns = p.get("txns") or {}
                m5 = txns.get("m5") or {}
                buys = int(m5.get("buys") or 0)
                sells = int(m5.get("sells") or 0)
                if buys + sells >= ACTIVE_MIN_M5_TRADES:
                    active.add(addr)
        except Exception as e:
            logging.warning(f"[ACTIVE] Erreur Dexscreener: {e}")
            continue

    return active


async def active_pools_refresher():
    global ACTIVE_POOLS
    while True:
        try:
            new_active = await asyncio.to_thread(fetch_active_pools_once)
            ACTIVE_POOLS = new_active
            logging.info(f"[ACTIVE] Pools actives (5mn) : {len(ACTIVE_POOLS)}")
        except Exception as e:
            logging.warning(f"[ACTIVE] Erreur refresh: {e}")
        await asyncio.sleep(ACTIVE_REFRESH_INTERVAL)


# ---------------- TELEGRAM ----------------

def get_chat_id() -> Optional[int]:
    try:
        updates = bot.get_updates()
        if not updates:
            logging.warning(
                "Aucun chat Telegram détecté. Envoie d'abord un message à ton bot."
            )
            return None
        cid = updates[-1].message.chat_id
        logging.info(f"[TELEGRAM] chat_id détecté : {cid}")
        return cid
    except Exception as e:
        logging.error(f"[TELEGRAM] Erreur get_updates : {e}")
        return None


async def send_telegram(chat_id: int, text: str):
    try:
        await asyncio.to_thread(
            bot.send_message,
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=False,
        )
    except Exception as e:
        logging.warning(f"[TELEGRAM] Erreur envoi : {e}")


# ---------------- PRIX SOL ----------------

def fetch_sol_price() -> float:
    global SOL_PRICE_USD
    try:
        r = requests.get(
            BINANCE_SOL_PRICE,
            params={"symbol": "SOLUSDT"},
            timeout=8,
        )
        data = r.json()
        price = float(data["price"])
        SOL_PRICE_USD = price
        logging.info(f"[PRIX] SOL/USDT : {price:.2f} $")
        return price
    except Exception as e:
        logging.warning(f"[PRIX] Erreur récupération prix SOL : {e}")
        return SOL_PRICE_USD


def base_to_usd(base_mint: str, amount: float) -> float:
    if base_mint in (USDC_MINT, USDT_MINT):
        return amount
    if base_mint == WSOL_MINT:
        return amount * SOL_PRICE_USD
    return 0.0


# ---------------- RPC HELIUS HTTP ----------------

def rpc_post(payload: Dict[str, Any]) -> Dict[str, Any]:
    global _RPC_HTTP_INDEX

    last_exc: Optional[Exception] = None
    n = len(RPC_HTTP_ENDPOINTS)
    if n == 0:
        raise RuntimeError("Aucun endpoint RPC défini")

    for i in range(n):
        name, url = RPC_HTTP_ENDPOINTS[(_RPC_HTTP_INDEX + i) % n]
        try:
            r = requests.post(url, json=payload, timeout=15)
            status = r.status_code

            if status == 429:
                logging.warning(
                    f"[RPC] {name} renvoie 429 (rate limit), on essaie un autre endpoint…"
                )
                last_exc = RuntimeError("429 Too Many Requests")
                continue

            r.raise_for_status()
            data = r.json()
            if "error" in data:
                logging.warning(f"[RPC] Erreur JSON-RPC sur {name}: {data['error']}")
                last_exc = RuntimeError(str(data["error"]))
                continue

            _RPC_HTTP_INDEX = (_RPC_HTTP_INDEX + 1) % n
            return data

        except Exception as e:
            last_exc = e
            logging.warning(f"[RPC] Erreur sur endpoint {name}: {e}")
            continue

    raise RuntimeError(f"Tous les endpoints RPC ont échoué : {last_exc}")


def get_parsed_tx(signature: str) -> Optional[Dict[str, Any]]:
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
            ],
        }
        data = rpc_post(payload)
        return data.get("result")
    except Exception as e:
        logging.warning(f"[TX] getTransaction erreur {signature}: {e}")
        return None


# ---------------- DELTAS TOKEN ----------------

def extract_token_deltas(tx: Dict[str, Any]) -> List[Dict[str, Any]]:
    meta = tx.get("meta") or {}
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    pre_by = {b["accountIndex"]: b for b in pre}
    post_by = {b["accountIndex"]: b for b in post}

    deltas: List[Dict[str, Any]] = []

    def get_ui(entry):
        if not entry:
            return 0.0
        amt = entry.get("uiTokenAmount") or {}
        return float(amt.get("uiAmount") or 0.0)

    all_ids = set(pre_by.keys()) | set(post_by.keys())
    for idx in all_ids:
        pre_e = pre_by.get(idx)
        post_e = post_by.get(idx)
        if not pre_e and not post_e:
            continue

        mint = (post_e or pre_e).get("mint")
        owner = (post_e or pre_e).get("owner")

        pre_val = get_ui(pre_e)
        post_val = get_ui(post_e)
        delta = post_val - pre_val
        if abs(delta) < 1e-9:
            continue

        deltas.append({"mint": mint, "owner": owner, "delta": delta})

    return deltas


def collapse_by_mint(
    deltas: List[Dict[str, Any]],
    signer_pubkeys: Optional[Set[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    signer_pubkeys = signer_pubkeys or set()
    out: Dict[str, Dict[str, Any]] = {}

    for d in deltas:
        mint = d["mint"]
        owner = d.get("owner")
        is_signer = owner in signer_pubkeys

        if mint not in out:
            out[mint] = {**d, "_is_signer": is_signer}
            continue

        cur = out[mint]
        cur_signer = cur.get("_is_signer", False)

        if is_signer and not cur_signer:
            out[mint] = {**d, "_is_signer": is_signer}
        elif is_signer == cur_signer and abs(d["delta"]) > abs(cur["delta"]):
            out[mint] = {**d, "_is_signer": is_signer}

    for v in out.values():
        v.pop("_is_signer", None)

    return out


def classify_swap(by_mint: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Détermine base/other + sens de trade.
    IMPORTANT : le sens est basé sur le token OTHER (celui qu'on track).
      - other_delta > 0  => BUY du token
      - other_delta < 0  => SELL du token
    """
    if len(by_mint) < 2:
        return None

    # Les 2 mints principaux par taille de delta (absolue)
    items = sorted(by_mint.items(), key=lambda kv: abs(kv[1]["delta"]), reverse=True)
    (m0, d0), (m1, d1) = items[0], items[1]

    # Si les deux deltas sont dans le même sens, ce n'est pas un swap simple
    if (d0["delta"] > 0 and d1["delta"] > 0) or (d0["delta"] < 0 and d1["delta"] < 0):
        return None

    # On identifie le token de base (USDC/USDT/WSOL) et l'autre
    if m0 in BASE_MINTS:
        base_mint = m0
        base_delta = d0["delta"]
        other_mint = m1
        other_delta = d1["delta"]
    elif m1 in BASE_MINTS:
        base_mint = m1
        base_delta = d1["delta"]
        other_mint = m0
        other_delta = d0["delta"]
    else:
        # Aucun des deux n'est une base => swap exotique, on ignore
        return None

    # ❗ Nouveau : sens basé sur le token OTHER (celui qu'on surveille)
    # Si le wallet a + de token other => il a ACHETÉ le token
    # Si le wallet a - de token other => il a VENDU le token
    side = "BUY" if other_delta > 0 else "SELL"

    return {
        "base_mint": base_mint,
        "other_mint": other_mint,
        "base_delta": base_delta,
        "other_delta": other_delta,
        "side": side,
    }


# ---------------- TRAITEMENT D’UNE TX ----------------

async def process_signature(signature: str, program_label: str, chat_id: int):
    if signature in SEEN_SIGNATURES:
        return
    SEEN_SIGNATURES.add(signature)

    logging.info(f"[PROC] Analyse signature {signature} ({program_label})")

    tx: Optional[Dict[str, Any]] = None
    max_tries = 3
    for _ in range(max_tries):
        async with RPC_SEMAPHORE:
            tx = await asyncio.to_thread(get_parsed_tx, signature)
        if tx:
            break
        await asyncio.sleep(0.35)

    if not tx:
        logging.info(
            f"[PROC] Aucune donnée getTransaction pour {signature} "
            f"après {max_tries} tentatives"
        )
        return

    msg = tx.get("transaction", {}).get("message", {})
    keys = msg.get("accountKeys") or []

    signer_pubkeys: Set[str] = set()
    for idx, k in enumerate(keys):
        if isinstance(k, str):
            if idx == 0:
                signer_pubkeys.add(k)
        else:
            if k.get("signer"):
                pk = k.get("pubkey")
                if pk:
                    signer_pubkeys.add(pk)

    pool_addr = None
    for k in keys:
        pk = k if isinstance(k, str) else k.get("pubkey")
        if pk in POOLS_BY_ADDRESS:
            pool_addr = pk
            break

    if not pool_addr:
        return

    pool_info = POOLS_BY_ADDRESS.get(pool_addr, {})
    deltas = extract_token_deltas(tx)
    if not deltas:
        logging.info(f"[PROC] Pas de deltas pour {signature}")
        return

    by_mint = collapse_by_mint(deltas, signer_pubkeys)
    swap = classify_swap(by_mint)
    if not swap:
        logging.info(f"[PROC] Impossible de classifier swap : {signature}")
        return

    base_mint = swap["base_mint"]
    other_mint = swap["other_mint"]
    base_delta = swap["base_delta"]
    other_delta = swap["other_delta"]
    side = swap["side"]

    base_amount = abs(base_delta)
    token_amount = abs(other_delta)

    usd_amount = base_to_usd(base_mint, base_amount)
    if usd_amount < MIN_USD_TRADE:
        logging.info(
            f"[PROC] Trade ignoré (< {MIN_USD_TRADE}$) : {usd_amount:.2f}$"
        )
        return

    token_info = TOKENS_WHITELIST.get(other_mint)
    if not token_info:
        logging.info(f"[PROC] Mint non-whitelisté : {other_mint}")
        return

    token_symbol = token_info.get("symbol")
    token_name = token_info.get("name")
    base_name = MINT_NAMES.get(base_mint, short(base_mint))

    emoji = "🟢" if side == "BUY" else "🔴"
    token_url = f"https://solscan.io/token/{other_mint}"
    tx_url = f"https://solscan.io/tx/{signature}"
    price_in_base = base_amount / token_amount if token_amount > 0 else 0.0

    lines = [
        f"{emoji} <b>{side} {token_symbol}</b>",
        f"Nom: <b>{token_name}</b>",
        "",
        f"Base: <b>{base_name}</b>",
        f"Montant base: <b>{base_amount:.6f}</b>",
        f"Montant token: <b>{token_amount:.6f}</b>",
        f"Prix token en base: <b>{price_in_base:.6f}</b>",
        f"Taille ≈ <b>{usd_amount:,.2f} $</b>",
        "",
        f"Pool: <code>{short(pool_addr, 8)}</code> ({pool_info.get('dexId','?')})",
        "",
        f"Token: <a href=\"{token_url}\">{short(other_mint, 6)}</a>",
        f"Tx: <a href=\"{tx_url}\">Solscan</a>",
        "",
        f"Programme: <code>{program_label}</code>",
    ]

    await send_telegram(chat_id, "\n".join(lines))


# ---------------- WORKER ----------------

async def tx_worker():
    delay = 1.0 / MAX_TX_PER_SECOND
    logging.info(f"[WORKER] Démarrage worker TX (max {MAX_TX_PER_SECOND} tx/s).")

    while True:
        signature, program_label, chat_id = await TX_QUEUE.get()
        start = time.monotonic()
        try:
            await process_signature(signature, program_label, chat_id)
        except Exception as e:
            logging.warning(f"[WORKER] Erreur process_signature: {e}")
        finally:
            TX_QUEUE.task_done()

        elapsed = time.monotonic() - start
        if elapsed < delay:
            await asyncio.sleep(delay - elapsed)


# ---------------- WEBSOCKET LOOP (HELIUS ONLY) ----------------

async def ws_loop(chat_id: int, pools_subset: List[str], ws_index: int):
    ws_name, ws_url = WS_ENDPOINTS[0]  # Helius only
    label = f"WS[{ws_index}|{ws_name}]"
    backoff = 1.0

    logging.info(
        f"[{label}] Démarrage WebSocket pour {len(pools_subset)} pools sur {ws_name}..."
    )

    while True:
        try:
            logging.info(f"[{label}] Connexion à {ws_url}")

            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:

                logging.info(f"[{label}] Connecté ✔")

                reqid_to_pool: Dict[int, str] = {}
                subid_to_pool: Dict[int, str] = {}

                base_id = ws_index * 50000

                for i, pool_addr in enumerate(pools_subset):
                    req_id = base_id + i + 1
                    sub = {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [pool_addr]},
                            {"commitment": "confirmed"},
                        ],
                    }
                    reqid_to_pool[req_id] = pool_addr
                    await ws.send(json.dumps(sub))
                    await asyncio.sleep(0.05)  # petit délai pour ne pas spammer

                logging.info(
                    f"[{label}] {len(pools_subset)} subscriptions envoyées."
                )

                while True:
                    raw = await ws.recv()
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if "method" not in msg:
                        if "result" in msg and "id" in msg:
                            sub_id = msg["result"]
                            req_id = msg["id"]
                            pool_addr = reqid_to_pool.get(req_id)
                            if pool_addr:
                                subid_to_pool[int(sub_id)] = pool_addr
                        continue

                    if msg.get("method") != "logsNotification":
                        continue

                    params = msg.get("params", {})
                    result = params.get("result", {})
                    value = result.get("value", {})

                    signature = value.get("signature")
                    logs_arr = value.get("logs") or []
                    sub_id = params.get("subscription")

                    if not signature or sub_id is None:
                        continue

                    pool_addr = subid_to_pool.get(int(sub_id))
                    if not pool_addr:
                        continue

                    if ACTIVE_POOLS and pool_addr not in ACTIVE_POOLS:
                        continue

                    program_label = "Unknown"
                    for prog, lbl in PROGRAMS_TO_WATCH.items():
                        if any(prog in line for line in logs_arr):
                            program_label = lbl
                            break

                    logging.info(
                        f"[{label}] Tx détectée {signature} ({program_label})"
                    )

                    if TX_QUEUE.full():
                        try:
                            _ = TX_QUEUE.get_nowait()
                            TX_QUEUE.task_done()
                            logging.warning(
                                "[QUEUE] Pleine : drop d'une ancienne TX."
                            )
                        except asyncio.QueueEmpty:
                            pass

                    await TX_QUEUE.put((signature, program_label, chat_id))

        except Exception as e:
            logging.warning(f"[{label}] Erreur WebSocket : {e}")
            logging.info(f"[{label}] Reconnexion dans {backoff:.1f}s…")
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2)


# ---------------- OUTILS ----------------

def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


# ---------------- MAIN ----------------

async def main():
    global ACTIVE_POOLS

    load_pools_and_tokens()
    if not TOKENS_WHITELIST:
        logging.error("Whitelist vide. Vérifie pools-raydium-orca.json.")
        return
    if not WATCHLIST_POOLS:
        logging.error("Aucun pool dans WATCHLIST_POOLS.")
        return

    fetch_sol_price()

    chat_id = get_chat_id()
    if not chat_id:
        logging.error("Impossible de récupérer un chat_id Telegram.")
        return

    logging.info(f"[MAIN] Tokens whitelist: {len(TOKENS_WHITELIST)}")
    logging.info(f"[MAIN] Pools surveillées: {len(WATCHLIST_POOLS)}")
    logging.info(f"[MAIN] Seuil USD: {MIN_USD_TRADE}$")
    logging.info(
        "Programmes DEX (pour logs): " + ", ".join(PROGRAMS_TO_WATCH.values())
    )

    logging.info("[ACTIVE] Initialisation des pools actives via Dexscreener…")
    ACTIVE_POOLS = fetch_active_pools_once()
    logging.info(f"[ACTIVE] Pools actives initiales : {len(ACTIVE_POOLS)}")

    groups = chunk_list(WATCHLIST_POOLS, MAX_POOLS_PER_WS)
    logging.info(
        f"[MAIN] Total pools: {len(WATCHLIST_POOLS)} | "
        f"{len(groups)} WebSockets (max {MAX_POOLS_PER_WS} pools/WS)"
    )

    tasks = []
    tasks.append(asyncio.create_task(tx_worker()))
    tasks.append(asyncio.create_task(active_pools_refresher()))

    for index, subset in enumerate(groups):
        tasks.append(ws_loop(chat_id, subset, index))

    await asyncio.gather(*tasks)


# ---------------- ENTRYPOINT ----------------

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nArrêt manuel.")
