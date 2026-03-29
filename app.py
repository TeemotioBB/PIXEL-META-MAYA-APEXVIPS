#!/usr/bin/env python3
import os
import logging
import hashlib
import time
import asyncio
import threading
import requests
import redis
import json

from datetime import date
from flask import Flask, request, jsonify
from flask_cors import CORS
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ====================== LOGGING ======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ====================== ENV VARS ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")
PIXEL_ID = os.getenv("PIXEL_ID", "735253462874774")
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

# Validação de env vars essenciais
missing_envs = [k for k,v in {
    "TELEGRAM_TOKEN_APEX": TELEGRAM_TOKEN,
    "REDIS_URL": REDIS_URL,
    "META_ACCESS_TOKEN": ACCESS_TOKEN,
    "WEBHOOK_BASE_URL": WEBHOOK_BASE_URL
}.items() if not v]
if missing_envs:
    logger.error(f"Variáveis de ambiente faltando: {missing_envs}")
    raise SystemExit(1)

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Redis conectado!")
except Exception as e:
    logger.exception(f"❌ Erro Redis: {e}")
    raise

# ====================== FLASK / TELEGRAM / CORS ======================
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

application = Application.builder().token(TELEGRAM_TOKEN).build()

# ====================== HTTP SESSION COM RETRY ======================
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429,500,502,503,504], allowed_methods=["POST","GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))

# ====================== USER DATA ======================
def montar_user_data(uid):
    """
    Monta user_data para CAPI.
    Prefere email/phone hashed se disponíveis; senão usa external_id hashed do chat_id como fallback.
    Inclui fbp/fbc/ip/ua apenas se existirem.
    """
    user_data = {}

    # Preferir email/phone se existirem (armazenados previamente no Redis)
    email = r.get(f"email:{uid}")
    phone = r.get(f"phone:{uid}")
    if email:
        user_data["em"] = hash_data(email)
    if phone:
        user_data["ph"] = hash_data(phone)

    # Fallback para external_id (documentar que é fallback e pode reduzir match)
    if not any(k in user_data for k in ("em", "ph")):
        user_data["external_id"] = [hash_data(str(uid))]

    # Campos de tracking
    fbp = r.get(f"fbp:{uid}")
    fbc = r.get(f"fbc:{uid}")
    ip  = r.get(f"ip:{uid}")
    ua  = r.get(f"ua:{uid}")

    if fbp:
        user_data["fbp"] = fbp
    if fbc:
        user_data["fbc"] = fbc
    if ip:
        user_data["client_ip_address"] = ip
    if ua:
        user_data["client_user_agent"] = ua

    logger.info(f"[montar_user_data] UID {uid} → keys: {list(user_data.keys())}")
    return user_data

# ====================== POST PARA META (com log e fila de retry) ======================
def _post_to_meta(payload):
    url = f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events"
    try:
        resp = session.post(url, json=payload, timeout=10)
        logger.info(f"[Meta] status={resp.status_code} response={resp.text}")
        if resp.status_code >= 400:
            # gravar para retry assíncrono
            try:
                r.rpush("pending_capi_events", json.dumps({"payload": payload, "status": resp.status_code, "response": resp.text}))
            except Exception:
                logger.exception("Erro ao gravar pending_capi_events")
            return False
        return True
    except Exception as e:
        logger.exception("Erro ao postar no Meta")
        try:
            r.rpush("pending_capi_events", json.dumps({"payload": payload, "error": str(e)}))
        except Exception:
            logger.exception("Erro ao gravar pending_capi_events")
        return False

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    """
    Envia Lead com trava de envio único por dia.
    Se já foi enviado hoje COM tracking, não envia de novo.
    Se foi enviado SEM tracking mas agora tem FBP, reenvia enriquecido.
    """
    lock_key = f"lead_sent:{uid}:{date.today()}"
    lock_val = r.get(lock_key)

    fbp_exists = bool(r.get(f"fbp:{uid}"))

    if lock_val == "com_tracking":
        logger.info(f"⏭️ [CAPI] Lead já enviado COM tracking hoje — UID: {uid} ignorado")
        return

    if lock_val == "sem_tracking" and not fbp_exists:
        logger.info(f"⏭️ [CAPI] Lead já enviado SEM tracking e tracking ainda não chegou — UID: {uid} ignorado")
        return

    if lock_val == "sem_tracking" and fbp_exists:
        trigger = f"{trigger}_enriquecido"
        logger.info(f"🔄 [CAPI] Reenviando Lead enriquecido para UID: {uid}")

    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": f"lead_{uid}_{date.today()}_{trigger}",
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"trigger": trigger}
        }],
        "access_token": ACCESS_TOKEN
    }
    ok = _post_to_meta(payload)
    novo_lock = "com_tracking" if fbp_exists else "sem_tracking"
    if ok:
        r.set(lock_key, novo_lock, ex=86400)
        logger.info(f"🟢 [CAPI] Lead ENVIADO | UID: {uid} | Trigger: {trigger} | Lock: {novo_lock}")
    else:
        logger.warning(f"❌ [CAPI] Falha ao enviar Lead | UID: {uid} | gravado para retry")

def enviar_initiatecheckout_capi(uid: int):
    redis_key = f"checkout_sent:{uid}"
    if r.exists(redis_key):
        logger.info(f"⏭️ [CAPI] InitiateCheckout já enviado recentemente para UID: {uid}")
        return
    r.set(redis_key, "1", ex=3600)

    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "InitiateCheckout",
            "event_time": int(time.time()),
            "event_id": f"init_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"currency": "BRL", "value": 0.00}
        }],
        "access_token": ACCESS_TOKEN
    }
    ok = _post_to_meta(payload)
    if ok:
        logger.info(f"🔥 [CAPI] Checkout ENVIADO | UID: {uid}")
    else:
        logger.warning(f"❌ [CAPI] Falha ao enviar InitiateCheckout | UID: {uid}")

def enviar_purchase_capi(uid: int, valor: float):
    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": f"pur_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"value": valor, "currency": "BRL"}
        }],
        "access_token": ACCESS_TOKEN
    }
    ok = _post_to_meta(payload)
    if ok:
        logger.info(f"💰 [CAPI] Purchase ENVIADO | UID: {uid} | R$ {valor:.2f}")
    else:
        logger.warning(f"❌ [CAPI] Falha ao enviar Purchase | UID: {uid} | R$ {valor:.2f}")

# ====================== VINCULAR TRACKING ======================
def vincular_tracking_por_uid_temp(uid_real: int, uid_temp: str) -> bool:
    tracking_str = r.get(f"tracking:{uid_temp}")
    if not tracking_str:
        return False
    try:
        tracking_data = json.loads(tracking_str)
        if "fbp" in tracking_data:
            r.set(f"fbp:{uid_real}", tracking_data["fbp"], ex=604800)
        if "fbc" in tracking_data:
            r.set(f"fbc:{uid_real}", tracking_data["fbc"], ex=604800)
        if "client_ip" in tracking_data:
            r.set(f"ip:{uid_real}", tracking_data["client_ip"], ex=604800)
        if "client_user_agent" in tracking_data:
            r.set(f"ua:{uid_real}", tracking_data["client_user_agent"], ex=604800)
        r.expire(f"tracking:{uid_temp}", 300)
        logger.info(f"✅ [VÍNCULO] {uid_temp} → UID {uid_real} | FBP: {bool(tracking_data.get('fbp'))}")
        return True
    except Exception:
        logger.exception("❌ Erro ao vincular tracking")
        return False

# ====================== APEX JOINED FALLBACK ======================
def apex_joined_fallback(uid: int):
    time.sleep(20)
    if not r.get(f"pending_join:{uid}"):
        return
    r.delete(f"pending_join:{uid}")
    matched = False
    try:
        for key in r.scan_iter(match="tracking:track_*"):
            # key pode ser str ou bytes dependendo do client
            key_str = key.decode() if isinstance(key, bytes) else key
            uid_temp = key_str.replace("tracking:", "")
            if r.get(f"bridge:{uid_temp}"):
                continue
            ttl = r.ttl(f"tracking:{uid_temp}")
            # ignorar tracking com TTL muito baixo (ex.: < 60s)
            if ttl is not None and ttl < 60:
                logger.info(f"⏭️ [FALLBACK] {uid_temp} ignorado — tracking muito velho (TTL: {ttl})")
                continue
            if vincular_tracking_por_uid_temp(uid, uid_temp):
                r.set(f"bridge:{uid_temp}", str(uid), ex=3600)
                logger.info(f"🔁 [FALLBACK MATCH] {uid_temp} → {uid}")
                matched = True
                break
    except Exception:
        logger.exception("❌ Erro no fallback match")
    enviar_lead_capi(uid, "fallback_com_match" if matched else "apex_joined_sem_start")

# ====================== HANDLERS TELEGRAM ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    args = context.args
    payload = args[0] if args else ""

    logger.info(f"🚀 [START] User: {uid} | Payload: '{payload}'")

    if payload.startswith("track_"):
        temp_key = payload

        tracking_str = None
        for tentativa in range(10):
            tracking_str = r.get(f"tracking:{temp_key}")
            if tracking_str:
                break
            logger.info(f"⏳ Tentativa {tentativa+1}/10: Aguardando tracking de {temp_key}...")
            await asyncio.sleep(2.0)

        if tracking_str:
            vincular_tracking_por_uid_temp(uid, temp_key)
        else:
            logger.warning(f"⚠️ Tracking {temp_key} não chegou em 20s — salvando pending_uid")
            r.set(f"pending_uid:{temp_key}", str(uid), ex=300)

        # Salva bridge: uid_temp → uid_real (permite retro-vínculo no apex-tracking)
        r.set(f"bridge:{temp_key}", str(uid), ex=3600)
        logger.info(f"🌉 [BRIDGE] bridge:{temp_key} → {uid} salvo")

    # /start cancela o fallback e envia o Lead (dono do envio)
    r.delete(f"pending_join:{uid}")
    enviar_lead_capi(uid, "start")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()

    cb_data = (query.data or "").lower()
    logger.info(f"🖱️ [BOTÃO] User: {uid} | Clique: {cb_data}")

    enviar_lead_capi(uid, "button_click")

    if any(x in cb_data for x in ["plan", "buy", "pix", "pay"]):
        enviar_initiatecheckout_capi(uid)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").lower()

    logger.info(f"💬 [MENSAGEM] User: {uid}")
    enviar_lead_capi(uid, "chat")

    if "pagar com pix" in text or "plano selecionado" in text:
        enviar_initiatecheckout_capi(uid)

# ====================== ENGINE ======================
application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CallbackQueryHandler(button_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

bot_loop = asyncio.new_event_loop()

def run_bot():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())
    bot_loop.run_forever()

threading.Thread(target=run_bot, daemon=True).start()

# ====================== ROUTES ======================
@app.route('/apex-tracking', methods=['GET'])
def apex_tracking():
    uid = request.args.get('uid')
    if not uid:
        return jsonify({"error": "uid não fornecido"}), 400

    user_agent = request.headers.get('User-Agent', '')
    bots = ['vercel-screenshot', 'HeadlessChrome', 'Googlebot', 'bingbot', 'facebookexternalhit']
    if any(bot.lower() in user_agent.lower() for bot in bots):
        logger.info(f"🤖 [BOT BLOQUEADO] uid={uid} | UA: {user_agent[:60]}")
        return jsonify({"status": "ignored", "reason": "bot"}), 200

    fbclid            = request.args.get('fbclid')
    fbp               = request.args.get('fbp')
    fbc               = request.args.get('fbc')
    client_ip         = request.headers.get('X-Forwarded-For', request.remote_addr)
    client_user_agent = request.headers.get('User-Agent')

    tracking_data = {}
    if fbp:
        tracking_data["fbp"] = fbp
    if fbc:
        tracking_data["fbc"] = fbc
    elif fbclid:
        # usar timestamp em segundos
        tracking_data["fbc"] = f"fb.1.{int(time.time())}.{fbclid}"
    if client_ip:
        tracking_data["client_ip"] = client_ip.split(',')[0].strip()
    if client_user_agent:
        tracking_data["client_user_agent"] = client_user_agent

    try:
        r.set(f"tracking:{uid}", json.dumps(tracking_data), ex=86400)
        logger.info(f"💾 [TRACKING RECEBIDO E SALVO] uid={uid} | keys={list(tracking_data.keys())}")
    except Exception:
        logger.exception("Erro ao salvar tracking no Redis")

    # RETRO-VÍNCULO via pending_uid (o /start desistiu de esperar)
    uid_real = r.get(f"pending_uid:{uid}")
    if uid_real:
        logger.info(f"🔁 [RETRO-VÍNCULO pending_uid] uid_temp={uid} → uid_real={uid_real}")
        vincular_tracking_por_uid_temp(int(uid_real), uid)
        enviar_lead_capi(int(uid_real), "retro_tracking")
        r.delete(f"pending_uid:{uid}")

    # RETRO-VÍNCULO via bridge (o /start já chegou, mas apex_joined veio depois)
    uid_real_bridge = r.get(f"bridge:{uid}")
    if uid_real_bridge and not uid_real:
        logger.info(f"🌉 [RETRO-VÍNCULO bridge] uid_temp={uid} → uid_real={uid_real_bridge}")
        vincular_tracking_por_uid_temp(int(uid_real_bridge), uid)
        enviar_lead_capi(int(uid_real_bridge), "retro_bridge")

    return jsonify({"status": "ok", "uid": uid}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        if data:
            update = Update.de_json(data, application.bot)
            asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
        return "ok", 200
    except Exception:
        logger.exception("❌ Erro Webhook")
        return "error", 500

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data    = request.get_json() or {}
    evento  = data.get("event")
    uid     = data.get("customer", {}).get("chat_id")
    val_raw = data.get("transaction", {}).get("plan_value", 0)

    logger.info(f"[APEX WEBHOOK] Evento={evento} | chat_id={uid}")

    if not uid:
        return "ok", 200

    if evento == "user_joined":
        r.set(f"pending_join:{uid}", "1", ex=300)
        logger.info(f"🏁 [APEX JOINED] pending_join:{uid} criada — /start tem 90s")
        threading.Thread(target=apex_joined_fallback, args=(uid,), daemon=True).start()

    elif evento == "payment_created":
        enviar_initiatecheckout_capi(uid)

    elif evento == "payment_approved":
        try:
            valor = float(val_raw) / 100
        except Exception:
            valor = 0.0
        enviar_purchase_capi(uid, valor)

    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    try:
        async def s():
            await application.bot.set_webhook(url)
        asyncio.run_coroutine_threadsafe(s(), bot_loop).result()
        return f"✅ Webhook configurado: {url}", 200
    except Exception:
        logger.exception("Erro ao configurar webhook")
        return "error", 500

@app.route("/", methods=["GET"])
def home():
    return "Bot Online", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
