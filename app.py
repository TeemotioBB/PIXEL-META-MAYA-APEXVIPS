#!/usr/bin/env python3
import os
import logging
import hashlib
import time
import asyncio
import threading
import requests
import redis
import ast

from datetime import date
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes

# ====================== LOGGING ======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ====================== ENV VARS ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Redis conectado!")
except Exception as e:
    logger.error(f"❌ Erro Redis: {e}")
    raise

# ====================== USER DATA ======================
def montar_user_data(uid):
    user_data = {"external_id": [hash_data(str(uid))]}

    fbp     = r.get(f"fbp:{uid}")
    fbc_raw = r.get(f"fbc:{uid}")
    ip      = r.get(f"ip:{uid}")
    ua      = r.get(f"ua:{uid}")

    logger.info(f"[montar_user_data] UID {uid} → FBP: {bool(fbp)} | FBC: {bool(fbc_raw)} | IP: {bool(ip)} | UA: {bool(ua)}")

    if fbp:
        user_data["fbp"] = fbp
    if fbc_raw:
        user_data["fbc"] = fbc_raw
    if ip:
        user_data["client_ip_address"] = ip

    user_data["client_user_agent"] = ua if ua else "TelegramBot/1.0"

    return user_data

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": f"lead_{uid}_{date.today()}",  # mesmo event_id no mesmo dia = Meta deduplica
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"trigger": trigger}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🟢 [CAPI] Lead ENVIADO | UID: {uid} | Trigger: {trigger}")
    except Exception as e:
        logger.error(f"❌ Erro Lead: {e}")

def enviar_initiatecheckout_capi(uid: int):
    redis_key = f"checkout_sent:{uid}"
    if r.exists(redis_key):
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
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🔥 [CAPI] Checkout ENVIADO | UID: {uid}")
    except Exception as e:
        logger.error(f"❌ Erro Checkout: {e}")

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
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"💰 [CAPI] Purchase ENVIADO | UID: {uid} | R$ {valor:.2f}")
    except Exception as e:
        logger.error(f"❌ Erro Purchase: {e}")

# ====================== FALLBACK: envia Lead se /start nunca vier ======================
def apex_joined_fallback(uid: int):
    """
    NOVA LÓGICA — sem corrida de 30s.

    O /start agora é DONO do envio do Lead. Este fallback só dispara
    se o /start não aparecer em 90 segundos (ex: usuário entrou
    direto no bot sem usar o link de convite).

    A flag `pending_join:{uid}` é deletada pelo /start assim que ele
    envia o Lead. Se ainda existir depois de 90s, o /start não veio
    e enviamos sem tracking.
    """
    time.sleep(90)

    if r.get(f"pending_join:{uid}"):
        logger.warning(f"⚠️ [FALLBACK] /start não chegou em 90s para UID {uid} — enviando Lead sem tracking")
        r.delete(f"pending_join:{uid}")
        enviar_lead_capi(uid, "apex_joined_sem_start")
    else:
        logger.info(f"✅ [FALLBACK] UID {uid} já tratado pelo /start — fallback ignorado")

# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    args = context.args
    payload = args[0] if args else ""

    logger.info(f"🚀 [START] User: {uid} | Payload: '{payload}'")

    if payload.startswith("track_"):
        temp_key = f"tracking:{payload}"
        tracking_str = None

        # Tenta buscar 10 vezes (20 segundos no total)
        for tentativa in range(10):
            tracking_str = r.get(temp_key)
            if tracking_str:
                break
            logger.info(f"⏳ Tentativa {tentativa+1}/10: Aguardando tracking de {payload}...")
            await asyncio.sleep(2.0)

        if tracking_str:
            try:
                tracking_data = ast.literal_eval(tracking_str)

                if "fbp" in tracking_data:
                    r.set(f"fbp:{uid}", tracking_data["fbp"], ex=604800)
                if "fbc" in tracking_data:
                    r.set(f"fbc:{uid}", tracking_data["fbc"], ex=604800)
                if "client_ip" in tracking_data:
                    r.set(f"ip:{uid}", tracking_data["client_ip"], ex=604800)
                if "client_user_agent" in tracking_data:
                    r.set(f"ua:{uid}", tracking_data["client_user_agent"], ex=604800)

                r.expire(temp_key, 300)
                logger.info(f"✅ [SUCESSO] Dados vinculados para UID {uid} via payload {payload}")

            except Exception as e:
                logger.error(f"❌ Erro ao processar tracking data: {e}")

        else:
            # RETRO-VÍNCULO: guarda uid real para quando o tracking chegar depois
            logger.warning(f"⚠️ [AVISO] Payload {payload} não chegou em 20s — salvando pending_uid para retro-vínculo")
            r.set(f"pending_uid:{payload}", str(uid), ex=300)

    # ── ENVIO DO LEAD ──────────────────────────────────────────────────────────
    # O /start é SEMPRE responsável por enviar o Lead (com ou sem tracking).
    # Ele também cancela o fallback do apex_joined deletando a flag pending_join.
    # O event_id idêntico no mesmo dia garante deduplicação na Meta caso o
    # fallback já tenha disparado em paralelo.

    fbp_exists = bool(r.get(f"fbp:{uid}"))
    trigger = "start_com_tracking" if fbp_exists else "start_sem_tracking"

    enviar_lead_capi(uid, trigger)

    # Cancela o fallback do apex_joined (ele checa essa flag antes de enviar)
    r.delete(f"pending_join:{uid}")
    logger.info(f"🗑️ [START] pending_join:{uid} deletado — fallback cancelado")


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
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

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
        tracking_data["fbc"] = f"fb.1.{int(time.time() * 1000)}.{fbclid}"
    if client_ip:
        tracking_data["client_ip"] = client_ip.split(',')[0].strip()
    if client_user_agent:
        tracking_data["client_user_agent"] = client_user_agent

    r.set(f"tracking:{uid}", str(tracking_data), ex=86400)
    logger.info(f"💾 [TRACKING RECEBIDO E SALVO] uid={uid} | FBP: {bool(fbp)}")

    # RETRO-VÍNCULO: se o /start já desistiu de esperar, vincula agora e reenvia Lead
    uid_real = r.get(f"pending_uid:{uid}")
    if uid_real:
        logger.info(f"🔁 [RETRO-VÍNCULO] Tracking chegou tarde — vinculando para UID {uid_real}")
        if fbp:
            r.set(f"fbp:{uid_real}", fbp, ex=604800)
        if tracking_data.get("fbc"):
            r.set(f"fbc:{uid_real}", tracking_data["fbc"], ex=604800)
        if tracking_data.get("client_ip"):
            r.set(f"ip:{uid_real}", tracking_data["client_ip"], ex=604800)
        if client_user_agent:
            r.set(f"ua:{uid_real}", client_user_agent, ex=604800)
        enviar_lead_capi(int(uid_real), "retro_tracking")
        r.delete(f"pending_uid:{uid}")

    return jsonify({"status": "ok", "uid": uid}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        if data:
            update = Update.de_json(data, application.bot)
            asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
        return "ok", 200
    except Exception as e:
        logger.error(f"❌ Erro Webhook: {e}")
        return "error", 500

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data    = request.get_json() or {}
    evento  = data.get("event")
    uid     = data.get("customer", {}).get("chat_id")
    val_raw = data.get("transaction", {}).get("plan_value", 0)

    logger.info(f"[APEX WEBHOOK] Payload completo: {data}")
    logger.info(f"[APEX WEBHOOK] Evento={evento} | chat_id={uid}")

    if not uid:
        return "ok", 200

    if evento == "user_joined":
        # ─────────────────────────────────────────────────────────────────────
        # NOVA LÓGICA:
        #   1. Salva flag pending_join:{uid}
        #   2. Inicia thread de fallback (90s)
        #   3. O /start vai deletar a flag e enviar o Lead com tracking
        #   4. Se o /start não vier em 90s, o fallback envia sem tracking
        # ─────────────────────────────────────────────────────────────────────
        r.set(f"pending_join:{uid}", "1", ex=300)  # 5 min de segurança
        logger.info(f"🏁 [APEX JOINED] Flag pending_join:{uid} criada — aguardando /start por 90s")
        threading.Thread(target=apex_joined_fallback, args=(uid,), daemon=True).start()

    elif evento == "payment_created":
        logger.info(f"[PAYMENT CREATED] UID={uid} → enviando InitiateCheckout")
        enviar_initiatecheckout_capi(uid)

    elif evento == "payment_approved":
        valor = float(val_raw) / 100
        logger.info(f"[PAYMENT APPROVED] UID={uid} | valor R${valor:.2f} → enviando Purchase")
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
    except Exception as e:
        return str(e), 500

@app.route("/", methods=["GET"])
def home():
    return "Bot Online", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
