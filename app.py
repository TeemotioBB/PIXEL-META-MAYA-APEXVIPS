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
from flask import Flask, request, redirect
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

# ====================== USER DATA (CAPI) ======================
def montar_user_data(uid):
    """Busca dados no Redis e monta o dicionário user_data para a Meta"""
    user_data = {"external_id": [hash_data(str(uid))]}

    # ✅ IMPORTANTE: As chaves devem bater com o que o start_handler salva
    fbp = r.get(f"fbp:{uid}")
    fbc_raw = r.get(f"fbclid:{uid}")

    logger.info(f"[montar_user_data] UID {uid} | FBP: {bool(fbp)} | FBC: {bool(fbc_raw)}")

    if fbp:
        user_data["fbp"] = [fbp]
    if fbc_raw:
        user_data["fbc"] = [fbc_raw]

    try:
        ua = request.headers.get('User-Agent', 'TelegramBot/1.0')
    except Exception:
        ua = 'TelegramBot/1.0'

    user_data["client_user_agent"] = [ua]
    return user_data

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": f"lead_{uid}_{date.today()}",
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"trigger": trigger}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🟢 [CAPI] Lead ENVIADO | UID: {uid} | Tracking: {'fbc' in user_data}")
    except Exception as e:
        logger.error(f"❌ Erro Lead: {e}")

def enviar_initiatecheckout_capi(uid: int):
    redis_key = f"checkout_sent:{uid}"
    if r.exists(redis_key): return
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
        logger.info(f"🔥 [CAPI] Checkout ENVIADO | UID: {uid} | Tracking: {'fbc' in user_data}")
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
        logger.info(f"💰 [CAPI] Purchase ENVIADO | UID: {uid} | R$ {valor:.2f} | Tracking: {'fbc' in user_data}")
    except Exception as e:
        logger.error(f"❌ Erro Purchase: {e}")

# ====================== BOT HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    args = context.args
    payload = args[0] if args else ""

    logger.info(f"[START] UID: {uid} | Payload: {payload}")

    # ✅ PROCESSO DE TRACKING: Vincula temp_key -> UID
    if payload.startswith("track_"):
        temp_key = payload
        tracking_str = r.get(f"tracking:{temp_key}")

        if tracking_str:
            try:
                tracking_data = ast.literal_eval(tracking_str)

                # Salva os dados vinculados ao UID do Telegram para uso futuro (Checkout/Purchase)
                if "fbp" in tracking_data:
                    r.set(f"fbp:{uid}", tracking_data["fbp"], ex=604800) # 7 dias
                    logger.info(f"✅ FBP vinculado ao UID {uid}")

                if "fbc" in tracking_data:
                    # ✅ SALVA COMO 'fbclid' para alinhar com o montar_user_data
                    r.set(f"fbclid:{uid}", tracking_data["fbc"], ex=604800)
                    logger.info(f"✅ FBC vinculado ao UID {uid}")

                # Deleta a chave temporária para limpar o Redis
                r.delete(f"tracking:{temp_key}")

            except Exception as e:
                logger.error(f"❌ Erro ao processar tracking data: {e}")
        else:
            logger.warning(f"[START] Tracking expirado ou não encontrado: {temp_key}")

    # Pequeno fôlego para o Redis consolidar antes de enviar o Lead
    await asyncio.sleep(0.5)
    enviar_lead_capi(uid, "start")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    cb_data = (query.data or "").lower()
    enviar_lead_capi(uid, "button_click")
    if any(x in cb_data for x in ["plan", "buy", "pix", "pay"]):
        enviar_initiatecheckout_capi(uid)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").lower()
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

# ====================== FLASK ROUTES ======================
@app.route('/apex-tracking', methods=['GET'])
def apex_tracking():
    fbclid = request.args.get('fbclid')
    fbp = request.args.get('fbp')

    logger.info(f"[TRACKING RECEBIDO] fbclid={fbclid} | fbp={fbp}")

    # Chave temporária que será passada via ?start=track_...
    temp_key = f"track_{int(time.time())}"
    tracking_data = {}

    if fbp:
        tracking_data["fbp"] = fbp

    if fbclid:
        creation_time = int(time.time() * 1000)
        # Formata o FBC no padrão oficial da Meta
        tracking_data["fbc"] = f"fb.1.{creation_time}.{fbclid}"

    # ✅ SALVA POR 3 DIAS (259200 segundos)
    if tracking_data:
        r.set(f"tracking:{temp_key}", str(tracking_data), ex=259200)
        logger.info(f"📡 Rastro temporário criado: {temp_key}")

    # Redireciona para o bot com o parâmetro de tracking
    bot_url = f"https://t.me/Mayaoficial_bot?start={temp_key}"
    return redirect(bot_url)

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
    data = request.get_json() or {}
    evento = data.get("event")
    uid = data.get("customer", {}).get("chat_id")
    val_raw = data.get("transaction", {}).get("plan_value", 0)
    
    if not uid: return "ok", 200
    
    if evento == "user_joined": 
        enviar_lead_capi(uid, "apex_joined")
    elif evento == "payment_created": 
        enviar_initiatecheckout_capi(uid)
    elif evento == "payment_approved": 
        enviar_purchase_capi(uid, float(val_raw)/100)
    
    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    try:
        async def s(): await application.bot.set_webhook(url)
        asyncio.run_coroutine_threadsafe(s(), bot_loop).result()
        return f"✅ Webhook configurado: {url}", 200
    except Exception as e: return str(e), 500

@app.route("/", methods=["GET"])
def home(): 
    return "Bot Online", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
