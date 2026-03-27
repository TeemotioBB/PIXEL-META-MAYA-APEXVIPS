#!/usr/bin/env python3
import os
import logging
import hashlib
import time
import asyncio
import threading
import requests
import redis
from datetime import date
from flask import Flask, request
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
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

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

def montar_user_data(uid):
    """Busca dados no Redis e monta o dicionário user_data para a Meta"""
    user_data = {"external_id": [hash_data(str(uid))]}

    fbp = r.get(f"fbp:{uid}")
    fbc_raw = r.get(f"fbclid:{uid}")

    logger.info(f"[montar_user_data] UID {uid} → FBP encontrado: {bool(fbp)} | FBC encontrado: {bool(fbc_raw)}")

    if fbp:
        user_data["fbp"] = [fbp]
        logger.info(f"[montar_user_data] FBP incluído → {fbp[:50]}...")

    if fbc_raw:
        # Formata corretamente conforme documentação oficial do Meta
        creation_time = int(time.time() * 1000)  # milissegundos
        fbc_formatted = f"fb.1.{creation_time}.{fbc_raw}"
        user_data["fbc"] = [fbc_formatted]
        logger.info(f"[montar_user_data] FBC formatado e incluído → {fbc_formatted[:100]}...")

    try:
        ua = request.headers.get('User-Agent', 'TelegramBot/1.0')
    except Exception:
        ua = 'TelegramBot/1.0'

    user_data["client_user_agent"] = [ua]

    return user_data

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if r.exists(redis_key):
        return
    r.set(redis_key, "1", ex=86400)

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
    except Exception as e: logger.error(f"❌ Erro Lead: {e}")

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
        logger.info(f"🔥 [CAPI] Checkout ENVIADO | UID: {uid} | Tracking: {'fbc' in user_data}")
    except Exception as e: logger.error(f"❌ Erro Checkout: {e}")

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
    except Exception as e: logger.error(f"❌ Erro Purchase: {e}")

# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    args = context.args
    payload = " ".join(args) if args else ""

    logger.info(f"[START] UID: {uid} | Payload recebido: {payload}")

    saved_fbp = False
    saved_fbc = False

    if payload:
        try:
            # Parsing mais robusto (aceita fbp_ e fbc_)
            if "fbp_" in payload:
                fbp_part = payload.split("fbp_")[1]
                fbp_val = fbp_part.split("_fbc_")[0] if "_fbc_" in fbp_part else fbp_part
                r.set(f"fbp:{uid}", fbp_val.strip(), ex=259200)
                logger.info(f"✅ FBP salvo para UID {uid} | {fbp_val[:60]}...")
                saved_fbp = True

            if "fbc_" in payload:
                fbc_val = payload.split("fbc_")[-1].strip()
                r.set(f"fbclid:{uid}", fbc_val, ex=259200)
                logger.info(f"✅ FBC salvo (raw) para UID {uid} | {fbc_val[:80]}...")
                saved_fbc = True

        except Exception as e:
            logger.error(f"❌ Erro ao processar parâmetros do start: {e}")

    # Delay pequeno para Redis processar
    await asyncio.sleep(0.4)

    enviar_lead_capi(uid, "start")

    logger.info(f"[START] Finalizado UID {uid} | FBP salvo: {saved_fbp} | FBC salvo: {saved_fbc}")

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
    bot_loop.run_complete = application.start()
    bot_loop.run_forever()

threading.Thread(target=run_bot, daemon=True).start()

# ====================== ROUTES ======================
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
    if evento == "user_joined": enviar_lead_capi(uid, "apex_joined")
    elif evento == "payment_created": enviar_initiatecheckout_capi(uid)
    elif evento == "payment_approved": enviar_purchase_capi(uid, float(val_raw)/100)
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
def home(): return "Bot Online", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
