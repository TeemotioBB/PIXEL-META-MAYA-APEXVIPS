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
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

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

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if r.exists(redis_key):
        return
    r.set(redis_key, "1", ex=86400)
    payload = {
        "data": [{
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": f"lead_{uid}_{date.today()}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"trigger": trigger}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🟢 [CAPI] Lead ENVIADO | UID: {uid} | Trigger: {trigger}")
    except Exception as e: logger.error(f"❌ Erro Lead: {e}")

def enviar_initiatecheckout_capi(uid: int):
    # Trava de 1 hora para não repetir o checkout do mesmo usuário
    redis_key = f"checkout_sent:{uid}"
    if r.exists(redis_key):
        logger.info(f"⚠️ [CAPI] Checkout para {uid} ignorado (Já enviado recentemente).")
        return
    r.set(redis_key, "1", ex=3600)
    payload = {
        "data": [{
            "event_name": "InitiateCheckout",
            "event_time": int(time.time()),
            "event_id": f"init_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"currency": "BRL", "value": 12.90} 
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🔥 [CAPI] Checkout ENVIADO | UID: {uid}")
    except Exception as e: logger.error(f"❌ Erro Checkout: {e}")

def enviar_purchase_capi(uid: int, valor: float):
    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": f"pur_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"value": valor, "currency": "BRL"}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"💰 [CAPI] Purchase ENVIADO | UID: {uid} | R$ {valor}")
    except Exception as e: logger.error(f"❌ Erro Purchase: {e}")

# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"👤 [BOT] /start recebido do UID: {uid}")
    enviar_lead_capi(uid, "start")
    await update.message.reply_text("👋 Bem-vindo! Se você já realizou o pagamento, aguarde alguns instantes para a liberação do seu acesso.")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    cb_data = (query.data or "").lower()
    
    # Registra que o usuário interagiu (Lead)
    enviar_lead_capi(uid, f"btn_{cb_data}")
    
    # SE O BOTÃO TIVER PALAVRAS DE COMPRA, MANDA CHECKOUT
    if any(x in cb_data for x in ["plan", "buy", "pix", "pay", "assinar", "comprar"]):
        enviar_initiatecheckout_capi(uid)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").lower()
    enviar_lead_capi(uid, "chat_msg")

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
    transaction = data.get("transaction", {})
    val_raw = transaction.get("plan_value", 0)

    if not uid: return "ok", 200

    if evento == "user_joined": 
        enviar_lead_capi(uid, "apex_joined")
    elif evento == "payment_created": 
        # Rastreia o checkout assim que o PIX/Boleto é gerado no sistema
        enviar_initiatecheckout_capi(uid)
    elif evento == "payment_approved": 
        # Rastreia a venda finalizada
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
def home(): return "Bot Online e Rastreando!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
