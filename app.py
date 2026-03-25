#!/usr/bin/env python3
"""
APEX VIPS BOT - META CAPI (CORRIGIDO E OTIMIZADO)
"""

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
from telegram.ext import (
    Application, 
    MessageHandler, 
    CommandHandler, 
    CallbackQueryHandler, 
    filters, 
    ContextTypes
)

# ====================== LOGGING ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ====================== ENV VARS ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")

# ====================== META CAPI CONFIG ======================
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS CONNECTION ======================
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Redis conectado com sucesso!")
except Exception as e:
    logger.error(f"❌ Falha ao conectar no Redis: {e}")
    raise

# ====================== FLASK SETUP ======================
app = Flask(__name__)

# ====================== CAPI FUNCTIONS (CORE) ======================

def enviar_lead_capi(uid: int, trigger: str):
    """Envia Lead com trava de 24h para evitar duplicidade"""
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
            "custom_data": {"trigger": trigger, "bot_type": "apex_vips"}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🟢 [CAPI] Lead enviado | UID: {uid}")
    except Exception as e:
        logger.error(f"❌ Erro Lead CAPI: {e}")

def enviar_initiatecheckout_capi(uid: int):
    """Envia Checkout com trava de 1h para evitar 'clicadores' repetidos"""
    redis_key = f"checkout_sent:{uid}"
    if r.exists(redis_key):
        logger.info(f"🟡 [CAPI] Checkout já enviado recentemente para {uid}. Pulando.")
        return

    r.set(redis_key, "1", ex=3600)

    payload = {
        "data": [{
            "event_name": "InitiateCheckout",
            "event_time": int(time.time()),
            "event_id": f"initiate_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"currency": "BRL", "value": 12.90}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🔥 [CAPI] InitiateCheckout enviado | UID: {uid}")
    except Exception as e:
        logger.error(f"❌ Erro Checkout CAPI: {e}")

def enviar_purchase_capi(uid: int, valor_venda: float):
    """Envia Compra Real (Purchase) - Sem travas para permitir múltiplas compras"""
    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": f"pur_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"value": valor_venda, "currency": "BRL"}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        resp = requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"💰 [CAPI] Purchase enviado! UID: {uid} | R$ {valor_venda} | Status: {resp.status_code}")
    except Exception as e:
        logger.error(f"❌ Erro Purchase CAPI: {e}")

# ====================== TELEGRAM HANDLERS ======================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    enviar_lead_capi(uid, "start")

async def button_click_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    enviar_lead_capi(uid, "button_click")
    
    cb_data = (query.data or "").lower()
    payment_identifiers = ["plan", "buy", "pix", "pay", "assin", "checkout"]
    if any(idf in cb_data for idf in payment_identifiers):
        enviar_initiatecheckout_capi(uid)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").lower()
    enviar_lead_capi(uid, "chat_interaction")
    
    if "pagar com pix" in text or "plano selecionado" in text:
        enviar_initiatecheckout_capi(uid)

# ====================== BOT ENGINE & THREADING ======================

application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CallbackQueryHandler(button_click_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

bot_loop = asyncio.new_event_loop()

def start_bot_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_bot_loop, args=(bot_loop,), daemon=True).start()

# Inicialização assíncrona do Bot
asyncio.run_coroutine_threadsafe(application.initialize(), bot_loop).result()
asyncio.run_coroutine_threadsafe(application.start(), bot_loop).result()

# ====================== FLASK ROUTES ======================

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
        return "ok", 200
    except Exception as e:
        return "error", 500

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json()
    if not data: return "ok", 200
    
    evento = data.get("event")
    uid = data.get("customer", {}).get("chat_id")
    val_raw = data.get("transaction", {}).get("plan_value", 0)
    valor_real = float(val_raw) / 100

    if not uid: return "ok", 200

    if evento == "user_joined":
        enviar_lead_capi(uid, "apex_joined")
    elif evento == "payment_created":
        enviar_initiatecheckout_capi(uid)
    elif evento == "payment_approved":
        enviar_purchase_capi(uid, valor_real)

    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    if not WEBHOOK_BASE_URL: return "Erro URL", 400
    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/webhook"
    try:
        async def setup():
            await application.bot.set_webhook(webhook_url)
        asyncio.run(setup())
        return f"✅ Webhook configurado: {webhook_url}", 200
    except Exception as e:
        return f"❌ Erro: {e}", 500

@app.route("/", methods=["GET"])
def home():
    return "ApexVips Bot online", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
