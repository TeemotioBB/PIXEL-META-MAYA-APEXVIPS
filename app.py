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

# ====================== FUNÇÃO AUXILIAR ======================
def build_user_data(uid: int, user_info: dict = None) -> dict:
    """Monta o payload de user_data com parâmetros aceitos pela Meta."""
    user_data = {"external_id": [hash_data(str(uid))]}
    if user_info:
        if user_info.get("ip"): user_data["client_ip_address"] = user_info["ip"]
        if user_info.get("ua"): user_data["client_user_agent"] = user_info["ua"]
        if user_info.get("fbp"): user_data["fbp"] = user_info["fbp"]
        if user_info.get("fbc"): user_data["fbc"] = user_info["fbc"]
    return user_data

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str, user_info: dict = None):
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if r.exists(redis_key):
        logger.info(f"⚠️ [CAPI] Lead para {uid} já enviado hoje (Filtro Ativo).")
        return
    r.set(redis_key, "1", ex=86400)
    
    payload = {
        "data": [{
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": f"lead_{uid}_{date.today()}",
            "action_source": "chat",
            "user_data": build_user_data(uid, user_info),
            "custom_data": {"trigger": trigger}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🟢 [CAPI] Lead ENVIADO | UID: {uid} | Trigger: {trigger}")
    except Exception as e: logger.error(f"❌ Erro Lead: {e}")

def enviar_initiatecheckout_capi(uid: int, user_info: dict = None, eid: str = None):
    redis_key = f"checkout_sent:{uid}"
    if r.exists(redis_key):
        logger.info(f"⚠️ [CAPI] Checkout para {uid} ignorado (Já enviado na última 1h).")
        return
    r.set(redis_key, "1", ex=3600)
    
    # Se houver ID de transação da Apex, usa ele para desduplicar
    event_id = eid if eid else f"init_{uid}_{int(time.time())}"
    
    payload = {
        "data": [{
            "event_name": "InitiateCheckout",
            "event_time": int(time.time()),
            "event_id": event_id,
            "action_source": "chat",
            "user_data": build_user_data(uid, user_info),
            "custom_data": {"currency": "BRL", "value": 0.00}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🔥 [CAPI] Checkout ENVIADO | UID: {uid} | EID: {event_id}")
    except Exception as e: logger.error(f"❌ Erro Checkout: {e}")

def enviar_purchase_capi(uid: int, valor: float, user_info: dict = None, eid: str = None):
    # OBRIGATÓRIO usar o ID da transação da Apex se ele existir para não duplicar
    event_id = eid if eid else f"pur_{uid}_{int(time.time())}"
    
    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": event_id,
            "action_source": "chat",
            "user_data": build_user_data(uid, user_info),
            "custom_data": {"value": valor, "currency": "BRL"}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"💰 [CAPI] Purchase ENVIADO | UID: {uid} | R$ {valor} | EID: {event_id}")
    except Exception as e: logger.error(f"❌ Erro Purchase: {e}")

# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"👤 [BOT] /start recebido do UID: {uid}")
    enviar_lead_capi(uid, "start")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    cb_data = (query.data or "").lower()
    logger.info(f"🖱️ [BOT] Clique detectado: {cb_data}")
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
    
    customer = data.get("customer", {})
    transaction = data.get("transaction", {})
    
    uid = customer.get("chat_id")
    # Captura o ID real da transação para desduplicação na Meta
    transaction_id = str(transaction.get("id", ""))
    val_raw = transaction.get("plan_value", 0)
    
    if not uid: return "ok", 200

    user_info = {
        "ip": customer.get("client_ip") or customer.get("ip"),
        "ua": customer.get("user_agent"),
        "fbp": customer.get("fbp") or data.get("metadata", {}).get("fbp"),
        "fbc": customer.get("fbc") or data.get("metadata", {}).get("fbc")
    }

    if evento == "user_joined": 
        enviar_lead_capi(uid, "apex_joined", user_info)
    elif evento == "payment_created": 
        enviar_initiatecheckout_capi(uid, user_info, eid=transaction_id)
    elif evento == "payment_approved": 
        enviar_purchase_capi(uid, float(val_raw)/100, user_info, eid=transaction_id)
        
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
