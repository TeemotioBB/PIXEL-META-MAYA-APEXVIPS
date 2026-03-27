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

# ====================== ENV VARS (RECOMENDADO USAR .ENV) ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD")

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
    # Trava por 24h para evitar inflar CPL com o mesmo usuário
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if not r.set(redis_key, "1", ex=86400, nx=True):
        logger.info(f"⚠️ [CAPI] Lead {uid} já enviado hoje.")
        return

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
    requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
    logger.info(f"🟢 [CAPI] Lead Enviado: {uid}")

def enviar_initiatecheckout_capi(uid: int, valor: float = 0.0, transaction_id: str = None):
    # Se vier da Apex, usamos o ID deles. Se for clique no bot, geramos um ID temporário.
    t_id = transaction_id or f"init_{uid}_{int(time.time() / 600)}" # Agrupa por janelas de 10 min
    redis_key = f"checkout_sent:{t_id}"
    
    if not r.set(redis_key, "1", ex=600, nx=True):
        logger.info(f"⚠️ [CAPI] Checkout {t_id} já enviado recentemente.")
        return

    payload = {
        "data": [{
            "event_name": "InitiateCheckout",
            "event_time": int(time.time()),
            "event_id": t_id,
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"currency": "BRL", "value": valor}
        }],
        "access_token": ACCESS_TOKEN
    }
    requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
    logger.info(f"🔥 [CAPI] Checkout Enviado: {uid} | R$ {valor}")

def enviar_purchase_capi(uid: int, valor: float, transaction_id: str):
    # CRÍTICO: Trava por ID de transação real por 7 dias
    redis_key = f"pur_sent:{transaction_id}"
    if not r.set(redis_key, "1", ex=604800, nx=True):
        logger.info(f"⚠️ [CAPI] Compra {transaction_id} duplicada bloqueada.")
        return

    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": transaction_id, # Meta usa isso para deduplicar no servidor deles também
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"value": valor, "currency": "BRL"}
        }],
        "access_token": ACCESS_TOKEN
    }
    requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
    logger.info(f"💰 [CAPI] Purchase Enviado: {transaction_id} | R$ {valor}")

# ====================== HANDLERS ======================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"👤 [BOT] /start do UID: {uid}")
    # Removido lead automático aqui para evitar curiosos

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    cb_data = (query.data or "").lower()

    # 1. Filtro de Lead: Somente se clicar em botões de intenção real
    if any(x in cb_data for x in ["precos", "planos", "saber_mais", "como_funciona"]):
        enviar_lead_capi(uid, f"btn_{cb_data}")

    # 2. InitiateCheckout via Bot
    if any(x in cb_data for x in ["plan", "buy", "pix", "pay"]):
        # Se você souber o valor do plano pelo cb_data, pode passar aqui
        enviar_initiatecheckout_capi(uid)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Monitora palavras-chave, mas não envia Lead por qualquer "oi"
    text = (update.message.text or "").lower()
    if "valor" in text or "preço" in text or "como contrato" in text:
        enviar_lead_capi(update.effective_user.id, "msg_duvida_comercial")

# ====================== ENGINE & ROUTES ======================
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

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    data = request.json
    if data:
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json() or {}
    evento = data.get("event")
    customer = data.get("customer", {})
    uid = customer.get("chat_id")
    
    # Pegando IDs de transação e valores da Apex
    transaction = data.get("transaction", {})
    t_id = transaction.get("id") # ID Único da transação
    val_raw = transaction.get("plan_value", 0)
    
    if not uid or not t_id: return "ok", 200

    if evento == "user_joined":
        enviar_lead_capi(uid, "apex_joined")
    elif evento == "payment_created":
        enviar_initiatecheckout_capi(uid, valor=float(val_raw)/100, transaction_id=f"init_{t_id}")
    elif evento == "payment_approved":
        enviar_purchase_capi(uid, float(val_raw)/100, transaction_id=f"pur_{t_id}")
        
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
