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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

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

# ====================== REDIS (BLINDADO) ======================
r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        logger.info("✅ Redis conectado com sucesso!")
    except Exception as e:
        logger.error(f"⚠️ Erro Redis (O bot continuará sem cache): {e}")
        r = None

# ====================== CAPI FUNCTIONS ======================

def enviar_lead_capi(uid: int, trigger: str):
    if not r: return
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if not r.set(redis_key, "1", ex=86400, nx=True):
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

def enviar_initiatecheckout_capi(uid: int, valor: float = 0.0, transaction_id: str = None):
    if not r: return
    t_id = transaction_id or f"init_{uid}_{int(time.time() / 600)}"
    if not r.set(f"checkout_sent:{t_id}", "1", ex=600, nx=True):
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

def enviar_purchase_capi(uid: int, valor: float, transaction_id: str):
    if r and not r.set(f"pur_sent:{transaction_id}", "1", ex=604800, nx=True):
        return

    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": transaction_id,
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {"value": valor, "currency": "BRL"}
        }],
        "access_token": ACCESS_TOKEN
    }
    requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)

# ====================== HANDLERS ======================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"👤 [BOT] /start recebido: {uid}")
    await update.message.reply_text("Bem-vindo! Aguarde o processamento do seu acesso VIP.")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    cb_data = (query.data or "").lower()
    if any(x in cb_data for x in ["precos", "planos"]): enviar_lead_capi(uid, f"btn_{cb_data}")
    if any(x in cb_data for x in ["buy", "pix"]): enviar_initiatecheckout_capi(uid)

# ====================== ENGINE & FLASK ======================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CallbackQueryHandler(button_handler))

bot_loop = asyncio.new_event_loop()

def run_bot_init():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())

threading.Thread(target=run_bot_init, daemon=True).start()

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.json
        if not data: return "ok", 200
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
        return "ok", 200
    except Exception as e:
        logger.error(f"❌ Erro Webhook Telegram: {e}")
        return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json() or {}
    evento = data.get("event")
    uid = data.get("customer", {}).get("chat_id")
    transaction = data.get("transaction", {})
    t_id = transaction.get("id")
    val_raw = float(transaction.get("plan_value", 0)) / 100
    
    if not uid: return "ok", 200

    if evento == "user_joined":
        enviar_lead_capi(uid, "apex_joined")
    elif evento == "payment_created":
        enviar_initiatecheckout_capi(uid, valor=val_raw, transaction_id=f"init_{t_id}")
    elif evento == "payment_approved":
        enviar_purchase_capi(uid, val_raw, transaction_id=f"pur_{t_id}")
        
        async def send_msg():
            try:
                msg = "🚀 *Acesso Liberado!*\n\nClique abaixo para entrar no grupo VIP."
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("Entrar no VIP 💎", url="SEU_LINK_AQUI")]])
                await application.bot.send_message(chat_id=uid, text=msg, reply_markup=kb, parse_mode="Markdown")
            except Exception as e: logger.error(f"Erro envio: {e}")
        
        asyncio.run_coroutine_threadsafe(send_msg(), bot_loop)
    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    if not WEBHOOK_BASE_URL: return "ERRO: WEBHOOK_BASE_URL não definida nas variáveis!", 500
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    
    async def s(): return await application.bot.set_webhook(url)
    
    try:
        future = asyncio.run_coroutine_threadsafe(s(), bot_loop)
        res = future.result(timeout=10) # Timeout de 10s para não travar o navegador
        return f"✅ Webhook configurado com sucesso: {url}" if res else "❌ Falha no Telegram", 200
    except Exception as e:
        return f"❌ Erro ao configurar: {e}. Verifique se o TOKEN está correto nos logs.", 500

@app.route("/", methods=["GET"])
def home(): return "Bot Online e Rodando!", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
