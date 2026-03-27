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

# ====================== CONFIGURAÇÃO ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
WEBHOOK_BASE_URL = "https://pixel-meta-maya-apexvips-production.up.railway.app"
REDIS_URL = os.getenv("REDIS_URL")
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
        r.ping()
        logger.info("✅ Redis conectado!")
    except Exception as e:
        logger.error(f"⚠️ Redis Offline: {e}")

# ====================== CAPI ======================
def enviar_evento_capi(uid: int, event_name: str, custom_data=None, event_id=None):
    logger.info(f"📡 [CAPI] INICIANDO envio de {event_name} para UID {uid}")
    try:
        payload = {
            "data": [{
                "event_name": event_name,
                "event_time": int(time.time()),
                "event_id": event_id or f"{event_name.lower()}_{uid}_{int(time.time())}",
                "action_source": "chat",
                "user_data": {"external_id": [hash_data(str(uid))]},
                "custom_data": custom_data or {}
            }],
            "access_token": ACCESS_TOKEN
        }
        url = f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events"
        resp = requests.post(url, json=payload, timeout=15)
        logger.info(f"✅ [CAPI RESPONSE] {event_name} → {resp.status_code}")
    except Exception as e:
        logger.error(f"❌ [CAPI ERROR] {event_name}: {e}")

# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"🚀 [BOT] /start de {uid}")
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if r and not r.exists(redis_key):
        if r: r.set(redis_key, "1", ex=86400)
        threading.Thread(target=enviar_evento_capi, args=(uid, "Lead")).start()

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

# ====================== FLASK APP ======================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CallbackQueryHandler(button_handler))

bot_loop = asyncio.new_event_loop()

def run_bot():
    asyncio.set_event_loop(bot_loop)
    try:
        bot_loop.run_until_complete(application.initialize())
        bot_loop.run_until_complete(application.start())
        logger.info("✅ Bot Telegram inicializado com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar o bot: {e}")

# Inicia o bot em background
threading.Thread(target=run_bot, daemon=True).start()

# ====================== ROTAS ======================
@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json(silent=True)
    if data:
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json(silent=True) or {}
    logger.info(f"📥 [APEX WEBHOOK] Recebido | IP: {request.remote_addr} | Dados: {data}")

    evento = data.get("event")
    customer = data.get("customer") or {}
    uid = customer.get("chat_id") if isinstance(customer, dict) else None

    if not uid:
        logger.warning("⚠️ Apex webhook sem chat_id")
        return "ok", 200

    logger.info(f"🔄 Processando evento Apex: {evento} | UID: {uid}")

    if evento in ["checkout_created", "bill_created", "checkout_initiated"]:
        threading.Thread(target=enviar_evento_capi, args=(uid, "InitiateCheckout")).start()

    elif evento == "payment_approved":
        transaction = data.get("transaction", {})
        t_id = transaction.get("id")
        try:
            val = float(transaction.get("plan_value") or 0) / 100
        except:
            val = 0.0

        threading.Thread(target=enviar_evento_capi, args=(uid, "Purchase", {"value": val, "currency": "BRL"}, f"pur_{t_id}" if t_id else None)).start()

        # Envio da mensagem VIP
        async def send_vip():
            try:
                msg = "🚀 *Seu acesso VIP foi liberado!*\n\nClique abaixo para entrar."
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("Entrar no VIP 💎", url="https://t.me/+SEU_LINK_AQUI")]])
                await application.bot.send_message(chat_id=uid, text=msg, reply_markup=kb, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Erro mensagem VIP: {e}")
        asyncio.run_coroutine_threadsafe(send_vip(), bot_loop)

    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    logger.info("🔧 Tentativa de configurar webhook do Telegram...")
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    try:
        # Timeout curto para não travar
        future = asyncio.run_coroutine_threadsafe(application.bot.set_webhook(url=url), bot_loop)
        result = future.result(timeout=15)
        logger.info(f"✅ Webhook configurado com sucesso: {result}")
        return f"✅ Webhook configurado: {result}", 200
    except Exception as e:
        logger.error(f"❌ Erro ao configurar webhook: {e}")
        return f"❌ Erro: {e}", 500

@app.route("/")
def home():
    return "Bot Online ✅", 200

if __name__ == "__main__":
    # Força uso do servidor Flask (mais estável no Railway para esse caso)
    port = int(os.getenv("PORT", 8080))
    logger.info(f"🚀 Iniciando servidor Flask na porta {port}")
    app.run(host="0.0.0.0", port=port)
