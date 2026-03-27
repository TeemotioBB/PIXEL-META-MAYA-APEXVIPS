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

# ====================== CAPI CENTRALIZADA ======================
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
        resp = requests.post(url, json=payload, timeout=10)
        
        logger.info(f"✅ [CAPI RESPONSE] {event_name} → Status: {resp.status_code} | Resposta: {resp.text[:300]}")
    except Exception as e:
        logger.error(f"❌ [CAPI ERROR] {event_name} para {uid}: {e}")

# ====================== HANDLERS TELEGRAM ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"🚀 [BOT] /start recebido de {uid}")
   
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if r and not r.exists(redis_key):
        if r:
            r.set(redis_key, "1", ex=86400)
        threading.Thread(target=enviar_evento_capi, args=(uid, "Lead")).start()
        logger.info(f"📤 Lead enviado via CAPI para {uid}")
    else:
        logger.info(f"⏭️ Lead já enviado hoje para {uid}")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

# ====================== ENGINE ======================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CallbackQueryHandler(button_handler))

bot_loop = asyncio.new_event_loop()
bot_ready = threading.Event()

def run_bot_init():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())
    bot_ready.set()
    logger.info("✅ Bot pronto e rodando.")

threading.Thread(target=run_bot_init, daemon=True).start()

# ====================== ROTAS ======================
@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json()
    if data:
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json(silent=True) or {}
    
    # LOG PRINCIPAL - Sempre vai aparecer quando a Apex chamar
    logger.info(f"📥 [APEX WEBHOOK] Requisição recebida | IP: {request.remote_addr} | Dados completos: {data}")

    evento = data.get("event")
    customer = data.get("customer") or {}
    uid = customer.get("chat_id") if isinstance(customer, dict) else None

    if not uid:
        logger.warning(f"⚠️ [APEX] Webhook recebido SEM chat_id | Evento: {evento}")
        return "ok", 200

    logger.info(f"🔄 [APEX] Processando evento: '{evento}' | UID: {uid}")

    # ==================== ENVIO DOS EVENTOS ====================

    if evento in ["checkout_created", "bill_created", "checkout_initiated"]:
        threading.Thread(target=enviar_evento_capi, args=(uid, "InitiateCheckout")).start()

    elif evento == "payment_approved":
        transaction = data.get("transaction", {})
        t_id = transaction.get("id")
        try:
            val = float(transaction.get("plan_value") or 0) / 100
        except:
            val = 0.0

        threading.Thread(
            target=enviar_evento_capi,
            args=(uid, "Purchase", {"value": val, "currency": "BRL"}, f"pur_{t_id}" if t_id else None)
        ).start()

        # Envio da mensagem VIP
        async def send_vip():
            try:
                msg = "🚀 *Seu acesso VIP foi liberado!*\n\nClique abaixo para entrar."
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("Entrar no VIP 💎", url="https://t.me/+SEU_LINK_AQUI")]])
                await application.bot.send_message(chat_id=uid, text=msg, reply_markup=kb, parse_mode="Markdown")
                logger.info(f"✅ Mensagem VIP enviada para {uid}")
            except Exception as e:
                logger.error(f"❌ Erro ao enviar VIP para {uid}: {e}")

        asyncio.run_coroutine_threadsafe(send_vip(), bot_loop)

    else:
        logger.info(f"ℹ️ [APEX] Evento desconhecido ou ignorado: '{evento}'")

    return "ok", 200


@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    if not bot_ready.wait(timeout=20):
        return "Bot não pronto", 503
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    async def setup(): 
        return await application.bot.set_webhook(url=url)
    res = asyncio.run_coroutine_threadsafe(setup(), bot_loop).result()
    return f"Webhook: {res}", 200


@app.route("/")
def home():
    return "Bot Online ✅", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
