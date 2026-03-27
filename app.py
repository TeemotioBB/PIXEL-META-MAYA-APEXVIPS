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
        r = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        logger.info("✅ Redis conectado!")
    except Exception as e:
        logger.error(f"⚠️ Redis Offline: {e}")

# ====================== CAPI FUNCTIONS ======================

def enviar_lead_capi(uid: int, trigger: str):
    if not r: return
    try:
        redis_key = f"lead_sent:{uid}:{date.today()}"
        if not r.set(redis_key, "1", ex=86400, nx=True): return
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
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=5)
    except:
        pass

def enviar_purchase_capi(uid: int, valor: float, transaction_id: str):
    try:
        if r and not r.set(f"pur_sent:{transaction_id}", "1", ex=604800, nx=True): return
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
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=5)
    except:
        pass

# ====================== HANDLERS ======================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"👤 [BOT] /start de {uid}")
    await update.message.reply_text(
        "👋 Bem-vindo! Se você já realizou o pagamento, aguarde alguns instantes para a liberação do seu acesso."
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

# ====================== ENGINE ======================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CallbackQueryHandler(button_handler))

bot_loop = asyncio.new_event_loop()
bot_ready = threading.Event()  # Sinaliza quando o bot estiver pronto

def run_bot_init():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())
    bot_ready.set()  # Bot inicializado — libera as rotas
    logger.info("✅ Bot inicializado e loop rodando.")
    bot_loop.run_forever()  # Mantém o loop vivo para processar corrotinas

threading.Thread(target=run_bot_init, daemon=True).start()

# ====================== ROTAS FLASK ======================

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.json
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    except:
        pass
    return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json() or {}
    evento = data.get("event")
    uid = data.get("customer", {}).get("chat_id")
    transaction = data.get("transaction", {})
    t_id = transaction.get("id")
    val_raw = float(transaction.get("plan_value", 0)) / 100

    if not uid:
        return "ok", 200

    if evento == "payment_approved":
        enviar_purchase_capi(uid, val_raw, transaction_id=f"pur_{t_id}")

        async def send_msg():
            try:
                msg = "🚀 *Seu acesso VIP foi liberado!*\n\nClique no botão abaixo para entrar agora."
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("Entrar no VIP 💎", url="https://t.me/+SEU_LINK_AQUI")]])
                await application.bot.send_message(chat_id=uid, text=msg, reply_markup=kb, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Erro ao enviar msg VIP: {e}")

        asyncio.run_coroutine_threadsafe(send_msg(), bot_loop)

    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    # Aguarda o bot inicializar antes de continuar
    if not bot_ready.wait(timeout=20):
        return "❌ Bot ainda não inicializou. Aguarde e tente novamente.", 503

    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"

    async def setup():
        await application.bot.delete_webhook()
        return await application.bot.set_webhook(url=url)

    try:
        future = asyncio.run_coroutine_threadsafe(setup(), bot_loop)
        res = future.result(timeout=15)
        return f"✅ Webhook configurado com sucesso: {url} | Resultado: {res}", 200
    except Exception as e:
        return f"❌ Erro ao configurar webhook: {e}", 500

@app.route("/", methods=["GET"])
def home():
    status = "pronto" if bot_ready.is_set() else "inicializando..."
    return f"Bot Online! Status: {status}", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
