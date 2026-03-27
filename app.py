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
        logger.info("✅ Redis conectado com sucesso!")
    except Exception as e:
        logger.error(f"⚠️ Redis Offline: {e}")

# ====================== ENGINE CAPI (CENTRAL DE LOGS) ======================

def enviar_evento_capi(uid: int, event_name: str, custom_data=None, event_id=None):
    """
    Envia qualquer evento para o Meta e gera logs detalhados no Railway.
    """
    logger.info(f"📡 [CAPI] Iniciando tentativa de envio: {event_name} para o usuário {uid}")
    
    try:
        # 1. Montagem do Payload
        payload = {
            "data": [{
                "event_name": event_name,
                "event_time": int(time.time()),
                "event_id": event_id or f"{event_name.lower()}_{uid}_{int(time.time())}",
                "action_source": "chat",
                "user_data": {
                    "external_id": [hash_data(str(uid))]
                },
                "custom_data": custom_data or {}
            }],
            "access_token": ACCESS_TOKEN
        }
        
        # 2. Envio para o Meta
        url = f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events"
        resp = requests.post(url, json=payload, timeout=10)
        
        # 3. Logs de Resposta
        if resp.status_code == 200:
            logger.info(f"✅ [CAPI SUCCESS] Evento '{event_name}' entregue ao Meta. Status: 200 OK")
        else:
            logger.error(f"❌ [CAPI ERROR] Meta recusou o evento '{event_name}'. Status: {resp.status_code} | Resposta: {resp.text}")
            
    except Exception as e:
        logger.error(f"💥 [CAPI CRITICAL] Falha técnica ao conectar com o Meta ({event_name}): {e}")

# ====================== HANDLERS TELEGRAM ======================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"🚀 [BOT] Recebido /start do usuário {uid} (Rastreio Silencioso)")

    # --- EVENTO 1: LEAD (Com trava de 24h no Redis) ---
    redis_key = f"lead_sent:{uid}:{date.today()}"
    if r and r.exists(redis_key):
        logger.info(f"⏭️ [SKIP] Lead para {uid} já enviado hoje. Ignorando CAPI.")
    else:
        if r: r.set(redis_key, "1", ex=86400)
        threading.Thread(target=enviar_evento_capi, args=(uid, "Lead")).start()

    # --- EVENTO 2: INITIATE CHECKOUT (Sempre que iniciar o bot) ---
    threading.Thread(target=enviar_evento_capi, args=(uid, "InitiateCheckout")).start()

    # A linha de reply_text foi removida para o bot não interferir na experiência do usuário.

# ====================== ROTAS FLASK ======================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start_handler))

bot_loop = asyncio.new_event_loop()
bot_ready = threading.Event()

def run_bot_init():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())
    bot_ready.set()
    logger.info("🤖 Bot (Telegram Engine) iniciado.")
    bot_loop.run_forever()

threading.Thread(target=run_bot_init, daemon=True).start()

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json()
        logger.info("📩 [WEBHOOK] Mensagem recebida do Telegram.")
        if data:
            update = Update.de_json(data, application.bot)
            asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    except Exception as e:
        logger.error(f"❌ [WEBHOOK ERROR] Erro no processamento: {e}")
    return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    """
    Recebe confirmação de pagamento da Apex e envia o evento Purchase (Compra).
    """
    data = request.get_json() or {}
    evento = data.get("event")
    uid = data.get("customer", {}).get("chat_id")
    transaction = data.get("transaction", {})
    
    logger.info(f"📢 [APEX WEBHOOK] Evento recebido: {evento} | Usuário: {uid}")

    if not uid:
        return "ok", 200

    if evento == "payment_approved":
        t_id = transaction.get("id")
        valor = float(transaction.get("plan_value") or 0) / 100
        
        # --- EVENTO 3: PURCHASE ---
        custom_data = {"value": valor, "currency": "BRL"}
        event_id = f"pur_{t_id}"
        
        threading.Thread(target=enviar_evento_capi, args=(uid, "Purchase", custom_data, event_id)).start()

        # Envio da mensagem VIP
        async def send_vip_msg():
            try:
                msg = "🚀 *Seu acesso VIP foi liberado!*\n\nClique no botão abaixo para entrar."
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("Entrar no VIP 💎", url="https://t.me/+SEU_LINK_AQUI")]])
                await application.bot.send_message(chat_id=uid, text=msg, reply_markup=kb, parse_mode="Markdown")
                logger.info(f"✅ [BOT] Mensagem VIP enviada para {uid}")
            except Exception as e:
                logger.error(f"❌ [BOT ERROR] Falha ao enviar link VIP: {e}")

        asyncio.run_coroutine_threadsafe(send_vip_msg(), bot_loop)

    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    if not bot_ready.wait(timeout=30):
        return "❌ [ERRO] Bot não inicializou.", 503
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    async def setup():
        await application.bot.delete_webhook()
        return await application.bot.set_webhook(url=url)
    try:
        res = asyncio.run_coroutine_threadsafe(setup(), bot_loop).result(timeout=20)
        return f"✅ Webhook configurado: {url} | Resultado: {res}", 200
    except Exception as e:
        return f"❌ Erro ao configurar: {e}", 500

@app.route("/", methods=["GET"])
def home():
    return f"Bot Online! Status: {'Pronto' if bot_ready.is_set() else 'Iniciando...'}", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
