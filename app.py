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
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

# ====================== CONFIGURAÇÕES DE LOGGING ======================
# Forçamos o flush para garantir que o Railway mostre o log em tempo real
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True 
)
logger = logging.getLogger(__name__)

# Variáveis de Ambiente
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# Conexão Redis com Log de Verificação
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Conectado ao Redis com sucesso.")
except Exception as e:
    logger.error(f"❌ Erro Crítico no Redis: {e}")
    # Não levantamos Exception aqui para o Flask não crashar no boot, 
    # mas o app terá problemas se o Redis estiver offline.

# ====================== LOGICA DE RASTREIO CAPI ======================

def enviar_evento_meta(uid: int, event_name: str, event_id: str, value: float = 0.0, trigger: str = None):
    """Envia o evento para a API de Conversões da Meta com debug detalhado."""
    user_data = {"external_id": [hash_data(str(uid))]}
    custom_data = {"currency": "BRL", "value": value}
    if trigger:
        custom_data["trigger"] = trigger

    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(time.time()),
            "event_id": event_id,
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": custom_data
        }],
        "access_token": ACCESS_TOKEN
    }
    
    try:
        url = f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events"
        response = requests.post(url, json=payload, timeout=10)
        res_data = response.json()
        
        if response.status_code == 200:
            logger.info(f"🚀 [META CAPI] Sucesso: {event_name} | UID: {uid} | ID: {event_id}")
        else:
            logger.warning(f"⚠️ [META CAPI] Erro da API ({response.status_code}): {res_data}")
    except Exception as e:
        logger.error(f"❌ [META CAPI] Erro de conexão: {e}")

# ====================== TRATAMENTO DE CLIQUE ======================

async def silent_button_tracker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    cb_data = (query.data or "").lower()
    
    await query.answer()
    logger.info(f"🖱️ Clique detectado: {cb_data} de UID: {uid}")

    if any(x in cb_data for x in ["precos", "planos", "saber_mais"]):
        key = f"lead_sent:{uid}:{date.today()}"
        if r.set(key, "1", ex=86400, nx=True):
            enviar_evento_meta(uid, "Lead", f"lead_{uid}_{date.today()}", trigger=f"btn_{cb_data}")

# ====================== ENGINE & WEBHOOKS ======================

app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CallbackQueryHandler(silent_button_tracker))

bot_loop = asyncio.new_event_loop()

def run_bot():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    # Note: application.start() não é necessário para webhooks puros, mas initialize() sim.
    logger.info("🤖 Thread do Bot Telegram inicializada.")
    bot_loop.run_forever()

threading.Thread(target=run_bot, daemon=True).start()

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    data = request.json
    if not data: 
        return "No data", 400
    
    upd_id = data.get("update_id")
    if upd_id and not r.set(f"proc_upd:{upd_id}", "1", ex=3600, nx=True):
        return "Duplicate", 200

    update = Update.de_json(data, application.bot)
    # Encaminha o processamento para a thread do bot
    bot_loop.call_soon_threadsafe(asyncio.create_task, application.process_update(update))
    return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json() or {}
    evento = data.get("event")
    customer = data.get("customer", {})
    uid = customer.get("chat_id")
    transaction = data.get("transaction", {})
    t_id = transaction.get("id")
    
    # Previne erros de conversão se o valor vier vazio
    try:
        valor = float(transaction.get("plan_value", 0)) / 100
    except:
        valor = 0.0

    logger.info(f"📩 Webhook Apex recebido: {evento} para UID: {uid}")

    if not uid or not t_id: 
        return "Missing data", 200

    if evento == "user_joined":
        if r.set(f"lead_sent:{uid}:{date.today()}", "1", ex=86400, nx=True):
            enviar_evento_meta(uid, "Lead", f"lead_{uid}_{date.today()}", trigger="apex_joined")

    elif evento == "payment_created":
        if r.set(f"check_sent:{t_id}", "1", ex=600, nx=True):
            enviar_evento_meta(uid, "InitiateCheckout", f"init_{t_id}", value=valor)

    elif evento == "payment_approved":
        if r.set(f"pur_sent:{t_id}", "1", ex=604800, nx=True):
            enviar_evento_meta(uid, "Purchase", f"pur_{t_id}", value=valor)

    return "ok", 200

@app.route("/", methods=["GET"])
def home():
    return {"status": "online", "service": "CAPI Tracker"}, 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    if not WEBHOOK_BASE_URL:
        return "Erro: WEBHOOK_BASE_URL não configurada", 500
        
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    
    async def setup():
        return await application.bot.set_webhook(url, drop_pending_updates=True)
    
    future = asyncio.run_coroutine_threadsafe(setup(), bot_loop)
    try:
        success = future.result(timeout=10)
        if success:
            return f"✅ Webhook configurado: {url}", 200
        return "❌ Falha ao configurar webhook", 500
    except Exception as e:
        return f"❌ Erro: {str(e)}", 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    logger.info(f"🌐 Servidor Flask iniciando na porta {port}")
    app.run(host="0.0.0.0", port=port)
