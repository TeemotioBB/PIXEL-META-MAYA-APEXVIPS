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

# ====================== CONFIGURAÇÕES ======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Conectado ao Redis para controle de rastreio.")
except Exception as e:
    logger.error(f"❌ Erro Redis: {e}")
    raise

# ====================== LOGICA DE RASTREIO CAPI ======================

def enviar_evento_meta(uid: int, event_name: str, event_id: str, value: float = 0.0, trigger: str = None):
    """Envia o evento para a API de Conversões da Meta."""
    # User Data básico (External ID é o mais forte no Telegram)
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
        requests.post(url, json=payload, timeout=10)
        logger.info(f"🚀 [META CAPI] Evento: {event_name} | UID: {uid} | ID: {event_id}")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar CAPI: {e}")

# ====================== TRATAMENTO DE CLIQUE (BOTÕES) ======================

async def silent_button_tracker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Apenas rastreia o clique sem enviar nenhuma mensagem de volta."""
    query = update.callback_query
    uid = query.from_user.id
    cb_data = (query.data or "").lower()
    
    # Avisa o Telegram que recebemos o clique (necessário para o botão parar de girar)
    await query.answer()

    # Rastreia apenas botões específicos de intenção
    if any(x in cb_data for x in ["precos", "planos", "saber_mais"]):
        # Trava de 24h para Lead não duplicar no mesmo dia
        if r.set(f"lead_sent:{uid}:{date.today()}", "1", ex=86400, nx=True):
            enviar_evento_meta(uid, "Lead", f"lead_{uid}_{date.today()}", trigger=f"btn_{cb_data}")

# ====================== WEBHOOKS (FLASK) ======================

app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CallbackQueryHandler(silent_button_tracker))

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    """Recebe dados do Telegram apenas para capturar cliques em botões."""
    data = request.json
    if not data: return "ok", 200
    
    # Evita processar o mesmo update duas vezes (Retries do Telegram)
    upd_id = data.get("update_id")
    if upd_id and not r.set(f"proc_upd:{upd_id}", "1", ex=86400, nx=True):
        return "ok", 200

    update = Update.de_json(data, application.bot)
    asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    return "ok", 200

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    """Recebe dados da Apex para InitiateCheckout e Purchase."""
    data = request.get_json() or {}
    evento = data.get("event")
    uid = data.get("customer", {}).get("chat_id")
    transaction = data.get("transaction", {})
    t_id = transaction.get("id")
    valor = float(transaction.get("plan_value", 0)) / 100

    if not uid or not t_id: return "ok", 200

    if evento == "user_joined":
        if r.set(f"lead_sent:{uid}:{date.today()}", "1", ex=86400, nx=True):
            enviar_evento_meta(uid, "Lead", f"lead_{uid}_{date.today()}", trigger="apex_joined")

    elif evento == "payment_created":
        # Trava de 10 min para não duplicar checkout se o cara gerar vários boletos
        if r.set(f"check_sent:{t_id}", "1", ex=600, nx=True):
            enviar_evento_meta(uid, "InitiateCheckout", f"init_{t_id}", value=valor)

    elif evento == "payment_approved":
        # Trava de 7 dias para Purchase (Segurança máxima)
        if r.set(f"pur_sent:{t_id}", "1", ex=604800, nx=True):
            enviar_evento_meta(uid, "Purchase", f"pur_{t_id}", value=valor)

    return "ok", 200

# ====================== START ENGINE ======================

bot_loop = asyncio.new_event_loop()
def run_bot():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())
    bot_loop.run_forever()

threading.Thread(target=run_bot, daemon=True).start()

# ====================== ROUTES ======================

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    if not WEBHOOK_BASE_URL:
        return "❌ Erro: WEBHOOK_BASE_URL não configurada nas variáveis de ambiente.", 500
        
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    try:
        async def s(): 
            await application.bot.set_webhook(url, drop_pending_updates=True)
        
        asyncio.run_coroutine_threadsafe(s(), bot_loop).result()
        return f"✅ Webhook configurado com sucesso: {url}", 200
    except Exception as e:
        logger.error(f"❌ Erro ao configurar webhook: {e}")
        return f"❌ Erro: {str(e)}", 500

@app.route("/", methods=["GET"])
def home(): 
    return "Tracker Online - Rastreio Meta CAPI Ativo", 200

if __name__ == "__main__":
    # O port 8080 é padrão para Render/Heroku/Railway
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
