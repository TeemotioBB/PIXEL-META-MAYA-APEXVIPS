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
    if not value: return ""
    return hashlib.sha256(str(value).strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
        r.ping()
        logger.info("✅ Redis conectado!")
    except Exception as e:
        logger.error(f"⚠️ Redis OFFLINE: {e}")

# ====================== CAPI CENTRALIZADA ======================

def enviar_evento_capi(uid: int, event_name: str, custom_data=None, event_id=None):
    generated_event_id = event_id or f"{event_name.lower()}_{uid}_{int(time.time())}"
    
    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(time.time()),
            "event_id": generated_event_id,
            "action_source": "chat",
            "user_data": {
                "external_id": [hash_data(str(uid))]
            },
            "custom_data": custom_data or {},
            "test_event_code": "TEST22278"  # REMOVA ESTA LINHA APÓS VALIDAR NO FACEBOOK
        }],
        "access_token": ACCESS_TOKEN
    }

    try:
        url = f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events"
        resp = requests.post(url, json=payload, timeout=10)
        resp_json = resp.json()
        
        if resp.status_code == 200 and resp_json.get("events_received", 0) > 0:
            logger.info(f"✅ [CAPI] Evento {event_name} ACEITO | uid={uid}")
            return True
        else:
            logger.error(f"❌ [CAPI] Evento REJEITADO | erro: {resp_json}")
            return False
    except Exception as e:
        logger.error(f"💥 [CAPI] Erro ao enviar: {e}")
        return False

def enviar_evento_capi_async(uid: int, event_name: str, custom_data=None, event_id=None):
    threading.Thread(target=enviar_evento_capi, args=(uid, event_name, custom_data, event_id), daemon=True).start()

# ====================== HANDLERS TELEGRAM ======================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    redis_key = f"lead_sent:{uid}:{date.today()}"

    if r and not r.exists(redis_key):
        r.set(redis_key, "1", ex=86400)
        enviar_evento_capi_async(uid, "Lead")
    elif not r:
        enviar_evento_capi_async(uid, "Lead")

# ====================== ENGINE FLASK ======================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start_handler))

bot_loop = asyncio.new_event_loop()

def run_bot_init():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())
    bot_loop.run_forever()

threading.Thread(target=run_bot_init, daemon=True).start()

# ====================== ROTAS WEBHOOK ======================

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json()
    if data:
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    return "ok", 200

@app.route('/apex-webhook', methods=['POST', 'GET']) # Adicionado 'GET' aqui
def apex_webhook():
    # Se for apenas um teste da Apex (GET), responde OK
    if request.method == 'GET':
        return {"status": "ok", "message": "Endpoint ativo"}, 200

    data = request.get_json(silent=True) or request.form.to_dict() or {}
    logger.info(f"📢 [APEX] Payload recebido: {data}")

    evento = data.get("event")
    customer = data.get("customer", {})
    uid = customer.get("chat_id")
    
    transaction = data.get("transaction", {})
    plan_name = transaction.get("plan_name", "Plano VIP")
    t_id = transaction.get("internal_transaction_id") or transaction.get("external_transaction_id")
    
    plan_value_raw = transaction.get("plan_value") or 0
    valor_real = float(plan_value_raw) / 100

    if not uid:
        logger.warning(f"⚠️ [APEX] chat_id ausente no evento: {evento}")
        return {"status": "ok"}, 200

    # 1. INICIAR CHECKOUT
    if evento in ["user_joined", "payment_created", "checkout_created"]:
        enviar_evento_capi_async(uid, "InitiateCheckout", {
            "value": 0.00,
            "currency": "BRL",
            "content_name": plan_name,
            "content_type": "product"
        })

    # 2. COMPRA APROVADA
    elif evento in ["payment_approved", "sale_approved"]:
        enviar_evento_capi_async(uid, "Purchase", {
            "value": valor_real,
            "currency": "BRL",
            "content_name": plan_name,
            "content_type": "product",
            "num_items": 1
        }, f"pur_{t_id}")

    return {"status": "ok"}, 200

    # 1. INICIAR CHECKOUT (Valor fixo 0.00 conforme pedido)
    if evento in ["user_joined", "payment_created", "checkout_created"]:
        logger.info(f"🛒 [APEX] Iniciando Checkout (0.00) para uid={uid}")
        enviar_evento_capi_async(uid, "InitiateCheckout", {
            "value": 0.00,
            "currency": "BRL",
            "content_name": plan_name,
            "content_type": "product"
        })

    # 2. COMPRA APROVADA (Valor real pago)
    elif evento in ["payment_approved", "sale_approved"]:
        logger.info(f"💰 [APEX] Compra Real Aprovada: R${valor_real:.2f} para uid={uid}")
        enviar_evento_capi_async(uid, "Purchase", {
            "value": valor_real,
            "currency": "BRL",
            "content_name": plan_name,
            "content_type": "product",
            "num_items": 1
        }, f"pur_{t_id}")

    return {"status": "ok"}, 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    async def setup(): return await application.bot.set_webhook(url=url)
    asyncio.run_coroutine_threadsafe(setup(), bot_loop).result()
    return f"✅ Webhook configurado: {url}", 200

@app.route("/")
def home(): return "Bot Online", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
