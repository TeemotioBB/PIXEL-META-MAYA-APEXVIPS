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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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
        logger.error(f"⚠️ Redis OFFLINE — eventos duplicados são possíveis: {e}")
else:
    logger.warning("⚠️ REDIS_URL não definida — sem proteção contra duplicatas!")

# ====================== CAPI CENTRALIZADA ======================

def enviar_evento_capi(uid: int, event_name: str, custom_data=None, event_id=None):
    """
    Envia evento ao Meta CAPI com logs detalhados.
    Retorna True se aceito pelo Meta, False em qualquer falha.
    """
    generated_event_id = event_id or f"{event_name.lower()}_{uid}_{int(time.time())}"
    logger.info(f"📡 [CAPI] Iniciando envio | evento={event_name} uid={uid} event_id={generated_event_id}")

    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(time.time()),
            "event_id": generated_event_id,
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": custom_data or {},  # <-- TEM QUE TER ESSA VÍRGULA AQUI
            "test_event_code": "TEST22278"    # <-- E ESSA LINHA TAMBÉM
        }],
        "access_token": ACCESS_TOKEN
    }

    # Log do payload completo (sem o token)
    payload_log = {k: v for k, v in payload.items() if k != "access_token"}
    logger.info(f"📦 [CAPI] Payload: {payload_log}")

    try:
        url = f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events"
        resp = requests.post(url, json=payload, timeout=10)

        logger.info(f"📬 [CAPI] HTTP Status: {resp.status_code} | evento={event_name} uid={uid}")

        # O Meta retorna 200 mesmo com erros lógicos — precisamos ler o body
        try:
            resp_json = resp.json()
            logger.info(f"📋 [CAPI] Resposta Meta: {resp_json}")

            # "events_received" > 0 confirma que o Meta aceitou o evento
            events_received = resp_json.get("events_received", 0)
            if resp.status_code == 200 and events_received > 0:
                logger.info(f"✅ [CAPI] Evento ACEITO pelo Meta | evento={event_name} uid={uid} events_received={events_received}")
                return True
            else:
                messages = resp_json.get("messages", [])
                errors = resp_json.get("error", {})
                logger.error(
                    f"❌ [CAPI] Evento REJEITADO pelo Meta | evento={event_name} uid={uid} "
                    f"events_received={events_received} messages={messages} errors={errors}"
                )
                return False

        except Exception as parse_err:
            logger.error(f"❌ [CAPI] Falha ao parsear resposta do Meta: {parse_err} | body={resp.text}")
            return False

    except requests.exceptions.Timeout:
        logger.error(f"⏱️ [CAPI] TIMEOUT ao enviar {event_name} para uid={uid}")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"🔌 [CAPI] ERRO DE CONEXÃO ao enviar {event_name} para uid={uid}: {e}")
        return False
    except Exception as e:
        logger.error(f"💥 [CAPI] ERRO INESPERADO ao enviar {event_name} para uid={uid}: {e}")
        return False


def enviar_evento_capi_async(uid: int, event_name: str, custom_data=None, event_id=None):
    """Wrapper para disparar CAPI em thread com log de resultado."""
    def _run():
        result = enviar_evento_capi(uid, event_name, custom_data, event_id)
        if not result:
            logger.error(f"🚨 [CAPI THREAD] Falha confirmada | evento={event_name} uid={uid}")
    threading.Thread(target=_run, daemon=True).start()

# ====================== HANDLERS TELEGRAM ======================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or "sem_username"
    logger.info(f"🚀 [BOT] /start | uid={uid} username=@{username}")

    redis_key = f"lead_sent:{uid}:{date.today()}"

    if r:
        if not r.exists(redis_key):
            r.set(redis_key, "1", ex=86400)
            logger.info(f"🔑 [REDIS] Chave criada: {redis_key}")
            enviar_evento_capi_async(uid, "Lead")
        else:
            logger.info(f"⏭️ [REDIS] Lead duplicado bloqueado para uid={uid} (chave: {redis_key})")
    else:
        logger.warning(f"⚠️ [LEAD] Redis indisponível — enviando Lead sem trava | uid={uid}")
        enviar_evento_capi_async(uid, "Lead")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    data = update.callback_query.data
    logger.info(f"🖱️ [BOT] Botão clicado | uid={uid} data={data}")
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
    logger.info("✅ Bot pronto e aguardando updates.")
    bot_loop.run_forever()

threading.Thread(target=run_bot_init, daemon=True).start()

# ====================== ROTAS WEBHOOK ======================

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json()
    if data:
        logger.info(f"📩 [WEBHOOK TELEGRAM] Update recebido: {list(data.keys())}")
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
    else:
        logger.warning("⚠️ [WEBHOOK TELEGRAM] Payload vazio ou não-JSON recebido")
    return "ok", 200


@app.route('/apex-webhook', methods=['POST', 'GET'])
def apex_webhook():
    logger.info(f"📢 [APEX] Headers: {dict(request.headers)}")
    data = request.get_json(silent=True) or request.form.to_dict() or {}
    logger.info(f"📢 [APEX] Payload recebido: {data}")

    evento = data.get("event")
    customer = data.get("customer", {})
    uid = customer.get("chat_id")
    
    transaction = data.get("transaction", {})
    # A Apex usa 'internal_transaction_id' ou 'external_transaction_id'
    t_id = transaction.get("internal_transaction_id") or transaction.get("id")
    plan_value = transaction.get("plan_value")

    logger.info(f"📢 [APEX] Evento={evento} | uid={uid} | transaction_id={t_id} | plan_value={plan_value}")

    if not uid:
        logger.warning(f"⚠️ [APEX] chat_id ausente — ignorando evento={evento}")
        return {"status": "ok"}, 200

    # --- MAPEAMENTO CORRIGIDO ---

    # 1. Quando o usuário entra no bot ou inicia checkout
    if evento in ["user_joined", "checkout_created", "payment_created"]:
        logger.info(f"🛒 [APEX] Enviando InitiateCheckout para uid={uid}")
        enviar_evento_capi_async(uid, "InitiateCheckout")

    # 2. Quando o pagamento é aprovado
    elif evento in ["payment_approved", "sale_approved"]:
        val = float(plan_value or 0) / 100
        logger.info(f"💰 [APEX] Enviando Purchase para uid={uid} | valor=R${val:.2f}")
        enviar_evento_capi_async(uid, "Purchase", {"value": val, "currency": "BRL"}, f"pur_{t_id}")

    else:
        logger.info(f"ℹ️ [APEX] Evento ignorado (não configurado para Meta): {evento}")

    return {"status": "ok"}, 200

    # 1. INITIATE CHECKOUT
    if evento == "checkout_created":
        logger.info(f"🛒 [APEX] InitiateCheckout para uid={uid}")
        enviar_evento_capi_async(uid, "InitiateCheckout")

    # 2. PURCHASE — apenas rastreia no pixel, entrega é responsabilidade da Apex
    elif evento == "payment_approved":
        val = float(plan_value or 0) / 100
        logger.info(f"💰 [APEX] Purchase para uid={uid} | valor=R${val:.2f} | event_id=pur_{t_id}")
        enviar_evento_capi_async(uid, "Purchase", {"value": val, "currency": "BRL"}, f"pur_{t_id}")

    else:
        logger.info(f"ℹ️ [APEX] Evento não mapeado: {evento} | uid={uid}")

    return {"status": "ok"}, 200


@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    logger.info("🔧 [SET-WEBHOOK] Iniciando configuração...")
    if not bot_ready.wait(timeout=20):
        logger.error("❌ [SET-WEBHOOK] Bot não ficou pronto a tempo")
        return "Erro: bot não pronto", 503
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    async def setup(): return await application.bot.set_webhook(url=url)
    res = asyncio.run_coroutine_threadsafe(setup(), bot_loop).result()
    logger.info(f"✅ [SET-WEBHOOK] Webhook configurado: {url} | resultado={res}")
    return f"✅ Webhook: {res}", 200


@app.route("/")
def home():
    return "Bot Online", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
