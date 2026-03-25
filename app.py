#!/usr/bin/env python3
"""
APEX VIPS BOT - META CAPI MAX POWER
"""

import os
import logging
import hashlib
import time
import asyncio
import threading
import requests
from datetime import date
from flask import Flask, request
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

# ====================== LOGGING ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("=" * 50)
logger.info("🚀 APEX VIPS BOT - Iniciando...")
logger.info("=" * 50)

# ====================== ENV VARS CHECK ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")

logger.info(f"TELEGRAM_TOKEN_APEX: {'Definida' if TELEGRAM_TOKEN else 'NAO DEFINIDA'}")
logger.info(f"REDIS_URL: {'Definida' if REDIS_URL else 'NAO DEFINIDA'}")
logger.info(f"WEBHOOK_BASE_URL: {'Definida' if WEBHOOK_BASE_URL else 'NAO DEFINIDA'}")

# ====================== META CAPI ======================
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

logger.info(f"META CAPI - Pixel ID: {PIXEL_ID}")

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
import redis

logger.info("Conectando ao Redis...")
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("Redis conectado com sucesso!")
except Exception as e:
    log.error(f"Falha ao conectar no Redis: {e}")
    raise

# ====================== FLASK ======================
app = Flask(__name__)
logger.info("Flask inicializado")

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    redis_key = f"lead_sent:{uid}:{date.today()}"
    logger.info(f"[CAPI] Tentando enviar Lead | UID: {uid} | Trigger: {trigger}")

    if r.exists(redis_key):
        logger.info(f"[CAPI] Lead ja enviado hoje para UID: {uid} — pulando")
        return

    r.set(redis_key, "1", ex=86400)
    logger.info(f"[REDIS] Chave salva: {redis_key}")

    payload = {
        "data": [{
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": f"lead_{uid}_{date.today()}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {
                "lead_score": 98,
                "lead_level": "SUPER_HOT",
                "intent_type": "vip_purchase_intent",
                "funnel_phase": "bottom_funnel",
                "trigger": trigger,
                "content_category": "adult_content",
                "niche": "hot_adult_vip",
                "product_type": "digital_subscription",
                "subscription_duration": "lifetime",
                "predicted_ltv": 97.50,
                "bot_type": "apex_vips"
            }
        }],
        "access_token": ACCESS_TOKEN
    }

    logger.info(f"[CAPI] Enviando evento Lead para Meta | UID: {uid}")
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events",
            json=payload,
            timeout=15
        )
        logger.info(f"[CAPI] Lead enviado | UID: {uid} | Trigger: {trigger} | Status: {resp.status_code}")
    except Exception as e:
        log.error(f"[CAPI] Erro ao enviar Lead | UID: {uid} | Erro: {e}")


def enviar_initiatecheckout_capi(uid: int):
    logger.info(f"[CAPI] Enviando InitiateCheckout | UID: {uid}")

    payload = {
        "data": [{
            "event_name": "InitiateCheckout",
            "event_time": int(time.time()),
            "event_id": f"initiate_{uid}_{date.today()}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {
                "currency": "BRL",
                "value": 12.90,
                "content_category": "adult_content",
                "niche": "hot_adult_vip",
                "predicted_ltv": 97.50
            }
        }],
        "access_token": ACCESS_TOKEN
    }

    try:
        resp = requests.post(
            f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events",
            json=payload,
            timeout=15
        )
        logger.info(f"[CAPI] InitiateCheckout enviado | UID: {uid} | Status: {resp.status_code}")
    except Exception as e:
        log.error(f"[CAPI] Erro ao enviar InitiateCheckout | UID: {uid} | Erro: {e}")


# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or "sem_username"
    logger.info(f"[TELEGRAM] /start recebido | UID: {uid} | Username: @{username}")
    enviar_lead_capi(uid, "start")
    logger.info(f"[PROCESSADO] Lead /start capturado e enviado para Meta CAPI")


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or "sem_username"
    text = (update.message.text or "").lower().strip()

    logger.info(f"[TELEGRAM] Mensagem recebida | UID: {uid} | Username: @{username} | Texto: {text}")

    enviar_lead_capi(uid, "user_message")

    payment_keywords = ["pix", "pagar", "pagamento", "qr", "como pago", "valor", "preco"]
    matched = [kw for kw in payment_keywords if kw in text]

    if matched:
        logger.info(f"[INTENT] Intencao de pagamento detectada | UID: {uid} | Keywords: {matched}")
        enviar_lead_capi(uid, "payment_intent")
        enviar_initiatecheckout_capi(uid)
        logger.info(f"[PROCESSADO] InitiateCheckout enviado para Meta CAPI | UID: {uid}")
    else:
        logger.info(f"[INTENT] Mensagem comum, sem intencao de pagamento | UID: {uid}")


# ====================== EVENT LOOP EM THREAD DEDICADA ======================
logger.info("Construindo Application do Telegram...")
application = Application.builder().token(TELEGRAM_TOKEN).build()

application.add_handler(CommandHandler("start", start_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
logger.info("Handlers registrados: /start + mensagens de texto")

# Cria um loop dedicado que roda em thread separada
bot_loop = asyncio.new_event_loop()

def start_bot_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

bot_thread = threading.Thread(target=start_bot_loop, args=(bot_loop,), daemon=True)
bot_thread.start()
logger.info("Thread do event loop iniciada")

# Inicializa o Application dentro do loop dedicado
logger.info("Inicializando Application do Telegram...")
try:
    future = asyncio.run_coroutine_threadsafe(application.initialize(), bot_loop)
    future.result(timeout=30)
    future = asyncio.run_coroutine_threadsafe(application.start(), bot_loop)
    future.result(timeout=30)
    logger.info("Application do Telegram inicializada e startada com sucesso!")
except Exception as e:
    log.error(f"Falha ao inicializar Application do Telegram: {e}")
    raise


# ====================== FLASK ROUTES ======================
@app.route("/webhook", methods=["POST"])
def webhook():
    logger.info("[WEBHOOK] Requisicao recebida")
    try:
        data = request.json
        if not data:
            log.warning("[WEBHOOK] Body vazio recebido")
            return "ok", 200

        logger.info(f"[WEBHOOK] Payload recebido | update_id: {data.get('update_id', 'N/A')}")
        update = Update.de_json(data, application.bot)
        logger.info(f"[WEBHOOK] Processando update ID: {update.update_id}")

        future = asyncio.run_coroutine_threadsafe(
            application.process_update(update),
            bot_loop
        )
        future.result(timeout=30)

        logger.info(f"[WEBHOOK] Update {update.update_id} processado com sucesso")
        return "ok", 200
    except Exception as e:
        log.error(f"[WEBHOOK] Erro ao processar: {e}", exc_info=True)
        return "error", 500


@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    logger.info("[SET-WEBHOOK] Iniciando configuracao do webhook...")
    if not WEBHOOK_BASE_URL:
        log.error("[SET-WEBHOOK] WEBHOOK_BASE_URL nao configurada")
        return "WEBHOOK_BASE_URL nao configurada", 400

    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/webhook"
    logger.info(f"[SET-WEBHOOK] URL alvo: {webhook_url}")

    try:
        async def setup():
            await application.bot.delete_webhook(drop_pending_updates=True)
            logger.info("[SET-WEBHOOK] Webhook anterior deletado")
            await application.bot.set_webhook(webhook_url)
            logger.info(f"[SET-WEBHOOK] Webhook definido para: {webhook_url}")

        future = asyncio.run_coroutine_threadsafe(setup(), bot_loop)
        future.result(timeout=30)
        return f"Webhook configurado! URL: {webhook_url}", 200
    except Exception as e:
        log.error(f"[SET-WEBHOOK] Erro: {e}", exc_info=True)
        return f"Erro: {str(e)}", 500


@app.route("/", methods=["GET"])
def home():
    logger.info("[HOME] Health check acessado")
    return "ApexVips Bot esta online", 200


# ====================== START ======================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    logger.info(f"Subindo Flask na porta {port}")
    app.run(host="0.0.0.0", port=port)
