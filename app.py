#!/usr/bin/env python3
"""
APEX VIPS BOT - META CAPI MAX POWER (Versão Limpa e Otimizada)
"""

import os
import logging
import hashlib
import time
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

log.info("=" * 50)
log.info("🚀 APEX VIPS BOT - Iniciando...")
log.info("=" * 50)

# ====================== ENV VARS CHECK ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")

log.info(f"🔑 TELEGRAM_TOKEN_APEX: {'✅ Definida' if TELEGRAM_TOKEN else '❌ NÃO DEFINIDA'}")
log.info(f"🔑 REDIS_URL: {'✅ Definida' if REDIS_URL else '❌ NÃO DEFINIDA'}")
log.info(f"🔑 WEBHOOK_BASE_URL: {'✅ Definida' if WEBHOOK_BASE_URL else '❌ NÃO DEFINIDA'}")

# ====================== META CAPI ======================
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

log.info(f"📡 META CAPI - Pixel ID: {PIXEL_ID}")

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
import redis

log.info("🔌 Conectando ao Redis...")
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    log.info("✅ Redis conectado com sucesso!")
except Exception as e:
    log.error(f"❌ Falha ao conectar no Redis: {e}")
    raise

# ====================== FLASK ======================
app = Flask(__name__)
log.info("✅ Flask inicializado")

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    redis_key = f"lead_sent:{uid}:{date.today()}"
    log.info(f"📊 [CAPI] Tentando enviar Lead | UID: {uid} | Trigger: {trigger}")

    if r.exists(redis_key):
        log.info(f"⏭️  [CAPI] Lead já enviado hoje para UID: {uid} — pulando")
        return

    r.set(redis_key, "1", ex=86400)
    log.info(f"💾 [REDIS] Chave salva: {redis_key}")

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

    log.info(f"📤 [CAPI] Enviando evento Lead para Meta | UID: {uid}")
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events",
            json=payload,
            timeout=15
        )
        log.info(f"✅ [CAPI] Lead enviado | UID: {uid} | Trigger: {trigger} | Status HTTP: {resp.status_code} | Resposta: {resp.text}")
    except Exception as e:
        log.error(f"❌ [CAPI] Erro ao enviar Lead | UID: {uid} | Erro: {e}")


def enviar_initiatecheckout_capi(uid: int):
    log.info(f"🛒 [CAPI] Enviando InitiateCheckout | UID: {uid}")

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
        log.info(f"✅ [CAPI] InitiateCheckout enviado | UID: {uid} | Status HTTP: {resp.status_code} | Resposta: {resp.text}")
    except Exception as e:
        log.error(f"❌ [CAPI] Erro ao enviar InitiateCheckout | UID: {uid} | Erro: {e}")


# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or "sem_username"
    log.info(f"▶️  [TELEGRAM] /start recebido | UID: {uid} | Username: @{username}")
    enviar_lead_capi(uid, "start")


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or "sem_username"
    text = (update.message.text or "").lower().strip()

    log.info(f"💬 [TELEGRAM] Mensagem recebida | UID: {uid} | Username: @{username} | Texto: '{text}'")

    enviar_lead_capi(uid, "user_message")

    payment_keywords = ["pix", "pagar", "pagamento", "qr", "como pago", "valor", "preço", "preco"]
    matched = [kw for kw in payment_keywords if kw in text]

    if matched:
        log.info(f"💰 [INTENT] Intenção de pagamento detectada | UID: {uid} | Keywords: {matched}")
        enviar_lead_capi(uid, "payment_intent")
        enviar_initiatecheckout_capi(uid)
    else:
        log.info(f"📨 [INTENT] Mensagem comum, sem intenção de pagamento | UID: {uid}")


# ====================== FLASK ROUTES ======================
log.info("🔧 Construindo Application do Telegram...")
application = Application.builder().token(TELEGRAM_TOKEN).build()

application.add_handler(CommandHandler("start", start_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
log.info("✅ Handlers registrados: /start + mensagens de texto")

import asyncio

log.info("⚙️  Inicializando Application do Telegram (initialize + start)...")
try:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.start())
    log.info("✅ Application do Telegram inicializada e startada com sucesso!")
except Exception as e:
    log.error(f"❌ Falha ao inicializar Application do Telegram: {e}")
    raise


@app.route("/webhook", methods=["POST"])
def webhook():
    logger.info("📥 Webhook recebido do Telegram")
    try:
        data = request.json
        if data:
            update = Update.de_json(data, application.bot)
            asyncio.create_task(application.process_update(update))
            logger.info(f"🔄 Update recebido e enviado para processamento - ID: {update.update_id if 'update_id' in str(data) else 'N/A'}")
        return "ok", 200
    except Exception as e:
        logger.error(f"❌ Erro no webhook: {e}", exc_info=True)
        return "error", 500


@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    logger.info("🔗 Iniciando configuração do webhook...")
    if not WEBHOOK_BASE_URL:
        logger.error("❌ WEBHOOK_BASE_URL não configurada")
        return "❌ WEBHOOK_BASE_URL não configurada", 400
   
    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/webhook"
    logger.info(f"🔗 URL alvo: {webhook_url}")
   
    try:
        async def setup():
            await application.bot.delete_webhook(drop_pending_updates=True)
            await application.bot.set_webhook(webhook_url)
        asyncio.run(setup())
        logger.info(f"✅ Webhook definido com sucesso: {webhook_url}")
        return f"✅ Webhook configurado!<br>URL: {webhook_url}", 200
    except Exception as e:
        logger.error(f"❌ Erro ao configurar webhook: {e}", exc_info=True)
        return f"❌ Erro: {e}", 500


@app.route("/", methods=["GET"])
def home():
    log.info("🏠 [HOME] Health check acessado")
    return "ApexVips Bot está online ✅", 200


# ====================== START ======================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    log.info(f"🌐 Subindo Flask na porta {port}")
    app.run(host="0.0.0.0", port=port)
