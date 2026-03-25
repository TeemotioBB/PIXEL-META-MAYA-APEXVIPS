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

# ====================== META CAPI ======================
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
import redis
r = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)

# ====================== FLASK ======================
app = Flask(__name__)

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    if r.exists(f"lead_sent:{uid}:{date.today()}"):
        return
    r.set(f"lead_sent:{uid}:{date.today()}", "1", ex=86400)

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

    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=15)
        logging.info(f"✅ LEAD SUPER_HOT → UID: {uid} | Trigger: {trigger}")
    except Exception as e:
        logging.error(f"Lead erro: {e}")


def enviar_initiatecheckout_capi(uid: int):
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
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=15)
        logging.info(f"✅ INITIATECHECKOUT → UID: {uid}")
    except Exception as e:
        logging.error(f"InitiateCheckout erro: {e}")


# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logging.info(f"🚀 /start ApexVips: {uid}")
    enviar_lead_capi(uid, "start")


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").lower().strip()

    enviar_lead_capi(uid, "user_message")

    if any(kw in text for kw in ["pix", "pagar", "pagamento", "qr", "como pago", "valor", "preço", "preco"]):
        enviar_lead_capi(uid, "payment_intent")
        enviar_initiatecheckout_capi(uid)


# ====================== FLASK ROUTES ======================
application = Application.builder().token(os.getenv("TELEGRAM_TOKEN_APEX")).build()

application.add_handler(CommandHandler("start", start_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        if data:
            update = Update.de_json(data, application.bot)
            application.update_queue.put(update)
        return "ok", 200
    except Exception as e:
        logging.error(f"Webhook erro: {e}")
        return "error", 500

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    base_url = os.getenv("WEBHOOK_BASE_URL")
    if not base_url:
        return "❌ WEBHOOK_BASE_URL não configurada", 400
    
    webhook_url = base_url.rstrip("/") + "/webhook"
    try:
        async def setup():
            await application.bot.delete_webhook(drop_pending_updates=True)
            await application.bot.set_webhook(webhook_url)
        import asyncio
        asyncio.run(setup())
        return f"✅ Webhook configurado!<br>URL: {webhook_url}", 200
    except Exception as e:
        return f"❌ Erro: {str(e)}", 500

@app.route("/", methods=["GET"])
def home():
    return "ApexVips Bot está online ✅", 200


# ====================== START ======================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("🚀 ApexVips Bot iniciado - CAPI otimizado")
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
