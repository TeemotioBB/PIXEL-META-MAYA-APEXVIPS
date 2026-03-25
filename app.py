#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                APEX VIPS BOT - META CAPI MAX POWER (Nicho Hot)               ║
║                 Apenas sinais ideais para Meta Ads - Botões já existentes    ║
╚══════════════════════════════════════════════════════════════════════════════╝
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

# ====================== META CAPI - CONFIG ======================
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = "EAANRM9QJv7YBRG54vW9VkOT3rgEQDry9PA2UzN7HsdauowZBDKZB0e1MtvZBvUuUSc9Ub2I96psCQTl0PZBRoIG7ElDCyMU7uO2idnf0nrebj4u3f7ZA396AGXCrBZC4NljW8OURxBu4qi5zGFZBEaWVtqlfwdZCoqGFeJ238YqE86c2tfwjdjBBJ52xLX3xZCh1sqwZDZD"

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
import redis
r = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)

# ====================== EVENTOS CAPI OTIMIZADOS PARA NICHO HOT ======================
def enviar_lead_capi(uid: int, trigger: str):
    """Lead com sinais fortes para nicho hot"""
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
                "customer_segmentation": "new_customer_to_business",
                "bot_type": "apex_vips"
            }
        }],
        "access_token": ACCESS_TOKEN
    }

    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=12)
        logging.info(f"✅ LEAD SUPER_HOT → {uid} | Trigger: {trigger}")
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
                "num_items": 1,
                "content_ids": ["apex_vip_lifetime"],
                "content_category": "adult_content",
                "niche": "hot_adult_vip",
                "subscription_type": "lifetime",
                "predicted_ltv": 97.50
            }
        }],
        "access_token": ACCESS_TOKEN
    }

    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=12)
        logging.info(f"✅ INITIATECHECKOUT → {uid}")
    except Exception as e:
        logging.error(f"InitiateCheckout erro: {e}")


def enviar_purchase_capi(uid: int, valor: float = 12.90, tx_id: str = None):
    if not tx_id:
        tx_id = f"pix_{uid}_{int(time.time())}"

    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": f"purchase_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": {"external_id": [hash_data(str(uid))]},
            "custom_data": {
                "currency": "BRL",
                "value": float(valor),
                "transaction_id": tx_id,
                "num_items": 1,
                "content_ids": ["apex_vip_lifetime"],
                "content_category": "adult_content",
                "niche": "hot_adult_vip",
                "subscription_type": "lifetime",
                "predicted_ltv": 97.50
            }
        }],
        "access_token": ACCESS_TOKEN
    }

    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=12)
        logging.info(f"✅ PURCHASE → {uid} | R${valor}")
    except Exception as e:
        logging.error(f"Purchase erro: {e}")


# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logging.info(f"🚀 Usuário iniciou ApexVips: {uid}")
    
    enviar_lead_capi(uid, trigger="start")   # Sinal mais importante no começo


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").lower().strip()

    # Qualquer mensagem do usuário = interesse → Lead forte
    enviar_lead_capi(uid, trigger="user_message")

    # Se mencionar pagamento → sinal ainda mais quente
    if any(kw in text for kw in ["pix", "pagar", "pagamento", "qr", "como pago", "valor", "preço", "preco"]):
        enviar_lead_capi(uid, trigger="payment_intent")
        enviar_initiatecheckout_capi(uid)   # Reforça intenção de compra


# ====================== COMANDO PARA ADMIN CONFIRMAR COMPRA ======================
async def compra_confirmada_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in [1293602874]:  # Coloque seus IDs de admin aqui
        return

    try:
        uid = int(context.args[0])
        valor = float(context.args[1]) if len(context.args) > 1 else 12.90
        tx_id = context.args[2] if len(context.args) > 2 else None

        enviar_purchase_capi(uid, valor=valor, tx_id=tx_id)
        await update.message.reply_text(f"✅ PURCHASE enviado para UID {uid} | R${valor}")
    except:
        await update.message.reply_text("Uso: /compra_confirmada <user_id> [valor] [tx_id]")


# ====================== SETUP ======================
def main():
    TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
    if not TOKEN:
        raise ValueError("Configure TELEGRAM_TOKEN_APEX")

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("compra_confirmada", compra_confirmada_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    logging.info("🚀 ApexVips Bot iniciado - CAPI otimizado para nicho HOT")
    application.run_polling()  # ou configure webhook como você já faz no Maya


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
