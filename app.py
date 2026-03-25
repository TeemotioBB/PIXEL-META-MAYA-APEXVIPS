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
from telegram.ext import (
    Application, 
    MessageHandler, 
    CommandHandler, 
    CallbackQueryHandler, 
    filters, 
    ContextTypes
)
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
    logger.info(f"[TELEGRAM] /start recebido | UID: {uid} | @{username}")
    
    # Envia Lead (Morno/Warm)
    enviar_lead_capi(uid, "start")

async def button_click_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Captura cliques nos botões Inline (os botões verdes de preço)"""
    query = update.callback_query
    uid = query.from_user.id
    # O callback_data é o que está 'atrás' do botão
    callback_data = (query.data or "").lower()
    
    # IMPORTANTE: Responde ao Telegram que o clique foi recebido
    await query.answer()

    # Esse log vai te mostrar exatamente o que a ApexVips usa no botão!
    logger.info(f"[BOTAO] Clique Inline detectado | UID: {uid} | Data: {callback_data}")

    # Envia um Lead de interação (Mais quente que o start)
    enviar_lead_capi(uid, "button_click")

    # Identificadores comuns de botões de checkout
    # Se o botão tiver qualquer um desses termos, enviamos o InitiateCheckout
    payment_identifiers = ["plan", "buy", "pix", "pay", "assin", "checkout", "p_", "v_"]
    
    if any(idf in callback_data for idf in payment_identifiers):
        logger.info(f"[INTENT] Intenção de compra no botão detectada! Data: {callback_data}")
        enviar_initiatecheckout_capi(uid)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    # O 'text' aqui pode vir do usuário OU do próprio bot se o seu código estiver lendo o chat
    text = (update.message.text or "").lower().strip()

    logger.info(f"[MONITOR] Mensagem detectada no chat | UID: {uid} | Texto: {text[:50]}...")

    # 1. Gatilho de Lead por interação comum
    enviar_lead_capi(uid, "chat_interaction")

    # 2. LISTA DE GATILHOS DA APEXVIPS (O que aparece na mensagem após o clique)
    # Se a mensagem contiver esses termos, o cara selecionou um plano!
    checkout_indicators = [
        "plano selecionado", 
        "valor: r$", 
        "escolha o método de pagamento",
        "pagar com pix"
    ]

    if any(indicator in text for indicator in checkout_indicators):
        logger.info(f"[🔥 CHECKOUT] Mensagem de sistema da ApexVips detectada! Enviando CAPI...")
        enviar_initiatecheckout_capi(uid)
# ====================== EVENT LOOP EM THREAD DEDICADA ======================
logger.info("Construindo Application do Telegram...")
application = Application.builder().token(TELEGRAM_TOKEN).build()

# --- REGISTRO DOS HANDLERS (ORDEM IMPORTA) ---
application.add_handler(CommandHandler("start", start_handler))

# NOVO: Escuta cliques em botões (Callback)
application.add_handler(CallbackQueryHandler(button_click_handler))

# NOVO: Escuta quando o bot EDITA a mensagem (Gatilho da ApexVips)
application.add_handler(MessageHandler(filters.UpdateType.EDITED_MESSAGE, message_handler))

# Escuta mensagens novas de texto
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

logger.info("Handlers atualizados: /start + Cliques + Edições de Mensagem")

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
    # Usando run_coroutine_threadsafe para garantir compatibilidade com o Gunicorn
    future = asyncio.run_coroutine_threadsafe(application.initialize(), bot_loop)
    future.result(timeout=30)
    future = asyncio.run_coroutine_threadsafe(application.start(), bot_loop)
    future.result(timeout=30)
    logger.info("Application do Telegram inicializada e startada com sucesso!")
except Exception as e:
    logger.error(f"Falha ao inicializar Application do Telegram: {e}")
    raise


# ====================== FLASK ROUTES ======================
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        update = Update.de_json(data, application.bot)
        
        # O SEGREDO ESTÁ AQUI: Mandar para o loop que está na outra thread
        asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
        
        return "ok", 200
    except Exception as e:
        logger.error(f"❌ Erro no webhook: {e}")
        return "error", 500

# NOVO: Rota para o Webhook da ApexVips
@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    """
    Recebe os avisos da ApexVips (Joined, Created, Approved)
    Configure esta URL no painel da ApexVips:
    https://pixel-meta-maya-apexvips-production.up.railway.app/apex-webhook
    """
    data = request.get_json()
    if not data:
        return "Sem dados", 200 # Retornamos 200 para evitar retentativas infinitas da Apex

    evento = data.get("event")
    customer = data.get("customer", {})
    uid = customer.get("chat_id")
    transaction = data.get("transaction", {})
    
    # Converte centavos da ApexVips para Real (ex: 1881 -> 18.81)
    valor_centavos = transaction.get("plan_value", 0)
    valor_real = float(valor_centavos) / 100 if valor_centavos > 0 else 0.0

    logger.info(f"📩 [APEX WEBHOOK] Evento: {evento} | UID: {uid} | Valor: R$ {valor_real}")

    if not uid:
        return "UID não identificado", 200

    # 1. Alguém entrou no Bot
    if evento == "user_joined":
        enviar_lead_capi(uid, "apex_user_joined")

    # 2. Alguém clicou no botão e GEROU o PIX (InitiateCheckout)
    elif evento == "payment_created":
        logger.info(f"🔥 [CHECKOUT] Usuário {uid} gerou um PIX. Enviando CAPI...")
        enviar_initiatecheckout_capi(uid)

    # 3. Alguém PAGOU o PIX (Purchase)
    elif evento == "payment_approved":
        logger.info(f"✅ [VENDA] Pagamento aprovado para UID {uid}. Valor: {valor_real}")
        enviar_purchase_capi(uid, valor_real)

    return "OK", 200


@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    logger.info("🔗 [SET-WEBHOOK] Iniciando configuração...")
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
        logger.info(f"✅ Webhook configurado com sucesso: {webhook_url}")
        return f"✅ Webhook configurado!<br>URL: {webhook_url}", 200
    except Exception as e:
        logger.error(f"❌ Erro no set-webhook: {e}", exc_info=True)
        return f"❌ Erro: {e}", 500


@app.route("/", methods=["GET"])
def home():
    logger.info("[HOME] Health check acessado")
    return "ApexVips Bot esta online", 200


# ====================== START ======================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    logger.info(f"Subindo Flask na porta {port}")
    app.run(host="0.0.0.0", port=port)
