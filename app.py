#!/usr/bin/env python3
import os
import logging
import hashlib
import time
import asyncio
import threading
import requests
import redis
import ast
from datetime import date
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes

# ====================== LOGGING ======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ====================== ENV VARS ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")
PIXEL_ID = "735253462874774"
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

def hash_data(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

# ====================== REDIS ======================
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Redis conectado!")
except Exception as e:
    logger.error(f"❌ Erro Redis: {e}")
    raise

# ====================== USER DATA ======================
def montar_user_data(uid):
    """Busca dados no Redis e monta o dicionário user_data para a Meta com IP e UA reais"""
    user_data = {"external_id": [hash_data(str(uid))]}

    fbp = r.get(f"fbp:{uid}")
    fbc_raw = r.get(f"fbclid:{uid}")
    ip = r.get(f"ip:{uid}")
    ua = r.get(f"ua:{uid}")

    logger.info(f"[montar_user_data] UID {uid} → FBP: {bool(fbp)} | FBC: {bool(fbc_raw)} | IP: {bool(ip)} | UA: {bool(ua)}")

    if fbp:
        user_data["fbp"] = fbp
    if fbc_raw:
        user_data["fbc"] = fbc_raw
    if ip:
        user_data["client_ip_address"] = ip
    
    if ua:
        user_data["client_user_agent"] = ua
    else:
        user_data["client_user_agent"] = "TelegramBot/1.0"
        
    return user_data

# ====================== CAPI FUNCTIONS ======================
def enviar_lead_capi(uid: int, trigger: str):
    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": f"lead_{uid}_{date.today()}",
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"trigger": trigger}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🟢 [CAPI] Lead ENVIADO | UID: {uid} | Trigger: {trigger}")
    except Exception as e:
        logger.error(f"❌ Erro Lead: {e}")

def enviar_initiatecheckout_capi(uid: int):
    redis_key = f"checkout_sent:{uid}"
    if r.exists(redis_key):
        return
    r.set(redis_key, "1", ex=3600)

    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "InitiateCheckout",
            "event_time": int(time.time()),
            "event_id": f"init_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"currency": "BRL", "value": 0.00}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"🔥 [CAPI] Checkout ENVIADO | UID: {uid}")
    except Exception as e:
        logger.error(f"❌ Erro Checkout: {e}")

def enviar_purchase_capi(uid: int, valor: float):
    user_data = montar_user_data(uid)
    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": f"pur_{uid}_{int(time.time())}",
            "action_source": "chat",
            "user_data": user_data,
            "custom_data": {"value": valor, "currency": "BRL"}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        requests.post(f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events", json=payload, timeout=10)
        logger.info(f"💰 [CAPI] Purchase ENVIADO | UID: {uid} | R$ {valor:.2f}")
    except Exception as e:
        logger.error(f"❌ Erro Purchase: {e}")

# ====================== HANDLERS ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    args = context.args
    payload = args[0] if args else "" 

    logger.info(f"🚀 [START] User: {uid} | Payload: {payload}")

    if payload.startswith("track_"):
        temp_key = f"tracking:{payload}"
        tracking_str = None
        
        # SUPER-LOOP: Tenta buscar 10 vezes (20 segundos no total)
        for tentativa in range(10):
            tracking_str = r.get(temp_key)
            if tracking_str:
                break
            logger.info(f"⏳ Tentativa {tentativa+1}/10: Aguardando tracking de {payload}...")
            await asyncio.sleep(2.0)

        if tracking_str:
            try:
                tracking_data = ast.literal_eval(tracking_str)

                if "fbp" in tracking_data:
                    r.set(f"fbp:{uid}", tracking_data["fbp"], ex=604800)
                if "fbc" in tracking_data:
                    r.set(f"fbclid:{uid}", tracking_data["fbc"], ex=604800)
                if "client_ip" in tracking_data:
                    r.set(f"ip:{uid}", tracking_data["client_ip"], ex=604800)
                if "client_user_agent" in tracking_data:
                    r.set(f"ua:{uid}", tracking_data["client_user_agent"], ex=604800)

                r.delete(temp_key)
                logger.info(f"✅ [SUCESSO] Dados vinculados para UID {uid}")
                enviar_lead_capi(uid, "start_com_tracking")

            except Exception as e:
                logger.error(f"❌ Erro ao processar tracking data: {e}")
                enviar_lead_capi(uid, "start_erro_tracking")
        else:
            logger.warning(f"⚠️ [AVISO] Payload {payload} não chegou no Redis após 20 segundos.")
            enviar_lead_capi(uid, "start_sem_chave")
    else:
        logger.info(f"ℹ️ [START] Direto sem tracking para UID {uid}")
        enviar_lead_capi(uid, "start_direto")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    
    cb_data = (query.data or "").lower()
    logger.info(f"🖱️ [BOTÃO] User: {uid} | Clique: {cb_data}")
    
    enviar_lead_capi(uid, "button_click")
    
    if any(x in cb_data for x in ["plan", "buy", "pix", "pay"]):
        enviar_initiatecheckout_capi(uid)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").lower()
    
    logger.info(f"💬 [MENSAGEM] User: {uid}")
    enviar_lead_capi(uid, "chat")
    
    if "pagar com pix" in text or "plano selecionado" in text:
        enviar_initiatecheckout_capi(uid)

# ====================== ENGINE ======================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CallbackQueryHandler(button_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

bot_loop = asyncio.new_event_loop()

def run_bot():
    asyncio.set_event_loop(bot_loop)
    bot_loop.run_until_complete(application.initialize())
    bot_loop.run_until_complete(application.start())
    bot_loop.run_forever()

threading.Thread(target=run_bot, daemon=True).start()

# ====================== ROUTES ======================
@app.route('/apex-tracking', methods=['GET'])
def apex_tracking():
    uid = request.args.get('uid')
    if not uid:
        return jsonify({"error": "uid não fornecido"}), 400

    fbclid = request.args.get('fbclid')
    fbp = request.args.get('fbp')
    fbc = request.args.get('fbc') 
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    client_user_agent = request.headers.get('User-Agent')

    tracking_data = {}

    if fbp:
        tracking_data["fbp"] = fbp
    if fbc:
        tracking_data["fbc"] = fbc
    elif fbclid:
        tracking_data["fbc"] = f"fb.1.{int(time.time() * 1000)}.{fbclid}"

    if client_ip:
        tracking_data["client_ip"] = client_ip.split(',')[0].strip()
    if client_user_agent:
        tracking_data["client_user_agent"] = client_user_agent

    r.set(f"tracking:{uid}", str(tracking_data), ex=86400)
    logger.info(f"💾 [TRACKING RECEBIDO E SALVO] uid={uid} | FBP: {bool(fbp)}")
    
    return jsonify({"status": "ok", "uid": uid}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        if data:
            update = Update.de_json(data, application.bot)
            asyncio.run_coroutine_threadsafe(application.process_update(update), bot_loop)
        return "ok", 200
    except Exception as e:
        logger.error(f"❌ Erro Webhook: {e}")
        return "error", 500

@app.route('/apex-webhook', methods=['POST'])
def apex_webhook():
    data = request.get_json() or {}
    evento = data.get("event")
    uid = data.get("customer", {}).get("chat_id")
    val_raw = data.get("transaction", {}).get("plan_value", 0)
    if not uid: 
        return "ok", 200
    if evento == "user_joined": 
        enviar_lead_capi(uid, "apex_joined")
    elif evento == "payment_created": 
        enviar_initiatecheckout_capi(uid)
    elif evento == "payment_approved": 
        enviar_purchase_capi(uid, float(val_raw)/100)
    return "ok", 200

@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    try:
        async def s(): 
            await application.bot.set_webhook(url)
        asyncio.run_coroutine_threadsafe(s(), bot_loop).result()
        return f"✅ Webhook configurado: {url}", 200
    except Exception as e: 
        return str(e), 500

@app.route("/", methods=["GET"])
def home(): 
    return "Bot Online", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
