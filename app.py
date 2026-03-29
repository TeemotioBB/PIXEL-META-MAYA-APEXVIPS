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

# ====================== CONFIGURAÇÕES E LOGS ======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Variáveis de Ambiente (Railway)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_APEX")
REDIS_URL = os.getenv("REDIS_URL")
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
PIXEL_ID = "735253462874774"

# Conexão Redis
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Redis Conectado com sucesso!")
except Exception as e:
    logger.error(f"❌ Erro ao conectar no Redis: {e}")
    raise

def hash_data(value: str) -> str:
    return hashlib.sha256(str(value).strip().lower().encode("utf-8")).hexdigest()

# ====================== LÓGICA DE DADOS META ======================
def montar_user_data(chat_id):
    """Recupera os dados de rastreio vinculados ao chat_id do Telegram"""
    user_data = {"external_id": [hash_data(str(chat_id))]}
    
    fbp = r.get(f"fbp:{chat_id}")
    fbc = r.get(f"fbc:{chat_id}")
    ip  = r.get(f"ip:{chat_id}")
    ua  = r.get(f"ua:{chat_id}")

    if fbp: user_data["fbp"] = fbp
    if fbc: user_data["fbc"] = fbc
    if ip:  user_data["client_ip_address"] = ip
    
    user_data["client_user_agent"] = ua if ua else "TelegramBot/1.0"
    return user_data

def enviar_para_meta(event_name, chat_id, event_id, custom_data=None):
    """Envia o evento via API de Conversões (CAPI)"""
    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(time.time()),
            "event_id": event_id,
            "action_source": "chat",
            "user_data": montar_user_data(chat_id),
            "custom_data": custom_data or {}
        }],
        "access_token": ACCESS_TOKEN
    }
    try:
        url = f"https://graph.facebook.com/v22.0/{PIXEL_ID}/events"
        res = requests.post(url, json=payload, timeout=10)
        logger.info(f"🚀 [CAPI] {event_name} enviado | ID: {event_id} | Status: {res.status_code}")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar CAPI: {e}")

# ====================== HANDLERS DO TELEGRAM ======================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_user.id
    payload = context.args[0] if context.args else ""
    
    logger.info(f"🚀 [START] User: {chat_id} | Payload: {payload}")

    if payload.startswith("track_"):
        # Tenta recuperar o tracking salvo pela rota /apex-tracking
        tracking_str = r.get(f"tracking:{payload}")
        
        if tracking_str:
            data = ast.literal_eval(tracking_str)
            # Vincula os cookies ao chat_id real
            for k, v in data.items():
                r.set(f"{k}:{chat_id}", v, ex=604800) # Expira em 7 dias
            
            # DEDUPLICAÇÃO: O event_id do Lead é o próprio UID gerado no JS
            enviar_para_meta("Lead", chat_id, payload)
        else:
            # Caso o usuário seja rápido demais e o tracking não tenha chegado no Redis
            r.set(f"pending_uid:{payload}", str(chat_id), ex=300)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Otimização: Dispara Lead no chat para manter o Pixel aquecido
    chat_id = update.effective_user.id
    enviar_para_meta("Lead", chat_id, f"msg_{chat_id}_{int(time.time())}")

# ====================== ROTAS FLASK (API / WEBHOOK) ======================
app = Flask(__name__)

@app.route('/apex-tracking', methods=['GET'])
def apex_tracking():
    """Recebe os dados do navegador (JS) e salva no Redis"""
    uid = request.args.get('uid')
    if not uid: return "Missing UID", 400

    fbc = request.args.get('fbc')
    fbclid = request.args.get('fbclid')
    
    # AJUSTE: Garante que o FBC use segundos (Unix Timestamp) para bater com o navegador
    if not fbc and fbclid:
        fbc = f"fb.1.{int(time.time())}.{fbclid}"

    tracking_data = {
        "fbp": request.args.get('fbp'),
        "fbc": fbc,
        "ip": request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0].strip(),
        "ua": request.headers.get('User-Agent')
    }
    
    r.set(f"tracking:{uid}", str(tracking_data), ex=86400)
    logger.info(f"💾 [TRACKING] UID {uid} salvo no Redis.")

    # Retro-vínculo: Se o bot já recebeu o /start mas o tracking atrasou
    chat_id = r.get(f"pending_uid:{uid}")
    if chat_id:
        for k, v in tracking_data.items():
            r.set(f"{k}:{chat_id}", v, ex=604800)
        enviar_para_meta("Lead", int(chat_id), uid)
        r.delete(f"pending_uid:{uid}")

    return "ok", 200

@app.route('/set-webhook', methods=['GET'])
def set_webhook():
    url = f"https://{request.host}/webhook"
    try:
        async def s():
            await application.bot.set_webhook(url)
        asyncio.run_coroutine_threadsafe(s(), asyncio.get_event_loop())
        return f"✅ Webhook do Telegram configurado para: {url}", 200
    except Exception as e:
        return str(e), 500

@app.route('/webhook', methods=['POST'])
def telegram_webhook():
    """Rota específica para as mensagens do Bot do Telegram"""
    try:
        data = request.json
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), asyncio.get_event_loop())
        return "ok", 200
    except Exception as e:
        return "err", 500

@app.route("/", methods=["GET"])
def home():
    return "🚀 Maya Meta API Online", 200

# ====================== INICIALIZAÇÃO DO BOT ======================
def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

if __name__ == "__main__":
    # Inicia Flask em uma thread separada
    threading.Thread(target=run_flask, daemon=True).start()

    # Inicia o Bot do Telegram
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    logger.info("🤖 Bot em execução...")
    application.run_polling()
