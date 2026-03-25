import logging
import sys

# Força saída imediata sem buffer
bind = "0.0.0.0:8080"
workers = 1
worker_class = "sync"
timeout = 120

# Redireciona logs do Python para o stdout do gunicorn
accesslog = "-"
errorlog = "-"
loglevel = "info"
capture_output = True  # ← captura print() e logging do Python
enable_stdio_inheritance = True  # ← herda stdout/stderr
