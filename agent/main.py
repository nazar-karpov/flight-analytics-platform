"""
AI Flight Analytics Agent -- FastAPI + OpenRouter + ClickHouse tool use.

POST /chat  -- accepts {message, history} -> returns {response}
GET  /      -- single-page chat UI
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from openai import OpenAI
from pydantic import BaseModel

log = logging.getLogger("agent")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -- Config --------------------------------------------------------------------
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "flights")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")


# -- ClickHouse client ---------------------------------------------------------
def _ch():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        username="default",
        password="",
    )


def _get_table_schemas() -> str:
    try:
        ch = _ch()
        tables = ch.query("SHOW TABLES FROM flights").result_rows
        parts = []
        for (tbl,) in tables:
            ddl = ch.query(f"SHOW CREATE TABLE flights.{tbl}").result_rows[0][0]
            parts.append(ddl)
        return "\n\n".join(parts)
    except Exception as exc:
        log.warning("Could not fetch schemas: %s", exc)
        return "(schemas unavailable -- ClickHouse may still be initializing)"


TABLE_SCHEMAS = _get_table_schemas()

# -- OpenAI / OpenRouter client ------------------------------------------------
openai_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
    default_headers={
        "HTTP-Referer": "http://localhost:8000",
        "X-Title": "Flight Analytics Agent",
    },
)

# -- Tool definitions ----------------------------------------------------------
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Execute a read-only SELECT query against ClickHouse and return "
                "the result rows as a JSON array (max 500 rows). "
                "Always use fully-qualified table names: flights.<table>."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A valid ClickHouse SELECT statement.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": (
                "Return the names and CREATE TABLE DDL for all tables in the "
                "flights database. Use this to understand available data before "
                "writing queries."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]


def run_sql(query: str) -> str:
    q = query.strip()
    upper = q.upper()
    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return json.dumps({"error": "Only SELECT queries are allowed."})
    try:
        ch = _ch()
        result = ch.query(q)
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows[:500]]
        return json.dumps(rows, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def list_tables() -> str:
    return TABLE_SCHEMAS or "(no tables found)"


def dispatch_tool(name: str, args: dict) -> str:
    if name == "run_sql":
        return run_sql(args.get("query", ""))
    if name == "list_tables":
        return list_tables()
    return json.dumps({"error": f"Unknown tool: {name}"})


# -- System prompt -------------------------------------------------------------
def build_system_prompt() -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""Ты — ассистент по аналитике авиатрафика. У тебя есть доступ к ClickHouse с данными о позициях самолётов в реальном времени (OpenSky Network).
Текущая дата/время: {now}

СХЕМА БАЗЫ ДАННЫХ:
{TABLE_SCHEMAS}

ОПИСАНИЕ ТАБЛИЦ:
- flights.mart_flights_current — последняя известная позиция каждого борта в воздухе (обновляется каждые 5 мин). Поля: icao24, callsign, airline_code, longitude, latitude, baro_altitude, velocity, true_track, origin_country, region, last_seen.
- flights.mart_airport_traffic_hourly — количество уникальных бортов в радиусе 50 км от топ-30 мировых аэропортов, по часам. Поля: airport_iata, airport_name, hour, unique_aircraft.
- flights.mart_airline_stats_daily — статистика по авиакомпаниям за день: число рейсов, средняя высота, средняя скорость, число регионов. Поля: airline_code, date, flight_count, avg_altitude, avg_speed, countries.
- flights.mart_region_traffic_current — количество самолётов в воздухе по регионам (Europe, Asia, NorthAmerica, SouthAmerica, Africa, Oceania, Other). Поля: region, aircraft_count, snapshot_time.
- flights.mart_flight_anomalies — обнаруженные аномалии: low_altitude (низкая высота вдали от аэропорта), unusual_speed (скорость выше 95-го перцентиля). Поля: icao24, callsign, anomaly_type, detected_at, latitude, longitude, details (JSON-строка).

ИНСТРУКЦИИ:
- Всегда отвечай на русском языке.
- Всегда проверяй ответы SQL-запросом перед тем как отвечать.
- Используй полные имена таблиц: flights.<table_name>.
- ClickHouse использует DateTime и функции toStartOfHour, date_trunc и т.д.
- Для «последних» данных используй ORDER BY ... DESC LIMIT n или ключевое слово FINAL.
- Давай краткие ответы с ключевыми цифрами из результатов запроса.
- Если данных ещё нет (таблицы пустые), скажи об этом прямо.
- airline_code — первые 3 символа callsign (код ICAO авиакомпании). Частые коды: AFL=Аэрофлот, BAW=British Airways, DLH=Lufthansa, UAL=United Airlines, DAL=Delta, RYR=Ryanair, SWA=Southwest.
"""


# -- FastAPI app ---------------------------------------------------------------
app = FastAPI(title="Flight Analytics AI Agent")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"detail": str(exc)})


class ChatRequest(BaseModel):
    message: str
    history: list[dict[str, Any]] = []


class ChatResponse(BaseModel):
    response: str


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not OPENROUTER_API_KEY:
        raise HTTPException(status_code=503, detail="OPENROUTER_API_KEY not configured")
    try:
        return await _chat_impl(req)
    except Exception as exc:
        log.error("Chat error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Agent error: {exc}")


async def _chat_impl(req: ChatRequest) -> ChatResponse:
    messages = [{"role": "system", "content": build_system_prompt()}]
    for h in req.history[-20:]:
        messages.append({"role": h.get("role", "user"), "content": h.get("content", "")})
    messages.append({"role": "user", "content": req.message})

    for iteration in range(10):
        completion = openai_client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
        )
        msg = completion.choices[0].message

        if not msg.tool_calls:
            return ChatResponse(response=msg.content or "")

        assistant_msg: dict[str, Any] = {"role": "assistant", "content": msg.content or ""}
        assistant_msg["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.function.name, "arguments": tc.function.arguments},
            }
            for tc in msg.tool_calls
        ]
        messages.append(assistant_msg)

        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                fn_args = {}
            log.info("Tool call: %s(%s)", fn_name, list(fn_args.keys()))
            result = dispatch_tool(fn_name, fn_args)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

    return ChatResponse(response="Не удалось завершить запрос после нескольких попыток.")


# -- Chat UI -------------------------------------------------------------------
CHAT_UI = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SkyWatch — AI Аналитика авиатрафика</title>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #f4f6fb;
    --surface: #ffffff;
    --border: #e2e6ef;
    --text: #1e293b;
    --text-sec: #64748b;
    --accent: #0369a1;
    --accent-light: #e0f2fe;
    --accent-hover: #075985;
    --user-bg: #0284c7;
    --radius: 14px;
  }
  body {
    font-family: -apple-system, 'Inter', 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    display: flex;
    flex-direction: column;
  }
  header {
    background: var(--surface);
    padding: 14px 24px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    gap: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
  }
  .logo {
    width: 36px; height: 36px;
    background: linear-gradient(135deg, #0ea5e9, #0369a1);
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 18px; color: #fff;
  }
  header .title-group { display: flex; flex-direction: column; }
  header h1 { font-size: 1.05rem; font-weight: 700; color: var(--text); line-height: 1.2; }
  header .subtitle { font-size: 0.72rem; color: var(--text-sec); }
  header .status {
    margin-left: auto;
    display: flex; align-items: center; gap: 6px;
    font-size: 0.75rem; color: #16a34a;
  }
  header .status::before {
    content: '';
    width: 7px; height: 7px;
    background: #22c55e;
    border-radius: 50%;
    animation: pulse 2s infinite;
  }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

  #chat-container {
    flex: 1;
    overflow-y: auto;
    padding: 24px 16px;
    display: flex;
    flex-direction: column;
    gap: 16px;
    max-width: 780px;
    width: 100%;
    margin: 0 auto;
  }
  .message {
    display: flex;
    gap: 10px;
    animation: fadeIn 0.25s ease;
  }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; } }
  .message.user { flex-direction: row-reverse; }

  .avatar {
    width: 32px; height: 32px;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 14px;
    flex-shrink: 0;
    margin-top: 2px;
  }
  .assistant .avatar { background: var(--accent-light); color: var(--accent); }
  .user .avatar { background: var(--user-bg); color: #fff; }

  .bubble {
    padding: 12px 16px;
    border-radius: var(--radius);
    line-height: 1.65;
    font-size: 0.92rem;
    max-width: 85%;
  }
  .user .bubble {
    background: var(--user-bg);
    color: #fff;
    border-bottom-right-radius: 4px;
  }
  .assistant .bubble {
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--text);
    border-bottom-left-radius: 4px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.03);
  }
  .assistant .bubble pre {
    background: #f8fafc;
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 14px;
    overflow-x: auto;
    margin: 8px 0;
    font-size: 0.82rem;
  }
  .assistant .bubble code { font-family: 'JetBrains Mono', 'Fira Code', monospace; font-size: 0.84em; color: var(--accent); }
  .assistant .bubble pre code { color: var(--text); }
  .assistant .bubble p { margin: 5px 0; }
  .assistant .bubble table {
    border-collapse: collapse;
    width: 100%;
    margin: 10px 0;
    font-size: 0.85rem;
  }
  .assistant .bubble th, .assistant .bubble td {
    border: 1px solid var(--border);
    padding: 6px 10px;
    text-align: left;
  }
  .assistant .bubble th { background: #f1f5f9; font-weight: 600; }
  .spinner {
    display: flex;
    align-items: center;
    gap: 8px;
    color: var(--text-sec);
    font-size: 0.85rem;
  }
  .dots span {
    width: 5px; height: 5px;
    background: var(--accent);
    border-radius: 50%;
    display: inline-block;
    animation: bounce 1.2s infinite;
  }
  .dots span:nth-child(2) { animation-delay: 0.15s; }
  .dots span:nth-child(3) { animation-delay: 0.3s; }
  @keyframes bounce { 0%,80%,100% { transform: translateY(0); } 40% { transform: translateY(-6px); } }

  #input-area {
    background: var(--surface);
    border-top: 1px solid var(--border);
    padding: 14px 16px;
    box-shadow: 0 -1px 3px rgba(0,0,0,0.03);
  }
  .suggestions {
    max-width: 780px;
    margin: 0 auto 10px;
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 6px;
  }
  .suggest-btn {
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text-sec);
    padding: 8px 12px;
    border-radius: 10px;
    font-size: 0.8rem;
    cursor: pointer;
    text-align: left;
    transition: all 0.15s;
  }
  .suggest-btn:hover { border-color: var(--accent); color: var(--accent); background: var(--accent-light); }
  #input-row {
    display: flex;
    gap: 8px;
    max-width: 780px;
    margin: 0 auto;
  }
  #user-input {
    flex: 1;
    background: var(--bg);
    border: 1.5px solid var(--border);
    color: var(--text);
    padding: 11px 16px;
    border-radius: 12px;
    font-size: 0.92rem;
    resize: none;
    min-height: 44px;
    max-height: 140px;
    font-family: inherit;
    transition: border-color 0.15s;
  }
  #user-input::placeholder { color: #94a3b8; }
  #user-input:focus { outline: none; border-color: var(--accent); background: #fff; }
  #send-btn {
    background: var(--accent);
    color: #fff;
    border: none;
    border-radius: 12px;
    width: 44px; height: 44px;
    cursor: pointer;
    font-size: 1.1rem;
    display: flex; align-items: center; justify-content: center;
    transition: background 0.15s;
    flex-shrink: 0;
  }
  #send-btn:hover { background: var(--accent-hover); }
  #send-btn:disabled { background: #cbd5e1; cursor: not-allowed; }
</style>
</head>
<body>
<header>
  <div class="logo">&#9992;</div>
  <div class="title-group">
    <h1>SkyWatch</h1>
    <span class="subtitle">AI-аналитика авиатрафика</span>
  </div>
  <span class="status">данные обновляются</span>
</header>

<div id="chat-container">
  <div class="message assistant">
    <div class="avatar">&#9992;</div>
    <div class="bubble">
      Привет! Я помогу разобраться в данных авиатрафика. Могу показать
      загруженность аэропортов, статистику авиакомпаний, обнаруженные
      аномалии и многое другое. Просто спросите!
    </div>
  </div>
</div>

<div id="input-area">
  <div class="suggestions" id="suggestions">
    <button class="suggest-btn" onclick="fillInput(this)">Какой аэропорт самый загруженный?</button>
    <button class="suggest-btn" onclick="fillInput(this)">Сколько самолётов над Европой?</button>
    <button class="suggest-btn" onclick="fillInput(this)">Топ авиакомпаний по рейсам сегодня</button>
    <button class="suggest-btn" onclick="fillInput(this)">Есть ли аномалии за последний час?</button>
  </div>
  <div id="input-row">
    <textarea id="user-input" placeholder="Спросите что-нибудь про авиатрафик..." rows="1"></textarea>
    <button id="send-btn" onclick="sendMessage()">&#10148;</button>
  </div>
</div>

<script>
const history = [];
const container = document.getElementById('chat-container');
const input = document.getElementById('user-input');
const btn = document.getElementById('send-btn');
const suggestions = document.getElementById('suggestions');

input.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
});
input.addEventListener('input', () => {
  input.style.height = 'auto';
  input.style.height = Math.min(input.scrollHeight, 140) + 'px';
});

function fillInput(el) { input.value = el.textContent; input.focus(); }

function addMessage(role, content, isSpinner = false) {
  if (suggestions) suggestions.style.display = 'none';
  const div = document.createElement('div');
  div.className = `message ${role}`;
  const avatar = document.createElement('div');
  avatar.className = 'avatar';
  avatar.innerHTML = role === 'user' ? '&#128100;' : '&#9992;';
  div.appendChild(avatar);
  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  if (isSpinner) {
    bubble.innerHTML = '<div class="spinner">Анализирую <div class="dots"><span></span><span></span><span></span></div></div>';
    div.dataset.spinner = 'true';
  } else {
    bubble.innerHTML = role === 'assistant' ? marked.parse(content) : escapeHtml(content);
  }
  div.appendChild(bubble);
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
  return div;
}

function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function removeSpinner() {
  const el = container.querySelector('[data-spinner]');
  if (el) el.remove();
}

async function sendMessage() {
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  input.style.height = 'auto';
  btn.disabled = true;

  addMessage('user', text);
  addMessage('assistant', '', true);

  try {
    const res = await fetch('/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, history }),
    });
    removeSpinner();
    if (!res.ok) {
      let errMsg = res.statusText;
      try { const err = await res.json(); errMsg = err.detail || errMsg; } catch {}
      addMessage('assistant', 'Ошибка: ' + errMsg);
    } else {
      const data = await res.json();
      addMessage('assistant', data.response);
      history.push({ role: 'user', content: text });
      history.push({ role: 'assistant', content: data.response });
      if (history.length > 40) history.splice(0, 2);
    }
  } catch (err) {
    removeSpinner();
    addMessage('assistant', 'Не удалось подключиться к серверу.');
  } finally {
    btn.disabled = false;
    input.focus();
  }
}
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return CHAT_UI
