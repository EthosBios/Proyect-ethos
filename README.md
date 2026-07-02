# Familia Mariño · Saraniti — Libro Familiar

A pipeline that converts family audio recordings into a printed book, using AI transcription and narrative generation.

---

## Architecture

```
index.html  ──POST──▶  Apps Script (Code.gs)
(GitHub Pages)          ├── saves audio → Google Drive
                        └── writes row → Google Sheets (Respuestas)

                        Google Sheets (Respuestas + Perfiles)
                              │
                              ▼
                    FastAPI backend (Cloud Run)
                    POST /run/pipeline
                              │
                    ┌─────────┴─────────────────────────────┐
                    │         Pipeline (5 steps)            │
                    │  1. Transcriber  → Whisper (OpenAI)   │
                    │  2. Voice agent  → Claude             │
                    │  3. Chapter agent→ Claude             │
                    │  4. Editor agent → Claude             │
                    │  5. Layout agent → WeasyPrint → PDF   │
                    └───────────────────────────────────────┘
                              │
                        PDF → Google Drive
```

---

## Components

| File/Dir | Purpose |
|---|---|
| `index.html` | Web form — records audio, sends to Apps Script |
| `Code.gs` | Google Apps Script — receives POST, saves audio to Drive, writes to Sheet |
| `pipeline/main.py` | FastAPI app entrypoint |
| `pipeline/agents/` | The 5 pipeline agents |
| `pipeline/utils/sheets.py` | Google Sheets + Drive helpers |
| `Dockerfile` | Container image for Cloud Run |
| `deploy.sh` | Builds and deploys to Cloud Run |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness check |
| POST | `/run/pipeline` | Full pipeline (all 5 steps) |
| POST | `/run/transcriber` | Step 1 only |
| POST | `/run/voice` | Step 2 only |
| POST | `/run/chapters` | Step 3 only |
| POST | `/run/editor` | Step 4 only |
| POST | `/run/layout` | Step 5 only — generates PDF |

### `/run/pipeline` request body

```json
{
  "nombres": ["Ana Mariño", "Carlos Saraniti"],
  "pais": "argentina",
  "solo_desde": null,
  "familia": "Familia Mariño · Saraniti",
  "upload_to_drive": false
}
```

`solo_desde` can be `"transcriber"`, `"voice"`, `"chapters"`, `"editor"`, or `"layout"` to resume from a step.

---

## Setup

### 1. Apps Script (audio collection layer)

1. Open [script.google.com](https://script.google.com) → New project
2. Paste `Code.gs` content
3. Deploy as Web App: run as **Me**, access to **Anyone**
4. Copy the deploy URL into `index.html` as `SCRIPT_URL`
5. Run `testTodo()` from the editor to verify permissions

### 2. Google Sheets

Two sheets are required:
- **Respuestas + Perfiles** (`SHEET_ID`): one row per audio response; pipeline writes transcriptions and voice profiles here
- **Integrantes + Relaciones** (`FAMILIA_SHEET_ID`): family members and relationships for narrative context

The Service Account must have Editor access to both sheets.

### 3. Local development

```bash
cp .env.example .env
# Fill in GOOGLE_CREDENTIALS_JSON, ANTHROPIC_API_KEY, OPENAI_API_KEY

pip install -r pipeline/requirements.txt
uvicorn pipeline.main:app --reload
```

### 4. Deploy to Cloud Run

```bash
./deploy.sh
```

See `.env.example` for all required environment variables.

---

## Google Sheets columns

### Respuestas sheet

| A | B | C | D | E | F | G |
|---|---|---|---|---|---|---|
| Fecha/hora | Nombre | FechaNac | Nº pregunta | Link Audio | Transcripción | Fotografía |

### Integrantes sheet

| A | B | C | D | E |
|---|---|---|---|---|
| Nombre completo | Fecha Nac (YYYY-MM-DD) | Fecha Fallec | Rol familiar | ¿Es menor? |

### Relaciones sheet

| A | B | C |
|---|---|---|
| Persona A | Relación (padre/madre/cónyuge) | Persona B |

---

## Notes

- **CORS**: The backend has `CORSMiddleware` allowing requests from `https://ethosbios.com`, `https://www.ethosbios.com`, and `https://ethosbios.vercel.app`. Update `allow_origins` if the frontend URL changes.
- **Credentials**: Always use `GOOGLE_CREDENTIALS_JSON` (JSON string) or `GOOGLE_CREDENTIALS_FILE` (path). Never commit credentials to git.
- **Pipeline duration**: Full pipeline for 10 people takes 35–56 min. The current `/run/pipeline` is synchronous. See `HEALTH_CHECK.md` for the async upgrade plan.
