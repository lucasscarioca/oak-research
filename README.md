# OakResearch

Minimal self-hostable deep research notebook starter.

> WIP: core app scaffold is in place; auth, ingestion, and research workflows are next.

## Run
```bash
docker compose up --build
```

Open:
- Web: http://localhost:5173
- API health: http://localhost:8000/health
- Worker health: http://localhost:8001/health

## Config
Copy `.env.example` to `.env` and set values as needed.

## Stack
- Frontend: TypeScript, React 19, Vite, Bun
- Backend: Python 3.12, FastAPI, uv
- Database: Postgres 16
- Deployment: Docker Compose
