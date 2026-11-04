# FinAlly — AI Trading Workstation

A visually stunning AI-powered trading workstation that streams live market data, simulates portfolio trading, and integrates an LLM chat assistant that can analyze positions and execute trades via natural language.

Built entirely by coding agents as a capstone project for an agentic AI coding course. Full specification lives in [`planning/PLAN.md`](planning/PLAN.md).

## Status

🚧 **In progress.** The market data subsystem (live price simulator + streaming) is complete — see [`planning/MARKET_DATA_SUMMARY.md`](planning/MARKET_DATA_SUMMARY.md). The frontend, portfolio, and AI chat layers are not yet built.

## Planned Architecture

Single Docker container serving everything on port 8000:

- **Frontend**: Next.js (static export) with TypeScript and Tailwind CSS
- **Backend**: FastAPI (Python/uv) with SSE streaming
- **Database**: SQLite with lazy initialization
- **AI**: LiteLLM → OpenRouter (Cerebras inference) with structured outputs
- **Market data**: Built-in GBM simulator (default) or Massive API (optional)

## Backend Development (current)

```bash
cd backend
uv sync --dev
uv run pytest
```

See [`backend/README.md`](backend/README.md) for details.

## Project Structure

```
finally/
├── backend/     # FastAPI uv project (market data complete; rest in progress)
├── frontend/    # Next.js static export (not yet started)
├── planning/    # Project spec and agent contracts
├── test/        # Playwright E2E tests (not yet started)
└── db/          # SQLite volume mount (runtime)
```

## License

See [LICENSE](LICENSE).
