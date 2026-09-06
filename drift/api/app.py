"""FastAPI application for Drift."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from drift.config import load_config
from drift.api.routes import queries, alerts, annotations, dbt


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Load config on startup
    app.state.config = load_config()
    yield


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Drift API",
        description="PostgreSQL query performance analyzer",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS middleware - allow all origins for local development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(queries.router, prefix="/api/v1", tags=["queries"])
    app.include_router(alerts.router, prefix="/api/v1", tags=["alerts"])
    app.include_router(annotations.router, prefix="/api/v1", tags=["annotations"])
    app.include_router(dbt.router, prefix="/api/v1", tags=["dbt"])

    # Serve static files (dashboard)
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    async def root():
        """Serve the dashboard."""
        index_path = Path(__file__).parent / "static" / "index.html"
        if index_path.exists():
            return FileResponse(index_path)
        return {"message": "Drift API", "docs": "/docs"}

    @app.get("/health")
    async def health():
        """Health check endpoint."""
        return {"status": "ok"}

    return app


# Create app instance for uvicorn
app = create_app()
