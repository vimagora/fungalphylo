from __future__ import annotations

import os

import typer


def get_token(explicit: str | None) -> str:
    """Resolve a JGI Bearer token from an explicit value or the JGI_TOKEN env var.

    Accepts either:
      - "/api/sessions/...."
      - "Bearer /api/sessions/...."
    """
    if explicit:
        tok = explicit.strip()
    else:
        tok = os.getenv("JGI_TOKEN", "").strip()

    if not tok:
        raise typer.BadParameter("Missing JGI token. Provide --token or set env var JGI_TOKEN.")

    if not tok.lower().startswith("bearer "):
        tok = f"Bearer {tok}"
    return tok
