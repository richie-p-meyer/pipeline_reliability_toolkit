#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python3
"""
Pipeline Reliability Toolkit (PRT)
Resiliency helpers for data pipelines: retry/backoff, schema validation, dead-letter capture, log enrichment.

Stack: Python, Pandas (optional), Pydantic, Requests (or Playwright), argparse
Highlights: early-fail input validation, consistent error taxonomy, CLI for rapid triage

Quick CLI:
  # Validate a CSV or JSONL against a built-in schema and write good/bad rows
  python3 pipeline_reliability_toolkit.py validate \
      --in-file data.csv --schema builtins.ProductRecord \
      --good-out good.jsonl --dead-letter dead_letter.jsonl

  # Fetch a URL with retries/backoff and structured logs
  python3 pipeline_reliability_toolkit.py run-url --url https://httpbin.org/status/500

  # Summarize a dead-letter file (JSONL of {"record":..., "error_code":..., "error_msg":...})
  python3 pipeline_reliability_toolkit.py triage --dead-letter dead_letter.jsonl
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import math
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type

# Optional deps import guard (pandas, requests)
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # lazy optional

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

# ---- Structured Logging ----------------------------------------------------

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        # Enrich with extra kwargs if present
        for k in ("run_id", "step", "correlation_id", "error_code"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)
        # Include exception info if any
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

def setup_logging(level: int = logging.INFO, run_id: Optional[str] = None, step: Optional[str] = None) -> None:
    """Initialize JSON logging with optional enrichment defaults via LoggerAdapter."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    # Adapter lets you pass default extra keys on each log
    global log
    log = logging.LoggerAdapter(root, {"run_id": run_id or new_run_id(), "step": step or "init"})

def new_run_id() -> str:
    return uuid.uuid4().hex[:12]

# global logger adapter (initialized by setup_logging)
log: logging.LoggerAdapter

# ---- Retry / Backoff Decorator ---------------------------------------------

class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

@dataclass
class RetryConfig:
    attempts: int = 5
    base_delay: float = 0.25  # seconds
    max_delay: float = 8.0
    jitter: float = 0.25      # add random(0, jitter) seconds
    multiplier: float = 2.0   # exponential factor
    retry_on: Tuple[Type[BaseException], ...] = (Exception,)

def retry(config: RetryConfig):
    """Exponential backoff with jitter. Preserves last exception."""
    def deco(fn: Callable):
        def wrapper(*args, **kwargs):
            delay = config.base_delay
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except config.retry_on as e:  # type: ignore[misc]
                    attempt += 1
                    if attempt >= config.attempts:
                        # Annotate with a consistent error taxonomy code
                        setattr(e, "error_code", getattr(e, "error_code", "RETRY_EXHAUSTED"))
                        log.error("Retry exhausted", extra={"error_code": "RETRY_EXHAUSTED"})
                        raise RetryError(f"All {config.attempts} attempts failed") from e
                    sleep_for = min(delay, config.max_delay) + random.random() * config.jitter
                    log.warning(
                        f"Retrying after error (attempt {attempt}/{config.attempts}) sleeping {sleep_for:.2f}s: {e}",
                        extra={"error_code": getattr(e, "error_code", "RETRY")}
                    )
                    time.sleep(sleep_for)
                    delay *= config.multiplier
        return wrapper
    return deco

# ---- Error Taxonomy Helpers ------------------------------------------------

class ErrorTaxonomy:
    """Centralized error codes/messages for consistent dead-letter + logs."""
    VALIDATION = "VALIDATION"
    SCHEMA = "SCHEMA"
    IO = "IO"
    NETWORK = "NETWORK"
    RETRY = "RETRY"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    UNKNOWN = "UNKNOWN"

# ---- Dead-letter Writer ----------------------------------------------------

class DeadLetterWriter:
    """
    Writes dead-letter entries as JSONL:
    {"record": <original>, "error_code": "...", "error_msg": "...", "ts": "..."}
    """
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def write(self, record: Any, error_code: str, error_msg: str) -> None:
        doc = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "error_code": error_code,
            "error_msg": error_msg,
            "record": record,
        }
        self._fh.write(json.dumps(doc, ensure_ascii=False) + "\n")
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass

# ---- Schema Validation (Pydantic) -----------------------------------------

from pydantic import BaseModel, ValidationError, Field  # type: ignore

# Example built-in schema you can extend/replace.
class ProductRecord(BaseModel):
    id: str = Field(..., min_length=1)
    name: str
    price: float = Field(..., ge=0.0)
    quantity: int = Field(..., ge=0)
    ts: Optional[datetime] = None

class SchemaRegistry:
    """Simple registry to resolve dotted schema names to classes."""
    _map: Dict[str, Type[BaseModel]] = {
        "builtins.ProductRecord": ProductRecord,
    }

    @classmethod
    def resolve(cls, dotted: str) -> Type[BaseModel]:
        if dotted in cls._map:
            return cls._map[dotted]
        # Dynamic import path: module.ClassName
        mod_name, _, klass = dotted.rpartition(".")
        if not mod_name or not klass:
            raise ValueError(f"Invalid schema path: {dotted}")
        module = __import__(mod_name, fromlist=[klass])
        model_cls = getattr(module, klass, None)
        if not model_cls:
            raise ValueError(f"Schema not found at: {dotted}")
        if not issubclass(model_cls, BaseModel):
            raise TypeError("Resolved class is not a Pydantic BaseModel")
        return model_cls  # type: ignore[return-value]

def validate_record(model: Type[BaseModel], record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Validate a single dict; return (clean_record, error_msg)
    If invalid, (None, error message string).
    """
    try:
        obj = model(**record)
        return obj.model_dump(mode="json"), None
    except ValidationError as ve:
        return None, ve.errors().__repr__()

def validate_iter(
    model: Type[BaseModel],
    records: Iterable[Dict[str, Any]],
    dead_writer: Optional[DeadLetterWriter] = None,
) -> Iterable[Dict[str, Any]]:
    """Yield only valid records; invalid ones go to dead-letter."""
    for rec in records:
        clean, err = validate_record(model, rec)
        if err is None:
            yield clean  # type: ignore[misc]
        else:
            if dead_writer:
                dead_writer.write(rec, ErrorTaxonomy.VALIDATION, err)
            log.warning("Validation failed", extra={"error_code": ErrorTaxonomy.VALIDATION})

def validate_dataframe(
    model: Type[BaseModel],
    df: "pd.DataFrame",
    dead_writer: Optional[DeadLetterWriter] = None,
) -> "pd.DataFrame":
    """Validate a DataFrame row-by-row; returns a clean DataFrame with only valid rows."""
    if pd is None:
        raise RuntimeError("pandas is required for validate_dataframe but is not installed.")
    good_rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        clean, err = validate_record(model, row.to_dict())
        if err is None:
            good_rows.append(clean)  # type: ignore[arg-type]
        else:
            if dead_writer:
                dead_writer.write(row.to_dict(), ErrorTaxonomy.VALIDATION, err)
            log.warning("Validation failed", extra={"error_code": ErrorTaxonomy.VALIDATION})
    return pd.DataFrame(good_rows)

# ---- Example: Resilient HTTP fetch with retries ---------------------------

@retry(RetryConfig(attempts=5, base_delay=0.3, max_delay=4.0, jitter=0.3, multiplier=2.0,
                   retry_on=(requests.exceptions.RequestException,) if requests else (Exception,)))
def resilient_get(url: str, timeout: float = 5.0) -> Tuple[int, str]:
    """GET with exponential backoff + jitter. Returns (status_code, text)."""
    if requests is None:
        raise RuntimeError("requests is not installed.")
    resp = requests.get(url, timeout=timeout)
    # Treat 5xx as retryable to demonstrate behavior
    if 500 <= resp.status_code < 600:
        e = RuntimeError(f"Server error {resp.status_code}")
        setattr(e, "error_code", ErrorTaxonomy.NETWORK)
        raise e
    return resp.status_code, resp.text

# ---- CLI Commands ----------------------------------------------------------

def cmd_validate(args: argparse.Namespace) -> int:
    setup_logging(step="validate")
    in_path = Path(args.in_file)
    good_out = Path(args.good_out) if args.good_out else None
    dead_out = Path(args.dead_letter) if args.dead_letter else None

    Model = SchemaRegistry.resolve(args.schema)
    dlw = DeadLetterWriter(dead_out) if dead_out else None

    def write_jsonl(path: Path, items: Iterable[Dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for it in items:
                fh.write(json.dumps(it, ensure_ascii=False) + "\n")

    # Auto-detect CSV vs JSONL
    suffix = in_path.suffix.lower()
    good_items: Iterable[Dict[str, Any]]
    if suffix in (".csv", ".tsv"):
        if pd is None:
            # fallback basic CSV reader if pandas missing
            delimiter = "\t" if suffix == ".tsv" else ","
            with in_path.open("r", encoding="utf-8") as fh:
                reader = csv.DictReader(fh, delimiter=delimiter)
                good_items = list(validate_iter(Model, reader, dlw))
        else:
            df = pd.read_csv(in_path)
            df_clean = validate_dataframe(Model, df, dlw)
            good_items = df_clean.to_dict(orient="records")
    else:
        with in_path.open("r", encoding="utf-8") as fh:
            def lines():
                for line in fh:
                    if line.strip():
                        yield json.loads(line)
            good_items = list(validate_iter(Model, lines(), dlw))

    # Write good rows if requested
    if good_out:
        write_jsonl(good_out, good_items)
        log.info(f"Wrote good records", extra={"step": "validate"})
    if dlw:
        dlw.close()
    return 0

def cmd_run_url(args: argparse.Namespace) -> int:
    setup_logging(step="run-url")
    try:
        status, _ = resilient_get(args.url, timeout=args.timeout)
        log.info(f"Fetched {args.url} status={status}")
        return 0
    except RetryError as re:
        log.error(str(re), extra={"error_code": ErrorTaxonomy.RETRY_EXHAUSTED})
        return 2
    except Exception as e:
        log.error(f"Fetch failed: {e}", extra={"error_code": ErrorTaxonomy.NETWORK})
        return 1

def cmd_triage(args: argparse.Namespace) -> int:
    setup_logging(step="triage")
    dl_path = Path(args.dead_letter)
    if not dl_path.exists():
        print(f"Dead-letter file not found: {dl_path}", file=sys.stderr)
        return 2

    by_code: Dict[str, int] = {}
    msgs: Dict[str, int] = {}
    sample: Dict[str, Any] = {}
    total = 0

    with dl_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            total += 1
            try:
                obj = json.loads(line)
            except Exception:
                continue
            code = obj.get("error_code", ErrorTaxonomy.UNKNOWN)
            msg = obj.get("error_msg", "")
            by_code[code] = by_code.get(code, 0) + 1
            if msg:
                msgs[msg] = msgs.get(msg, 0) + 1
            if not sample:
                sample = obj

    print("=== Dead-Letter Summary ===")
    print(f"File: {dl_path}")
    print(f"Total rows: {total}")
    print("By error_code:")
    for k, v in sorted(by_code.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  - {k}: {v}")
    if msgs:
        print("\nTop error messages:")
        for k, v in sorted(msgs.items(), key=lambda kv: (-kv[1], kv[0]))[:10]:
            print(f"  - ({v}) {k[:160]}")
    if sample:
        print("\nSample entry:")
        print(json.dumps(sample, indent=2)[:1200])
    return 0

# ---- Argparse Wiring -------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="prt", description="Pipeline Reliability Toolkit")
    sub = p.add_subparsers(dest="cmd", required=True)

    pv = sub.add_parser("validate", help="Validate CSV/JSONL against a Pydantic schema")
    pv.add_argument("--in-file", required=True, help="Path to CSV or JSONL")
    pv.add_argument("--schema", required=True, help="Dotted path, e.g. builtins.ProductRecord or mypkg.schemas.MyModel")
    pv.add_argument("--good-out", help="Where to write valid output (JSONL)")
    pv.add_argument("--dead-letter", help="Where to write invalid rows (JSONL)")

    pu = sub.add_parser("run-url", help="Fetch a URL with retries/backoff")
    pu.add_argument("--url", required=True)
    pu.add_argument("--timeout", type=float, default=5.0)

    pt = sub.add_parser("triage", help="Summarize a dead-letter JSONL")
    pt.add_argument("--dead-letter", required=True)

    return p

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "validate":
        return cmd_validate(args)
    elif args.cmd == "run-url":
        return cmd_run_url(args)
    elif args.cmd == "triage":
        return cmd_triage(args)
    else:
        parser.print_help()
        return 2

if __name__ == "__main__":
    sys.exit(main())

