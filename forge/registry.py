"""Local model registry helpers for Forge routing."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, List, Optional

from forge.store import ForgeStore, WorkflowValidationError


class ForgeRegistry:
    def __init__(self, store: ForgeStore, *, seed_path: str):
        self.store = store
        self.seed_path = seed_path

    @staticmethod
    def _compute_checksum(payload: Any) -> str:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(body).hexdigest()

    def _read_seed_document(self) -> Dict[str, Any]:
        if not os.path.exists(self.seed_path):
            return {"source_name": "seed", "source_ref": "local", "models": []}
        with open(self.seed_path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        if not isinstance(doc, dict):
            raise WorkflowValidationError("Registry seed document must be an object.")
        models = doc.get("models")
        if not isinstance(models, list):
            models = []
        return {
            "source_name": (doc.get("source_name") or "seed").strip() or "seed",
            "source_ref": (doc.get("source_ref") or "local").strip() or "local",
            "models": models,
            "previous_seeds": doc.get("previous_seeds") if isinstance(doc.get("previous_seeds"), list) else [],
        }

    def ensure_seeded(self) -> bool:
        doc = self._read_seed_document()
        if not isinstance(doc.get("models"), list) or not doc.get("models"):
            return False
        checksum = self._compute_checksum(doc.get("models") or [])
        seeded = self.store.seed_registry_if_empty(
            doc.get("models") or [],
            source_name=doc.get("source_name") or "seed",
            source_ref=doc.get("source_ref") or "local",
            checksum=checksum,
        )
        if seeded or not doc.get("previous_seeds"):
            return seeded
        result = self.store.replace_registry_snapshot(
            doc["models"], source_name=doc["source_name"], source_ref=doc["source_ref"],
            checksum=checksum, previous_seeds=doc["previous_seeds"],
        )
        return result["status"] == "succeeded"

    def list_models(
        self,
        *,
        modality: Optional[str] = None,
        provider: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        return self.store.list_registry_models(modality=modality, provider=provider, limit=limit)

    def sync_from_document(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise WorkflowValidationError("Registry payload must be an object.")
        rows = payload.get("models")
        if not isinstance(rows, list):
            raise WorkflowValidationError("Registry payload.models must be an array.")
        source_name = (payload.get("source_name") or "manual").strip() or "manual"
        source_ref = (payload.get("source_ref") or "api").strip() or "api"
        checksum = self._compute_checksum(rows)
        return self.store.replace_registry_snapshot(
            rows,
            source_name=source_name,
            source_ref=source_ref,
            checksum=checksum,
        )

    def sync_from_seed(self) -> Dict[str, Any]:
        doc = self._read_seed_document()
        checksum = self._compute_checksum(doc.get("models") or [])
        return self.store.replace_registry_snapshot(
            doc.get("models") or [],
            source_name=doc.get("source_name") or "seed",
            source_ref=doc.get("source_ref") or "local",
            checksum=checksum,
        )
