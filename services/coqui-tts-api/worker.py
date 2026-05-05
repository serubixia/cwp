#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import contextlib
import io
import json
import os
import sys
import traceback
import wave

import numpy as np


def patch_torch_load_for_coqui() -> None:
    import torch

    original_torch_load = torch.load
    if getattr(original_torch_load, "_coqui_weights_only_patch", False):
        return

    def compatible_torch_load(*args, **kwargs):
        kwargs.setdefault("weights_only", False)
        return original_torch_load(*args, **kwargs)

    compatible_torch_load._coqui_weights_only_patch = True
    torch.load = compatible_torch_load


def patch_xtts_load_checkpoint_for_coqui() -> None:
    try:
        from TTS.tts.models.xtts import Xtts
    except ImportError:
        return

    original_load_checkpoint = Xtts.load_checkpoint
    if getattr(original_load_checkpoint, "_coqui_strict_patch", False):
        return

    def compatible_load_checkpoint(self, config, *args, **kwargs):
        kwargs["strict"] = False
        return original_load_checkpoint(self, config, *args, **kwargs)

    compatible_load_checkpoint._coqui_strict_patch = True
    Xtts.load_checkpoint = compatible_load_checkpoint


def resolve_device(requested_device: str) -> str:
    import torch

    normalized = (requested_device or "cpu").strip().lower()
    if normalized != "auto":
        return normalized
    if torch.cuda.is_available():
        return "cuda"
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def emit(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def load_tts_model():
    patch_torch_load_for_coqui()
    patch_xtts_load_checkpoint_for_coqui()

    from TTS.api import TTS

    model_name = (os.getenv("COQUI_MODEL") or "tts_models/es/css10/vits").strip()
    requested_language = (os.getenv("COQUI_LANGUAGE") or "es").strip() or None
    device = resolve_device(os.getenv("COQUI_DEVICE") or "cpu")

    with contextlib.redirect_stdout(sys.stderr):
        tts = TTS(model_name).to(device)

    language = requested_language if getattr(tts, "is_multi_lingual", False) else None
    return tts, model_name, device, language


def audio_to_wav_bytes(audio_data: list[float] | np.ndarray, sample_rate: int) -> bytes:
    audio_array = np.asarray(audio_data, dtype=np.float32)
    if audio_array.ndim > 1:
        audio_array = np.squeeze(audio_array)
    audio_array = np.clip(audio_array, -1.0, 1.0)
    pcm_data = (audio_array * 32767.0).astype(np.int16)

    with io.BytesIO() as wav_buffer:
        with wave.open(wav_buffer, "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(pcm_data.tobytes())
        return wav_buffer.getvalue()


def synthesize(tts, *, text: str, language: str | None) -> tuple[bytes, int]:
    synthesis_kwargs = {"text": text}
    if language:
        synthesis_kwargs["language"] = language

    with contextlib.redirect_stdout(sys.stderr):
        audio = tts.tts(**synthesis_kwargs)

    sample_rate = getattr(getattr(tts, "synthesizer", None), "output_sample_rate", 22050)
    return audio_to_wav_bytes(audio, sample_rate), int(sample_rate)


def handle_requests(tts, *, model_name: str, device: str, language: str | None) -> int:
    emit({"ready": True, "model": model_name, "device": device})

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            emit({"ok": False, "error": "Worker received invalid JSON."})
            continue

        request_id = request.get("id")
        action = request.get("action")
        text = request.get("text")

        if action != "synthesize":
            emit({"id": request_id, "ok": False, "error": "Unsupported worker action."})
            continue
        if not isinstance(text, str) or not text.strip():
            emit({"id": request_id, "ok": False, "error": "Field `text` must be a non-empty string."})
            continue

        try:
            wav_bytes, sample_rate = synthesize(tts, text=text.strip(), language=language)
        except Exception as exc:  # pragma: no cover - runtime worker path
            traceback.print_exc(file=sys.stderr)
            emit({"id": request_id, "ok": False, "error": str(exc)})
            continue

        emit(
            {
                "id": request_id,
                "ok": True,
                "content_type": "audio/wav",
                "sample_rate": sample_rate,
                "audio_base64": base64.b64encode(wav_bytes).decode("ascii"),
            }
        )

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coqui TTS worker for the Node API service.")
    parser.add_argument(
        "--preload-only",
        action="store_true",
        help="Load the configured model and exit without starting the stdin worker loop.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tts, model_name, device, language = load_tts_model()

    if args.preload_only:
        print(f"Preloaded {model_name} on {device}", file=sys.stderr, flush=True)
        return 0

    return handle_requests(tts, model_name=model_name, device=device, language=language)


if __name__ == "__main__":
    raise SystemExit(main())