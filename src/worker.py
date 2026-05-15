#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import tempfile
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


def normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("Expected a string value.")

    normalized = value.strip()
    return normalized or None


def resolve_speaker_wav(value: object) -> str | None:
    speaker_wav = normalize_optional_string(value)
    if speaker_wav is None:
        return None
    if not os.path.isfile(speaker_wav):
        raise FileNotFoundError(f"Speaker reference audio not found: {speaker_wav}")
    return speaker_wav


def load_tts_model():
    patch_torch_load_for_coqui()
    patch_xtts_load_checkpoint_for_coqui()

    from TTS.api import TTS

    model_name = (os.getenv("COQUI_MODEL") or "tts_models/es/css10/vits").strip()
    requested_language = (os.getenv("COQUI_LANGUAGE") or "es").strip() or None
    device = resolve_device(os.getenv("COQUI_DEVICE") or "cpu")
    speaker_wav = resolve_speaker_wav(os.getenv("COQUI_SPEAKER_WAV"))

    with contextlib.redirect_stdout(sys.stderr):
        tts = TTS(model_name).to(device)

    language = requested_language if getattr(tts, "is_multi_lingual", False) else None
    return tts, model_name, device, language, speaker_wav


def audio_to_wav_file(audio_data: list[float] | np.ndarray, sample_rate: int) -> tuple[str, int]:
    audio_array = np.asarray(audio_data, dtype=np.float32)
    if audio_array.ndim > 1:
        audio_array = np.squeeze(audio_array)
    audio_array = np.clip(audio_array, -1.0, 1.0)
    pcm_data = (audio_array * 32767.0).astype(np.int16)

    temp_fd, wav_path = tempfile.mkstemp(prefix="coqui-tts-", suffix=".wav")
    os.close(temp_fd)

    try:
        with wave.open(wav_path, "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(pcm_data.tobytes())
    except Exception:
        with contextlib.suppress(OSError):
            os.remove(wav_path)
        raise

    return wav_path, os.path.getsize(wav_path)


def synthesize(tts, *, text: str, language: str | None, speaker_wav: str | None) -> tuple[str, int, int]:
    synthesis_kwargs = {"text": text}
    if language:
        synthesis_kwargs["language"] = language
    if speaker_wav:
        synthesis_kwargs["speaker_wav"] = speaker_wav

    with contextlib.redirect_stdout(sys.stderr):
        audio = tts.tts(**synthesis_kwargs)

    sample_rate = getattr(getattr(tts, "synthesizer", None), "output_sample_rate", 22050)
    wav_path, wav_size = audio_to_wav_file(audio, sample_rate)
    return wav_path, int(sample_rate), wav_size


def handle_requests(tts, *, model_name: str, device: str, language: str | None, speaker_wav: str | None) -> int:
    emit({
        "ready": True,
        "model": model_name,
        "device": device,
        "language": language,
        "speaker_wav": speaker_wav,
    })

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
            request_speaker_wav = speaker_wav
            if "speaker_wav" in request:
                if not isinstance(request.get("speaker_wav"), str) or not request.get("speaker_wav").strip():
                    raise ValueError("Field `speaker_wav` must be a non-empty string when provided.")
                request_speaker_wav = resolve_speaker_wav(request.get("speaker_wav"))
        except Exception as exc:
            emit({"id": request_id, "ok": False, "error": str(exc)})
            continue

        try:
            wav_path, sample_rate, wav_size = synthesize(
                tts,
                text=text.strip(),
                language=language,
                speaker_wav=request_speaker_wav,
            )
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
                "audio_path": wav_path,
                "audio_size": wav_size,
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
    tts, model_name, device, language, speaker_wav = load_tts_model()

    if args.preload_only:
        print(f"Preloaded {model_name} on {device}", file=sys.stderr, flush=True)
        return 0

    return handle_requests(
        tts,
        model_name=model_name,
        device=device,
        language=language,
        speaker_wav=speaker_wav,
    )


if __name__ == "__main__":
    raise SystemExit(main())
