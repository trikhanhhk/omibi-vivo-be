# VieNeu-TTS Testing Directory

This directory contains test suites and utilities for verifying the VieNeu-TTS package.

## How to run tests

Ensure you are in the project root:

```bash
uv run pytest
```

This will automatically discover and run all test suites in the `tests/` directory.

---

### Individual Test Suites
- **[test_normalize.py](test_normalize.py)**: Comprehensive Vietnamese text normalization (120+ cases).
- **[test_phonemize.py](test_phonemize.py)**: IPA phonemization logic.
- **[test_core_utils.py](test_core_utils.py)**: Core utility functions.
- **[test_tts_classes.py](test_tts_classes.py)**: Main TTS engine classes.

### Other Utilities
- **[benchmark.py](benchmark.py)**: RTF and latency benchmarking.
