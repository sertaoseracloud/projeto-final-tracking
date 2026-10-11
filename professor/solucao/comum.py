"""A solução de referência usa o mesmo módulo de apoio entregue ao aluno: pipeline/comum.py.

Este arquivo só carrega aquele e se coloca no lugar dele em sys.modules, para que
`from comum import ...` nos jobs (e o monkeypatch nos testes) usem exatamente o
mesmo objeto de módulo.
"""

import importlib.util
import sys
from pathlib import Path

_ARQUIVO = Path(__file__).resolve().parents[2] / "pipeline" / "comum.py"
_spec = importlib.util.spec_from_file_location(__name__, _ARQUIVO)
_modulo = importlib.util.module_from_spec(_spec)
sys.modules[__name__] = _modulo
_spec.loader.exec_module(_modulo)
