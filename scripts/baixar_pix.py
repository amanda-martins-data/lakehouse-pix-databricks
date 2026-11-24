"""Baixa a API do Banco Central no computador, para quando o workspace não alcança a API.

Exemplos:
    python scripts/baixar_pix.py --inicio 202011 --fim 202611
    python scripts/baixar_pix.py            # três meses mais recentes

Depois envie a pasta ao volume (Databricks CLI configurado):
    databricks fs cp -r data/raw/transacoes_municipio \
        dbfs:/Volumes/workspace/pix_bronze/landing/transacoes_municipio
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pix_lakehouse import extract  # noqa: E402


def main() -> int:
    mes_atual = int(datetime.now(timezone.utc).strftime("%Y%m"))
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--inicio", type=int, help="AAAAMM (padrão: dois meses antes do fim)")
    ap.add_argument("--fim", type=int, default=mes_atual, help="AAAAMM (padrão: mês atual)")
    ap.add_argument("--pasta", default="data/raw/transacoes_municipio")
    args = ap.parse_args()
    inicio = args.inicio or extract.mes_anterior(args.fim, 2)

    for f in extract.extrair_periodo(inicio, args.fim, Path(args.pasta)):
        print(f"{f.ano_mes}: {f.linhas:>5} linhas  {f.arquivo}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
