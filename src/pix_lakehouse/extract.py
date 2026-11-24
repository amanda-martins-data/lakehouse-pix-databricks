"""Extração da API OData do Banco Central (Pix, transações por município).

Três comportamentos da fonte, medidos em 24/11/2026, moldam este módulo
(detalhes em docs/achados-da-fonte.md):

1. O parâmetro DataBase significa "a partir de", não "o mês de". Pedir
   DataBase=202510 devolve todos os meses de 202510 até o mais recente.
   Por isso cada requisição fixa o mês também com $filter.
2. O mês corrente é publicado antes de fechar (parcial). Então cada
   extração grava a data em que foi feita, e a Silver decide qual versão vale.
3. A API não aceita $skip (responde 500, "Erro desconhecido"), então não dá
   para paginar. Um mês tem no máximo 5.572 linhas (5.571 municípios e uma
   linha "N/D"), e cabe numa resposta só. Se a resposta vier cheia, o
   extrator falha em vez de gravar um mês possivelmente cortado.

O módulo usa só a biblioteca padrão, para rodar igual no computador e
num notebook do Databricks.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

BASE = ("https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/"
        "TransacoesPixPorMunicipio(DataBase=@DataBase)")
TAMANHO_PAGINA = 10_000   # um mês tem cerca de 5.572 linhas
TENTATIVAS = 4


def meses(inicio: int, fim: int) -> list[int]:
    """Lista de meses AAAAMM de inicio a fim, inclusive."""
    if inicio > fim:
        raise ValueError(f"inicio {inicio} depois de fim {fim}")
    out, ano, mes = [], inicio // 100, inicio % 100
    while ano * 100 + mes <= fim:
        out.append(ano * 100 + mes)
        mes += 1
        if mes == 13:
            ano, mes = ano + 1, 1
    return out


def mes_anterior(ano_mes: int, n: int = 1) -> int:
    ano, mes = ano_mes // 100, ano_mes % 100
    total = ano * 12 + (mes - 1) - n
    return (total // 12) * 100 + (total % 12) + 1


def url_mes(ano_mes: int, top: int = TAMANHO_PAGINA) -> str:
    """URL que devolve só o mês pedido (DataBase sozinho devolveria daí em diante).

    Sem $skip: a API responde 500 a qualquer requisição com esse parâmetro.
    """
    params = {
        "@DataBase": f"'{ano_mes}'",
        "$filter": f"AnoMes eq {ano_mes}",
        "$format": "json",
        "$top": str(top),
    }
    return BASE + "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote, safe="@$'")


def _http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


@dataclass
class Extracao:
    ano_mes: int
    linhas: int
    arquivo: str


def extrair_mes(ano_mes: int, get_json: Callable[[str], dict] = _http_get_json,
                pausa: float = 2.0) -> list[dict]:
    """Todas as linhas de um mês, numa requisição só (a API não pagina)."""
    url = url_mes(ano_mes)
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            linhas = get_json(url)["value"]
            break
        except Exception:  # rede instável: espera e tenta de novo
            if tentativa == TENTATIVAS:
                raise
            time.sleep(pausa * tentativa)
    fora_do_mes = [r for r in linhas if r.get("AnoMes") != ano_mes]
    if fora_do_mes:
        raise ValueError(f"{len(fora_do_mes)} linhas de outro mês na resposta de {ano_mes}")
    if len(linhas) >= TAMANHO_PAGINA:
        raise ValueError(f"{ano_mes}: resposta cheia ({len(linhas)} linhas), pode estar cortada")
    return linhas


def nome_arquivo(ano_mes: int, extraido_em: datetime) -> str:
    """Caminho relativo de uma extração: uma pasta por mês, um arquivo por execução.

    A Bronze nunca sobrescreve: uma nova extração do mesmo mês vira um arquivo
    novo, e a data de extração no nome diz qual é a mais recente.
    """
    return f"ano_mes={ano_mes}/pix_municipio_{ano_mes}_{extraido_em:%Y%m%dT%H%M%SZ}.jsonl"


def gravar_jsonl(linhas: Iterable[dict], destino: Path) -> int:
    destino.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with destino.open("w", encoding="utf-8") as f:
        for linha in linhas:
            f.write(json.dumps(linha, ensure_ascii=False) + "\n")
            n += 1
    return n


def extrair_periodo(inicio: int, fim: int, pasta: Path,
                    get_json: Callable[[str], dict] = _http_get_json,
                    agora: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> list[Extracao]:
    """Extrai cada mês do período para pasta/ano_mes=AAAAMM/*.jsonl."""
    feitas = []
    for ano_mes in meses(inicio, fim):
        linhas = extrair_mes(ano_mes, get_json)
        caminho = pasta / nome_arquivo(ano_mes, agora())
        n = gravar_jsonl(linhas, caminho)
        feitas.append(Extracao(ano_mes, n, str(caminho)))
    return feitas
