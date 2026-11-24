from datetime import datetime, timezone

import pytest

from pix_lakehouse import extract


def test_meses_atravessa_a_virada_do_ano():
    assert extract.meses(202611, 202702) == [202611, 202612, 202701, 202702]


def test_meses_rejeita_intervalo_invertido():
    with pytest.raises(ValueError):
        extract.meses(202612, 202601)


def test_mes_anterior():
    assert extract.mes_anterior(202601) == 202512
    assert extract.mes_anterior(202611, 12) == 202511


def test_url_fixa_o_mes_com_filtro_alem_do_database():
    # DataBase sozinho devolve "a partir de"; o $filter é o que limita ao mês.
    url = extract.url_mes(202610)
    assert "@DataBase='202610'" in url
    assert "$filter=AnoMes%20eq%20202610" in url
    # A API responde 500 a qualquer $skip.
    assert "$skip" not in url


def test_falha_se_a_resposta_vier_cheia(monkeypatch):
    # Sem $skip não há como buscar o resto: melhor falhar do que gravar um mês cortado.
    monkeypatch.setattr(extract, "TAMANHO_PAGINA", 2)

    def falso(url):
        return {"value": [{"AnoMes": 202610}, {"AnoMes": 202610}]}

    with pytest.raises(ValueError, match="cortada"):
        extract.extrair_mes(202610, falso)


def test_recusa_resposta_com_outro_mes():
    # Protege contra a semântica "a partir de" do DataBase, caso o filtro falhe.
    def falso(url):
        return {"value": [{"AnoMes": 202610}, {"AnoMes": 202611}]}

    with pytest.raises(ValueError, match="outro mês"):
        extract.extrair_mes(202610, falso)


def test_tenta_de_novo_quando_a_rede_falha(monkeypatch):
    monkeypatch.setattr(extract.time, "sleep", lambda s: None)
    chamadas = {"n": 0}

    def instavel(url):
        chamadas["n"] += 1
        if chamadas["n"] < 3:
            raise ConnectionError("timeout")
        return {"value": [{"AnoMes": 202610}]}

    assert len(extract.extrair_mes(202610, instavel)) == 1
    assert chamadas["n"] == 3


def test_nome_do_arquivo_guarda_mes_e_data_da_extracao():
    quando = datetime(2026, 11, 24, 23, 10, 5, tzinfo=timezone.utc)
    assert extract.nome_arquivo(202611, quando) == "ano_mes=202611/pix_municipio_202611_20261124T231005Z.jsonl"


def test_extrair_periodo_grava_um_arquivo_por_mes(tmp_path):
    def falso(url):
        mes = int(url.split("AnoMes%20eq%20")[1].split("&")[0])
        return {"value": [{"AnoMes": mes, "Municipio_Ibge": 1}]}

    quando = datetime(2026, 11, 24, tzinfo=timezone.utc)
    feitas = extract.extrair_periodo(202610, 202611, tmp_path, falso, agora=lambda: quando)
    assert [f.ano_mes for f in feitas] == [202610, 202611]
    assert sorted(p.parent.name for p in tmp_path.rglob("*.jsonl")) == ["ano_mes=202610", "ano_mes=202611"]
