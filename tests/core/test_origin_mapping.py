"""Tests for core/origin_mapping — pure mapping functions."""

from __future__ import annotations

import pytest

from atlas_stf.core.origin_mapping import (
    STATE_TO_UF,
    UF_TO_TJ_INDEX,
    UF_TO_TRF,
    index_to_tribunal_label,
    map_origin_to_datajud_indices,
    normalize_state_description,
)


class TestNormalizeStateDescription:
    def test_exact_match(self) -> None:
        assert normalize_state_description("SAO PAULO") == "SP"

    def test_case_insensitive(self) -> None:
        assert normalize_state_description("São Paulo") == "SP"

    def test_accented(self) -> None:
        assert normalize_state_description("MINAS GERAIS") == "MG"

    def test_none_input(self) -> None:
        assert normalize_state_description(None) is None

    def test_empty_string(self) -> None:
        assert normalize_state_description("") is None

    def test_uf_abbreviation(self) -> None:
        assert normalize_state_description("SP") == "SP"

    def test_substring_collision_prefers_longer_state_name(self) -> None:
        assert normalize_state_description("PARAIBA") == "PB"
        assert normalize_state_description("PARANA") == "PR"
        assert normalize_state_description("PARA") == "PA"

    def test_unknown(self) -> None:
        assert normalize_state_description("UNKNOWN PLACE") is None


class TestMapOriginToDatajudIndices:
    def test_trf_with_state(self) -> None:
        result = map_origin_to_datajud_indices("TRIBUNAL REGIONAL FEDERAL", "SAO PAULO")
        assert result == ["api_publica_trf3"]

    def test_tj_with_state(self) -> None:
        result = map_origin_to_datajud_indices("TRIBUNAL DE JUSTICA ESTADUAL", "RIO DE JANEIRO")
        assert result == ["api_publica_tjrj"]

    def test_state_only_returns_both(self) -> None:
        result = map_origin_to_datajud_indices(None, "MINAS GERAIS")
        assert "api_publica_tjmg" in result
        assert "api_publica_trf6" in result

    def test_mg_maps_to_trf6(self) -> None:
        assert UF_TO_TRF["MG"] == 6

    def test_stj(self) -> None:
        result = map_origin_to_datajud_indices("STJ", None)
        assert result == ["api_publica_stj"]

    def test_tst(self) -> None:
        result = map_origin_to_datajud_indices("TST", None)
        assert result == ["api_publica_tst"]

    def test_none_inputs(self) -> None:
        assert map_origin_to_datajud_indices(None, None) == []

    def test_unknown_court(self) -> None:
        assert map_origin_to_datajud_indices("TRIBUNAL DESCONHECIDO", None) == []

    @pytest.mark.parametrize("uf", ["SP", "RJ", "MG", "RS", "PR", "BA"])
    def test_all_major_ufs_have_tj(self, uf: str) -> None:
        assert uf in UF_TO_TJ_INDEX

    @pytest.mark.parametrize("uf", ["SP", "RJ", "MG", "RS", "PR", "BA"])
    def test_all_major_ufs_have_trf(self, uf: str) -> None:
        assert uf in UF_TO_TRF


class TestIndexToTribunalLabel:
    def test_tjsp(self) -> None:
        assert index_to_tribunal_label("api_publica_tjsp") == "TJSP"

    def test_trf1(self) -> None:
        assert index_to_tribunal_label("api_publica_trf1") == "TRF1"

    def test_stj(self) -> None:
        assert index_to_tribunal_label("api_publica_stj") == "STJ"


class TestStateMapping:
    def test_all_27_states(self) -> None:
        assert len(STATE_TO_UF) == 27
