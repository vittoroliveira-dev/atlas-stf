"""Tests for TPU normalization layer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.core.tpu import (
    categorize_movement_text,
    is_devolvido_vista,
    is_pauta_inclusion,
    is_pauta_withdrawal,
    is_pedido_de_vista,
    is_prevencao,
    is_redistribution,
    movement_category_by_code,
    normalize_class_sigla_to_tpu,
    tpu_class_name,
    tpu_movement_name,
    tpu_subject_name,
    tpu_version,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def reference_dir(tmp_path: Path) -> Path:
    """Create a minimal reference directory with sample TPU data."""
    ref = tmp_path / "reference"
    ref.mkdir()
    (ref / "tpu_classes.json").write_text(
        json.dumps(
            {
                "1314": "Ação Direta de Inconstitucionalidade",
                "1331": "Habeas Corpus",
                "1348": "Recurso Extraordinário",
            }
        ),
        encoding="utf-8",
    )
    (ref / "tpu_movements.json").write_text(
        json.dumps(
            {
                "26": "Distribuição por sorteio",
                "12204": "Pedido de vista",
                "12112": "Inclusão em pauta",
            }
        ),
        encoding="utf-8",
    )
    (ref / "tpu_subjects.json").write_text(
        json.dumps(
            {
                "14781": "Direito Constitucional",
                "14782": "Direito Administrativo",
            }
        ),
        encoding="utf-8",
    )
    (ref / "tpu_version.json").write_text(
        json.dumps(
            {
                "sgt_version": "sgt-2025",
                "generated_at": "2026-03-15T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    return ref


# ---------------------------------------------------------------------------
# Reference data loading
# ---------------------------------------------------------------------------


class TestReferenceLoading:
    def test_tpu_version(self, reference_dir: Path):
        assert tpu_version(reference_dir) == "sgt-2025"

    def test_tpu_class_name(self, reference_dir: Path):
        assert tpu_class_name(1314, reference_dir) == "Ação Direta de Inconstitucionalidade"
        assert tpu_class_name(99999, reference_dir) is None

    def test_tpu_movement_name(self, reference_dir: Path):
        assert tpu_movement_name(26, reference_dir) == "Distribuição por sorteio"
        assert tpu_movement_name(99999, reference_dir) is None

    def test_tpu_subject_name(self, reference_dir: Path):
        assert tpu_subject_name(14781, reference_dir) == "Direito Constitucional"
        assert tpu_subject_name(99999, reference_dir) is None

    def test_missing_reference_dir(self, tmp_path: Path):
        """Gracefully handles missing reference files."""
        missing = tmp_path / "nonexistent"
        missing.mkdir()
        assert tpu_version(missing) == "unknown"
        assert tpu_class_name(11541, missing) is None


# ---------------------------------------------------------------------------
# Movement text categorization
# ---------------------------------------------------------------------------


class TestCategorizeMovementText:
    @pytest.mark.parametrize(
        "description,expected",
        [
            ("Distribuído por sorteio", "distribuicao"),
            ("distribuição ao relator", "distribuicao"),
            ("Pedido de vista - Min. X", "vista"),
            ("Devolvidos autos pelo pedido de vista", "vista"),
            ("Incluído em pauta", "pauta"),
            ("Retirado de pauta", "pauta"),
            ("Julgamento finalizado", "decisao"),
            ("Decisão monocrática", "decisao"),
            ("Provido por unanimidade", "decisao"),
            ("Embargos de declaração", "recurso"),
            ("Agravo regimental", "recurso"),
            ("Publicação no DJe", "publicacao"),
            ("Intimação eletrônica", "publicacao"),
            ("Redistribuição ao Min. Y", "deslocamento"),
            ("Remessa ao tribunal de origem", "deslocamento"),
            ("Trânsito em julgado", "baixa"),
            ("Arquivado definitivamente", "baixa"),
            ("Certidão de objeto e pé", "outros"),
            (None, "outros"),
            ("", "outros"),
        ],
    )
    def test_categorization(self, description: str | None, expected: str):
        assert categorize_movement_text(description) == expected


# ---------------------------------------------------------------------------
# Semantic boolean classifiers
# ---------------------------------------------------------------------------


class TestSemanticClassifiers:
    def test_is_redistribution(self):
        assert is_redistribution("Redistribuição ao Min. Y") is True
        assert is_redistribution("Redistribuído por sorteio") is True
        assert is_redistribution("Distribuído por sorteio") is False
        assert is_redistribution(None) is False

    def test_is_pedido_de_vista(self):
        assert is_pedido_de_vista("Pedido de vista - Min. X") is True
        assert is_pedido_de_vista("Vista dos autos ao PGR") is True
        assert is_pedido_de_vista("Decisão monocrática") is False
        assert is_pedido_de_vista(None) is False

    def test_is_devolvido_vista(self):
        assert is_devolvido_vista("Devolvidos autos pelo pedido de vista") is True
        assert is_devolvido_vista("Devolução de vista - Min. Z") is True
        assert is_devolvido_vista("Pedido de vista") is False
        assert is_devolvido_vista(None) is False

    def test_is_pauta_inclusion(self):
        assert is_pauta_inclusion("Incluído em pauta") is True
        assert is_pauta_inclusion("Inclusão em pauta para 2026-04-01") is True
        assert is_pauta_inclusion("Retirado de pauta") is False
        assert is_pauta_inclusion(None) is False

    def test_is_pauta_withdrawal(self):
        assert is_pauta_withdrawal("Retirado de pauta") is True
        assert is_pauta_withdrawal("Retirada de pauta a pedido") is True
        assert is_pauta_withdrawal("Incluído em pauta") is False
        assert is_pauta_withdrawal(None) is False

    def test_is_prevencao(self):
        assert is_prevencao("Prevenção - ADI 999") is True
        assert is_prevencao("Processo prevento") is True
        assert is_prevencao("Distribuído por sorteio") is False
        assert is_prevencao(None) is False


# ---------------------------------------------------------------------------
# Sigla → TPU code resolution
# ---------------------------------------------------------------------------


class TestSiglaResolution:
    def test_common_stf_classes(self, reference_dir: Path):
        assert normalize_class_sigla_to_tpu("ADI", reference_dir) == 1314
        assert normalize_class_sigla_to_tpu("HC", reference_dir) == 1331
        assert normalize_class_sigla_to_tpu("RE", reference_dir) == 1348
        assert normalize_class_sigla_to_tpu("ADPF", reference_dir) == 1322
        assert normalize_class_sigla_to_tpu("ARE", reference_dir) == 1045

    def test_case_insensitive(self, reference_dir: Path):
        assert normalize_class_sigla_to_tpu("adi", reference_dir) == 1314
        assert normalize_class_sigla_to_tpu("  hc  ", reference_dir) == 1331

    def test_unknown_sigla(self, reference_dir: Path):
        assert normalize_class_sigla_to_tpu("XYZ", reference_dir) is None


# ---------------------------------------------------------------------------
# Movement code categorization
# ---------------------------------------------------------------------------


class TestMovementCategoryByCode:
    def test_known_codes(self):
        assert movement_category_by_code(26) == "distribuicao"
        assert movement_category_by_code(12204) == "vista"
        assert movement_category_by_code(417) == "pauta"
        assert movement_category_by_code(36) == "deslocamento"
        assert movement_category_by_code(22) == "baixa"
        assert movement_category_by_code(92) == "publicacao"
        assert movement_category_by_code(193) == "decisao"

    def test_unknown_code(self):
        assert movement_category_by_code(99999) == "outros"
