"""Tests for _assuntos module."""

import pandas as pd

from atlas_stf.staging._assuntos import fix_assuntos, normalize_multi_value


class TestFixAssuntos:
    def test_newline_separated_items(self):
        df = pd.DataFrame(
            {"Assunto completo": ["1 - DIREITO TRIBUTÁRIO || CRÉDITO || ALÍQUOTA\n2 - DIREITO PENAL || CRIMES"]}
        )
        result, count = fix_assuntos(df, "Assunto completo")
        val = result["Assunto completo"].iloc[0]
        assert "ALÍQUOTA | 2 - DIREITO PENAL" in val
        assert count == 1

    def test_single_item_unchanged(self):
        df = pd.DataFrame({"Assunto completo": ["1 - DIREITO CIVIL || OBRIGAÇÕES"]})
        result, count = fix_assuntos(df, "Assunto completo")
        assert result["Assunto completo"].iloc[0] == "1 - DIREITO CIVIL || OBRIGAÇÕES"
        assert count == 0

    def test_na_values_preserved(self):
        df = pd.DataFrame({"Assunto completo": [pd.NA, None]})
        result, count = fix_assuntos(df, "Assunto completo")
        assert pd.isna(result["Assunto completo"].iloc[0])
        assert count == 0

    def test_missing_column_returns_zero(self):
        df = pd.DataFrame({"Other": ["x"]})
        result, count = fix_assuntos(df, "Assunto completo")
        assert count == 0


class TestNormalizeMultiValue:
    def test_semicolon_hash_pattern(self):
        df = pd.DataFrame({"Tipo": ["Administrativa;#1;#Saúde;#23"]})
        result, count = normalize_multi_value(df, ["Tipo"], ";#")
        assert result["Tipo"].iloc[0] == "Administrativa | Saúde"
        assert count == 1

    def test_single_value_with_id(self):
        df = pd.DataFrame({"Tipo": ["Administrativa;#1"]})
        result, count = normalize_multi_value(df, ["Tipo"], ";#")
        assert result["Tipo"].iloc[0] == "Administrativa"
        assert count == 1

    def test_no_separator_unchanged(self):
        df = pd.DataFrame({"Tipo": ["Simple value"]})
        result, count = normalize_multi_value(df, ["Tipo"], ";#")
        assert result["Tipo"].iloc[0] == "Simple value"
        assert count == 0

    def test_alphanumeric_ids_are_not_kept_as_values(self):
        df = pd.DataFrame({"Tipo": ["Administrativa;#abc123;#Saúde;#uuid-2"]})
        result, count = normalize_multi_value(df, ["Tipo"], ";#")
        assert result["Tipo"].iloc[0] == "Administrativa | Saúde"
        assert count == 1
