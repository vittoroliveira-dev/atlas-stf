"""Governance seed: join fitness classification and scope declaration.

Classifies columns as deterministic_key / probabilistic_key /
descriptive_only / absent based on observed data quality, and provides
the explicit scope declaration for Layer A.
"""

from __future__ import annotations

from typing import Any

# Join fitness levels
DETERMINISTIC = "deterministic_key"
PROBABILISTIC = "probabilistic_key"
DESCRIPTIVE = "descriptive_only"
ABSENT = "absent"

# Columns expected to serve as join keys, with ideal fitness level.
_EXPECTED_JOIN_KEYS: dict[str, dict[str, str]] = {
    "cgu": {
        "CPF OU CNPJ DO SANCIONADO": DETERMINISTIC,
        "NOME DO SANCIONADO": PROBABILISTIC,
        "CNPJ DO SANCIONADO": DETERMINISTIC,
    },
    "cvm": {
        "NUP": DETERMINISTIC,
        "Nome_Acusado": PROBABILISTIC,
        "CPF_CNPJ": DETERMINISTIC,
    },
    "rfb": {
        "cnpj_basico": DETERMINISTIC,
        "partner_cpf_cnpj": DETERMINISTIC,
        "partner_name_normalized": PROBABILISTIC,
        "razao_social": PROBABILISTIC,
        "representative_name_normalized": DESCRIPTIVE,
    },
    "tse": {
        "donor_cpf_cnpj": DETERMINISTIC,
        "donor_name_normalized": PROBABILISTIC,
        "candidate_cpf": DETERMINISTIC,
        "supplier_tax_id": DETERMINISTIC,
        "supplier_name_normalized": PROBABILISTIC,
    },
    "stf": {
        "processo": DETERMINISTIC,
        "no_do_processo": DETERMINISTIC,
        "relator_atual": PROBABILISTIC,
        "idfatodecisao": DETERMINISTIC,
    },
}

_NULL_THRESHOLD = 0.30
_EMPTY_THRESHOLD = 0.30


def classify_join_fitness(
    inventories: list[dict[str, Any]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Return ``{source/file: {column: {fitness info}}}``."""
    result: dict[str, dict[str, dict[str, Any]]] = {}

    for inv in inventories:
        source = inv["source"]
        fname = inv["file_name"]
        key = f"{source}/{fname}"
        expected = _EXPECTED_JOIN_KEYS.get(source, {})
        if not expected:
            continue

        observed = {c["observed_column_name"]: c for c in inv["columns"]}
        col_fitness: dict[str, dict[str, Any]] = {}

        for col_name, ideal in expected.items():
            if col_name in observed:
                col = observed[col_name]
                nr = col["null_rate"]
                er = col["empty_rate"]
                actual = ideal
                degradation = None
                if ideal == DETERMINISTIC and (nr > _NULL_THRESHOLD or er > _NULL_THRESHOLD):
                    actual = PROBABILISTIC
                    degradation = f"null={nr:.1%}, empty={er:.1%} exceeds threshold"
                col_fitness[col_name] = {
                    "expected": ideal,
                    "actual": actual,
                    "degradation": degradation,
                    "null_rate": nr,
                    "empty_rate": er,
                }
            else:
                col_fitness[col_name] = {
                    "expected": ideal,
                    "actual": ABSENT,
                    "degradation": f"column not present in {fname}",
                    "null_rate": None,
                    "empty_rate": None,
                }

        if col_fitness:
            result[key] = col_fitness

    return result


def scope_declaration() -> dict[str, Any]:
    """Explicit Layer A scope — prevents over-claiming coverage."""
    return {
        "layer": "A",
        "statement": (
            "Nenhum join crítico da fase inicial do grafo depende de coluna "
            "não inspecionada nas fontes priorizadas (CGU, CVM, RFB, TSE, STF)."
        ),
        "sources_inspected": ["cgu", "cvm", "rfb", "tse", "stf"],
        "sources_deferred": [
            "agenda",
            "datajud",
            "deoab",
            "oab",
            "oab_sp",
            "stf_portal",
            "jurisprudencia",
        ],
        "deferred_rationale": (
            "Fontes diferidas não participam dos joins críticos do núcleo "
            "do grafo de risco na fase inicial. Inspeção será feita quando "
            "essas fontes forem incorporadas ao grafo."
        ),
    }
