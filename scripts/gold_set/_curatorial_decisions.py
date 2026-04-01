"""Curatorial adjudication decisions keyed by intrinsic record fingerprint.

Key: match_id (for donation/sanction records) or
     donor_identity_key|entity_name (for ambiguous records without match_id).

These decisions were produced by individual inspection of each record's
evidence (names, strategy, score, CPF presence, name patterns) using
conservative criteria:

1. Token reorder + CPF -> correct (same tokens, confirmed identity)
2. Accent-only difference + CPF -> correct (orthographic variant)
3. Preposition-only difference (DE/DA) + CPF -> correct (optional in BR names)
4. Same-company PJ with minor formatting -> correct
5. Distinctive PJ sanction name -> correct
6. Name containment with extra surname -> ambiguous (could be relative)
7. Fuzzy without CPF -> ambiguous (no document confirmation)
8. SCL degree-2 indirect links -> ambiguous (corporate relationship, not identity)
9. Sanction PF without tax ID -> ambiguous (no identity confirmation)
10. MARIA->MARINA, MARIA->MARIO (dist=1 but distinct names) -> ambiguous
11. Different surnames in dist=2 (SOUSA->MOURA, VELHO->VENSO) -> incorrect
12. Different first names -> incorrect
"""

from __future__ import annotations

ADJUDICATION_TYPE = "operator_delegated_curatorial"
ADJUDICATOR = "operator_delegated_curatorial"


def record_fingerprint(rec: dict) -> str:
    """Compute intrinsic fingerprint for a gold set record.

    Uses match_id when available (stable across regenerations),
    falls back to donor_identity_key|entity_name for records without match_id.
    """
    mid = rec.get("match_id")
    if mid:
        return str(mid)
    return f"{rec.get('donor_identity_key', '')}|{rec.get('entity_name', '')}"


# fingerprint -> (final_label, justification)
DECISIONS: dict[str, tuple[str, str]] = {
    # --- counsel_match ---
    "dm-ba5bb17e912277d5": ("correct", "Token reorder + accent (ANTONIO JOSE ~ JOSÉ ANTÔNIO). CPF present."),
    "dm-9c49f2c330253c46": ("correct", "Accent-only (AURELIO ~ AURÉLIO). CPF present."),
    "dm-af1bf35942244948": ("ambiguous", "Accent-only (JUNIOR ~ JÚNIOR) but no CPF."),
    "dm-c8174a13946c9fe6": ("ambiguous", "Entity has extra AZEVEDO surname + reorder. Cannot confirm."),
    "dm-d50139fdec0fdbe8": ("incorrect", "ENOILSON→NILSON different first names despite shared suffix."),
    "dm-484ea431a78c648a": ("correct", "Exact name match with CPF."),
    "dm-f3c12f7790188775": ("correct", "Exact name match with CPF."),
    "dm-10a4b4bf4166fa0d": ("ambiguous", "Entity has extra CARMO surname."),
    "dm-178caa8a5db1f6e0": ("ambiguous", "Entity has extra MAROJA surname."),
    "dm-bc8182a551a3f810": ("ambiguous", "Entity has extra MOURA surname."),
    "dm-2070579ea4d0bf18": ("correct", "Exact name match with CPF."),
    "dm-ef09a123c3f83fbd": ("correct", "Exact name match with CPF."),
    "dm-d70b4aebd34bab8d": ("correct", "Exact name match with CPF."),
    "dm-4998a89b186496dd": ("ambiguous", "Exact match but abbreviated name (GILSON L DA SILVA) without CPF."),
    "dm-91b86a3b9a372083": ("ambiguous", "Entity has extra XAVIER + reorder."),
    # --- jaccard_borderline ---
    "dm-927fa39d075fe10f": ("ambiguous", "Extra LOPES surname + reorder. Different person risk."),
    "dm-b1dc0380f4fa6676": ("ambiguous", "MARILENE ALVES SANTNNA vs MARILENE ALVES — extra token, no CPF."),
    "dm-7b1002cb6185deec": ("ambiguous", "Donor has extra JOSE first name. Could be compound name or father/son."),
    "dm-f2908109f1984332": ("ambiguous", "Extra CRISTINA + reorder. Name containment insufficient for identity."),
    "dm-8f371cb56aa628e9": ("ambiguous", "Donor has extra LAURENTINO. Cannot confirm without independent evidence."),
    "dm-e46b7f805b3cd48e": ("ambiguous", "Donor has extra MARIA middle name. Common addition pattern."),
    "dm-6c892b8e512a86ca": ("ambiguous", "Entity has extra SOARES surname. Different person risk."),
    "dm-cf6dc631b5a12b2b": ("ambiguous", "Extra IZE + reorder. MARIA GLORIA ≠ GLÓRIA MARIA IZE definitively."),
    "dm-00c2c10235204ed3": ("ambiguous", "Extra BATISTA + reorder, no CPF."),
    "dm-b2a1e81237d30afb": ("correct", "GONCALVES LIMA vs GONÇALVES DE LIMA. Accent + preposition DE. CPF."),
    "dm-9684aca233c76b51": ("ambiguous", "Entity has extra FILHO (generational suffix) — could be father/son."),
    "dm-522c65374ecaa9bb": ("ambiguous", "Containment + reorder with extra OLIVEIRA surname."),
    "dm-a909b3af0fc5cf3b": ("ambiguous", "Reorder + donor has extra AMORIM surname."),
    "dm-e3442b86f20d276a": ("ambiguous", "Donor has extra SANTOS — very common surname, not sufficient for identity."),
    "dm-90bc3c6268368d0f": ("correct", "OLIVEIRA DE ARAUJO vs DE OLIVEIRA DE ARAUJO. Preposition only. CPF."),
    "dm-8170ae1bf55c0b7e": ("ambiguous", "Donor has extra TAVARES. Different person risk."),
    "dm-0d9cc33cc14c535b": ("ambiguous", "Entity has extra VILAR surname."),
    "dm-01c8124ea7549c9c": ("ambiguous", "Donor has extra SILVA — extremely common, insufficient for identity."),
    "dm-8c1a7b5f641a55e7": ("ambiguous", "Entity has extra MARIA middle name."),
    "dm-e94794f8972d8dfa": ("ambiguous", "Entity has extra JOSE first name, no CPF."),
    # --- jaccard_high ---
    "dm-0c4ebdf997aa9c76": ("ambiguous", "Token reorder without CPF. Likely same person but no document confirmation."),
    "dm-3631977b3feb581f": ("correct", "Token reorder (GILSON DE JESUS SANTOS ~ GILSON SANTOS DE JESUS). CPF."),
    "dm-10a78d1fd4070e14": ("ambiguous", "R.M.FERREIRA SERVICOS-ME vs M.F.R.S. PJ abbreviation mismatch."),
    "dm-37d6cd78bdf71b89": ("ambiguous", "Accent-only difference (JOSÉ ÂNGELO ~ JOSE ANGELO) without CPF."),
    "dm-510216ddd3666537": ("ambiguous", "Token reorder without CPF."),
    "dm-41ac29d934cc3755": ("correct", "Token reorder (DOMINGOS LEANDRO ~ LEANDRO DOMINGOS). CPF."),
    "dm-149f732704d2ea81": ("ambiguous", "Accent-only difference without CPF."),
    "dm-fe8ea0cc2e4e9cc4": ("correct", "Accent-only difference (MARCOS ANTONIO ~ MARCOS ANTÔNIO) with CPF."),
    "dm-2070045916ae0380": ("correct", "Accent-only difference (JOSE JERONIMO ~ JOSÉ JERÔNIMO) with CPF."),
    "dm-212af5a1a264224e": ("ambiguous", "Token reorder without CPF."),
    # --- levenshtein_dist1 ---
    "dm-e8f1d921a82a99e3": ("ambiguous", "MAVIA→MARIA dist=1, no CPF. Likely typo but no confirmation."),
    "dm-8e891b2633f4222b": ("ambiguous", "SILVASANTOS→SILVA SANTOS spacing error, no CPF."),
    "dm-d0020ab05cc61e56": ("ambiguous", "ATAIDE→ATAYDE known variant, no CPF."),
    "dm-0123366d9ba78b36": ("ambiguous", "MARIA→MARINA dist=1 but these are distinct established Brazilian names."),
    "dm-8c8c9606a24a5d56": ("correct", "SOUZA→SOUSA well-known Portuguese spelling variant. CPF present."),
    "dm-878a5114f9f35cf9": ("ambiguous", "MARIA→MÁRIO dist=1 but different gendered names (feminine vs masculine)."),
    "dm-a6d5b829c546024f": ("ambiguous", "ELISABETH→ELIZABETH known variant, no CPF."),
    "dm-e416713877a39b8b": ("correct", "NPEREIRA→PEREIRA clear typo (stray N prefix). CPF present."),
    "dm-90057ca13f79ab7a": ("correct", "JAYLTON→JAÍLTON accent/formatting variant only. CPF present."),
    "dm-7cfa11ca26318e9b": ("correct", "SOUSA→SOUZA well-known variant. CPF present."),
    "dm-f33269e9f355ccdd": ("ambiguous", "SALLES→SILLES significant vowel change (A→I), no CPF."),
    "dm-9e18d6cfbb11348b": ("ambiguous", "LOBARO→LOBATO could be typo or different surname, no CPF."),
    "dm-ad4bc792c003bb95": ("ambiguous", "SANTO→SANTOS missing final S, no CPF."),
    "dm-36680093776b8513": ("correct", "ALDENES→ALDENIS orthographic variant of uncommon name. CPF present."),
    "dm-ca109c88fb98309b": ("ambiguous", "SANCHES→SANCHEZ Portuguese/Spanish variant, no CPF."),
    "dm-a1ce84526c28d184": ("correct", "SOUSA→SOUZA + accent. CPF present."),
    "dm-6043c41e870fcddc": ("correct", "SERVICO→SERVICOS singular/plural PJ name. CPF present. Same company."),
    "dm-3e7febe6d8d3799e": ("ambiguous", "ANTONIO→ANTONIA masculine/feminine gender flip in middle name. Uncertain."),
    "dm-00b16560d79c540a": ("ambiguous", "JORIMAR→JOSIMAR different first names despite dist=1, no CPF."),
    "dm-6ca439c81436ac8c": ("correct", "SOUZA→SOUSA well-known variant. CPF present."),
    # --- levenshtein_dist2 ---
    "dm-4a6c5269e9c75648": ("incorrect", "VELHO→VENSO completely different surnames despite same first name."),
    "dm-8a07225f72dcd75e": ("incorrect", "SOUSA→MOURA different surnames (not a known variant)."),
    "dm-2e1a13133fa220ba": ("incorrect", "ADILO→DANILO different first names."),
    "dm-40eeacf7e3358327": ("incorrect", "LOYENS→LOPES different surnames."),
    "dm-eea273a0e07e77cc": ("ambiguous", "ALVES→ALVARES related patronymic variants. Genuinely uncertain."),
    "dm-a8c44ea3cd668cea": ("incorrect", "MANOEL→RANGEL completely different surnames."),
    "dm-fd541f07e70b61a5": ("incorrect", "DOJIVAL→DORIVAN different first names, no CPF."),
    "dm-38529bea390305c9": ("incorrect", "MARIA→MANICA different second tokens, no CPF."),
    "dm-087d24aef58fd4b2": ("ambiguous", "LEODATA→LEODINA similar uncommon names, genuinely uncertain."),
    "dm-b82cfbf37b11fd1e": ("incorrect", "MAURILIKO→MAURÍCIO first name dist=3, confirmed different."),
    "dm-37799e47706ad498": ("incorrect", "SOUSA→MOURA different surnames."),
    "dm-10d3d0ea94fe7df7": ("incorrect", "CONSTRUTORA MENDES SA LTDA vs CONSTRUTORA C. MENDES. Different PJ."),
    "dm-8c3de8af3f9313c8": ("incorrect", "PEREIRA→FERREIRA different surnames."),
    "dm-a2a7744a096c3bfc": ("incorrect", "MALBIR→VALDIR different first names."),
    "dm-8fc3c73b1eb5dd79": ("incorrect", "SOUZA→MOURA different surnames."),
    "dm-f55a35e657491734": ("correct", "PARTICIPACOES→PARTICIPAÇÕES + preposition E. Same PJ, formatting only."),
    "dm-a6df96accd98ed7e": ("ambiguous", "IRISNETE→IRANETE similar uncommon names. Genuinely uncertain."),
    "dm-aa98e9c8b35962f9": ("ambiguous", "GRASIANI→GRAZIANE orthographic variants of same name. Uncertain."),
    "dm-3f28c1edccd8822f": ("incorrect", "ROSA→COSTA different middle surnames."),
    "dm-ede6986f23665c71": ("incorrect", "MACEDO→MACHADO different surnames."),
    # --- sanction_match ---
    "sm-eff10b08d7291f32": ("ambiguous", "Common name MARCO ANTONIO DOS SANTOS without tax ID."),
    "sm-d5ddd2167a00ffcb": ("ambiguous", "Uncommon name but no tax ID to confirm cross-source identity."),
    "sm-74cebac4afccc303": ("ambiguous", "Uncommon name but no tax ID."),
    "sm-10626387602e545a": ("ambiguous", "No tax ID confirmation."),
    "sm-b4362a5bb232bc1b": ("ambiguous", "Common name pattern without tax ID."),
    "sm-239f0bf6c4c742a4": ("ambiguous", "Fuzzy sanction match without tax ID."),
    "sm-aa45ab8719b1dbce": ("correct", "Distinctive PJ name (O & M EMPREENDIMENTOS E CONSTRUCOES LTDA)."),
    "sm-27e2692c9e536665": ("correct", "Distinctive PJ name (LINKNET TECNOLOGIA E TELECOMUNICACOES LTDA)."),
    "sm-8dc1ebcb23b66cdd": ("ambiguous", "Fuzzy sanction match without tax ID."),
    "sm-4074a61de3870382": ("ambiguous", "No tax ID confirmation."),
    "sm-00d2e51a2328eb5f": ("ambiguous", "No tax ID confirmation."),
    "sm-0b7a86bc54ba8ed9": ("correct", "Distinctive PJ name (CCS SERVICOS TERCEIRIZADOS LTDA)."),
    "sm-e0b383c52f4a95bb": ("ambiguous", "No tax ID confirmation."),
    "sm-b9c06a9b2eb0e6f5": ("correct", "Distinctive PJ name (AGAPE CONSTRUCOES E SERVICOS LTDA)."),
    "sm-5db233e719074339": ("correct", "Distinctive PJ name (B & F TRANSPORTES E TURISMO LTDA)."),
    # --- scl_degree2 ---
    "scl-69c9c461cdf3012e": ("ambiguous", "Indirect SCL link via consortium. Corporate relationship."),
    "scl-5832e4e431c9cf87": ("ambiguous", "Indirect SCL link. Different companies in same consortium."),
    "scl-0352e692c1f7b767": ("ambiguous", "Indirect SCL link via consortium."),
    "scl-cd849e3bfe1cec86": ("ambiguous", "Indirect SCL link via consortium."),
    "scl-c5f2afd327874334": ("ambiguous", "Indirect SCL link — company to person through consortium."),
    "scl-f66154bb41211ce2": ("ambiguous", "Indirect SCL link. Different companies."),
    "scl-2326234f22ffc57a": ("ambiguous", "Indirect SCL link via corporate ownership chain."),
    "scl-6e75f0e80f29160d": ("ambiguous", "Indirect SCL link — company to individual partner."),
    "scl-95581605b980d769": ("ambiguous", "Indirect SCL link via consortium (duplicate bridge)."),
    "scl-33b801afd05c0158": ("ambiguous", "Indirect SCL link — company to person."),
    "scl-196280e7c08064e8": ("ambiguous", "Indirect SCL link. Different companies in same consortium."),
    "scl-8fa2aefcd1a1ce21": ("ambiguous", "Indirect SCL link — company to individual."),
    "scl-95a1c393f081b9e6": ("ambiguous", "Indirect SCL link — company to partner."),
    "scl-1419c2feb9cc75ee": ("ambiguous", "Indirect SCL link via consortium."),
    "scl-25212083ef00b368": ("ambiguous", "Indirect SCL link. Different companies in same consortium."),
}
