# ======================================================
# PISA 2022 – Média de Matemática por UF (Brasil)
# Script ROBUSTO e DIAGNÓSTICO COMPLETO
# ======================================================

import pyreadstat
import pandas as pd
import numpy as np
from pathlib import Path

# ======================================================
# Caminhos
# ======================================================
BASE = Path("data/raw/pisa_2022")
ARQ_STU = BASE / "CY08MSP_STU_QQQ.sav"
ARQ_SCH = BASE / "CY08MSP_SCH_QQQ.sav"

# ======================================================
# 1. LEITURA DAS ESCOLAS (SEM value labels)
# ======================================================
sch, meta_sch = pyreadstat.read_sav(
    ARQ_SCH,
    apply_value_formats=False,
    usecols=["CNT", "CNTSCHID", "REGION"]
)

# filtrar Brasil (robusto)
sch = sch[sch["CNT"] == "BRA"].copy()

# UF vem de REGION
sch.rename(columns={"REGION": "UF"}, inplace=True)

# ======================================================
# 2. LEITURA DOS ALUNOS (SEM value labels)
# ======================================================
stu, meta_stu = pyreadstat.read_sav(
    ARQ_STU,
    apply_value_formats=False,
    usecols=[
        "CNT",
        "CNTSCHID",
        "W_FSTUWT",
        "PV1MATH", "PV2MATH", "PV3MATH", "PV4MATH", "PV5MATH"
    ]
)

# filtrar Brasil
stu = stu[stu["CNT"] == "BRA"].copy()

# ======================================================
# 3. DIAGNÓSTICO INICIAL
# ======================================================
print("=== Diagnóstico inicial ===")
print("Total de alunos BRA:", len(stu))
print("Total de escolas BRA:", sch["CNTSCHID"].nunique())
print()

# ======================================================
# 4. DIAGNÓSTICO DE ESCOLAS SEM UF
# ======================================================
total_escolas = sch["CNTSCHID"].nunique()
escolas_sem_uf = sch[sch["UF"].isna()]["CNTSCHID"].nunique()
escolas_com_uf = total_escolas - escolas_sem_uf

perc_sem_uf = 100 * escolas_sem_uf / total_escolas if total_escolas > 0 else 0
perc_com_uf = 100 * escolas_com_uf / total_escolas if total_escolas > 0 else 0

print("=== Diagnóstico de escolas ===")
print(f"Total de escolas BRA: {total_escolas}")
print(f"Escolas COM UF válida: {escolas_com_uf} ({perc_com_uf:.2f}%)")
print(f"Escolas SEM UF válida: {escolas_sem_uf} ({perc_sem_uf:.2f}%)")
print()

# ======================================================
# 5. MERGE ALUNO → ESCOLA → UF
# ======================================================
df = stu.merge(
    sch[["CNTSCHID", "UF"]],
    on="CNTSCHID",
    how="left"
)

alunos_sem_uf = df["UF"].isna().sum()

print("=== Diagnóstico após merge ===")
print("Alunos sem UF:", alunos_sem_uf)
print(
    "Percentual de alunos sem UF:",
    round(100 * alunos_sem_uf / len(df), 2),
    "%"
)
print()

# manter apenas alunos com UF válida
df = df[df["UF"].notna()].copy()

# ======================================================
# 6. MÉDIA DOS VALORES PLAUSÍVEIS (MATEMÁTICA)
# ======================================================
pv_cols = ["PV1MATH", "PV2MATH", "PV3MATH", "PV4MATH", "PV5MATH"]
df["MATH"] = df[pv_cols].mean(axis=1)

# remover pesos zero ou nulos (segurança)
df = df[df["W_FSTUWT"] > 0].copy()

# ======================================================
# 7. MÉDIA PONDERADA POR UF
# ======================================================
def media_ponderada(x):
    return np.average(x["MATH"], weights=x["W_FSTUWT"])

resultado = (
    df.groupby("UF", as_index=False, observed=False)
      .apply(media_ponderada)
      .rename(columns={None: "MEDIA_PISA_MAT_2022"})
      .sort_values("MEDIA_PISA_MAT_2022", ascending=False)
)

# ======================================================
# 8. SALVAR RESULTADO
# ======================================================
out = Path("data/processed/pisa_2022_media_matematica_por_uf.csv")
resultado.to_csv(out, index=False, encoding="utf-8")

print("=== RESULTADO FINAL ===")
print(resultado)
print(f"\nArquivo salvo em: {out.resolve()}")
