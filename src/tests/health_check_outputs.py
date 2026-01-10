"""
================================================================================
SCRIPT:        src/tests/health_check_outputs.py
DESCRIPTION:   Auditoria de Qualidade (QA) dos arquivos processados.
================================================================================
"""
import pandas as pd
import os
from pathlib import Path

# Configuração de Caminhos
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent.parent
PROCESSED_DIR = PROJECT_ROOT / 'data' / 'processed'

EXPECTED_FILES = [
    {'name': 'pisa_2015_states.csv', 'type': 'STATE', 'min_rows': 20},
    {'name': 'pisa_2018_regional_summary.csv', 'type': 'REGION', 'min_rows': 5},
    {'name': 'pisa_2022_regional_summary.csv', 'type': 'REGION', 'min_rows': 5}
]

def run_health_check():
    print("="*60)
    print("      HEALTH CHECK - RESULTADOS PISA (ETL)")
    print("="*60)
    
    if not PROCESSED_DIR.exists():
        print(f"[CRÍTICO] Pasta não encontrada: {PROCESSED_DIR}")
        return

    all_passed = True

    for item in EXPECTED_FILES:
        fpath = PROCESSED_DIR / item['name']
        print(f"\n[CHECK] {item['name']}...")
        
        # 1. Existência
        if not fpath.exists():
            print(f"   ❌ ARQUIVO NÃO ENCONTRADO!")
            all_passed = False
            continue
            
        try:
            df = pd.read_csv(fpath)
            
            # 2. Volume de Dados
            rows, cols = df.shape
            print(f"   📊 Dimensões: {rows} linhas x {cols} colunas")
            
            if rows < item['min_rows']:
                print(f"   ❌ ALERTA: Número de linhas suspeito (Esperado > {item['min_rows']})")
                all_passed = False
            else:
                print(f"   ✅ Volume de dados OK")
                
            # 3. Sanidade das Médias (Cognitive_Global_Mean)
            if 'Cognitive_Global_Mean' in df.columns:
                mean_val = df['Cognitive_Global_Mean'].mean()
                min_val = df['Cognitive_Global_Mean'].min()
                max_val = df['Cognitive_Global_Mean'].max()
                
                print(f"   🧠 Score Global Médio: {mean_val:.2f} (Min: {min_val} | Max: {max_val})")
                
                if mean_val < 300 or mean_val > 600:
                    print("   ⚠️ AVISO: Médias parecem fora da escala padrão do PISA (300-600).")
                else:
                    print("   ✅ Escala de notas OK")
            else:
                print("   ❌ Coluna 'Cognitive_Global_Mean' ausente!")
                all_passed = False
                
            # 4. Amostra
            print(f"   Top 1 Region/State: {df.iloc[0, 0]} (Nota: {df.iloc[0]['Cognitive_Global_Mean']})")

        except Exception as e:
            print(f"   ❌ Erro ao ler arquivo: {e}")
            all_passed = False

    print("\n" + "="*60)
    if all_passed:
        print("RESULTADO FINAL: ✅ SUCESSO. Os dados parecem consistentes.")
    else:
        print("RESULTADO FINAL: ⚠️ ATENÇÃO. Verifique os erros acima.")
    print("="*60)

if __name__ == "__main__":
    run_health_check()