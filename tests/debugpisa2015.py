import pandas as pd
from pathlib import Path

# Path to the 2015 file (using the 'Pisa' folder structure we fixed)
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
RAW_FILE = PROJECT_ROOT / 'data' / 'raw' / 'Pisa' / 'pisa_2015' / 'CY6_MS_CMB_STU_QQQ.sav'

def inspect_pisa():
    print(f"🕵️ INSPECTING: {RAW_FILE.name}")
    
    if not RAW_FILE.exists():
        print("❌ File not found!")
        return

    try:
        # Read only the columns we need to check
        df = pd.read_spss(RAW_FILE, usecols=['CNT', 'STRATUM'])
        
        # Filter Brazil
        df_bra = df[df['CNT'] == 'BRA']
        
        print(f"   - Total Brazil Rows: {len(df_bra)}")
        
        if len(df_bra) > 0:
            print("\n--- SAMPLE STRATUM CODES (First 10) ---")
            print(df_bra['STRATUM'].head(10).tolist())
            
            print("\n--- UNIQUE STRATUM CODES (First 20) ---")
            print(df_bra['STRATUM'].unique()[:20])
        else:
            print("⚠️ No Brazil data found in the file!")
            
    except Exception as e:
        print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    inspect_pisa()