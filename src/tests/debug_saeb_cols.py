# src/cog/debug_saeb_cols.py
import zipfile

ZIP_PATH = 'data/raw/microdados_saeb_2023.zip'

with zipfile.ZipFile(ZIP_PATH) as z:
    target = next(f for f in z.namelist() if 'TS_ESCOLA.csv' in f)
    with z.open(target) as f:
        header = f.readline().decode('latin1').strip().split(';')
        print("COLUMNS FOUND:")
        print(header)