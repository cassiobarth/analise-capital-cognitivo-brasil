import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def main():
    root = os.path.dirname(__file__)
    dados_dir = os.path.join(root, 'dados')
    path_master = os.path.join(dados_dir, 'master_dataset_27uf.csv')

    df = pd.read_csv(path_master)

    # Selecionar colunas numéricas relevantes
    numeric = df.select_dtypes(include=[np.number]).copy()
    # Remover identificadores de ano se preferir (mantemos anos para referência)
    # Calcular correlações de Pearson e Spearman
    corr_pearson = numeric.corr(method='pearson')
    corr_spearman = numeric.corr(method='spearman')

    # Salvar matrizes
    corr_pearson.to_csv(os.path.join(dados_dir, 'correlation_pearson.csv'))
    corr_spearman.to_csv(os.path.join(dados_dir, 'correlation_spearman.csv'))

    # Plot heatmap (Pearson)
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_pearson, annot=True, fmt='.2f', cmap='coolwarm', square=True, cbar_kws={'shrink': .8})
    plt.title('Correlation matrix (Pearson) - master_dataset_27uf')
    out_fig = os.path.join(dados_dir, 'correlation_heatmap_pearson.png')
    plt.tight_layout()
    plt.savefig(out_fig, dpi=150)
    plt.close()

    print('\nMatrizes de correlação salvas em:')
    print(' -', os.path.join(dados_dir, 'correlation_pearson.csv'))
    print(' -', os.path.join(dados_dir, 'correlation_spearman.csv'))
    print('Heatmap salvo em:', out_fig)


if __name__ == '__main__':
    main()
