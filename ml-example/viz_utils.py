import bauplan
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse


def save_corr_heatmap(
    ref_branch: str,
    client: bauplan.Client,
    filename="heatmap.png",
    figsize=(10, 8),
    cmap='coolwarm',
):
    df = client.query("SELECT * FROM bauplan.taxi_training_dataset", ref=ref_branch).to_pandas()
    corr_matrix = df.corr()
    plt.figure(figsize=figsize)
    sns.heatmap(corr_matrix, annot=True, cmap=cmap, linewidths=0.5)
    plt.title('Heatmap of Feature Correlations')
    plt.tight_layout()  # Ensures nothing is cut off
    plt.savefig(filename)
    plt.close()


def save_actual_vs_predicted_plot(
        ref_branch: str,
        client: bauplan.Client,
        filename="actual_vs_predicted.png",
        figsize=(10, 6)
):
    df = client.query("SELECT * FROM bauplan.tip_predictions", ref=ref_branch).to_pandas()
    plt.figure(figsize=figsize)
    plt.scatter(df['tips'], df['predictions'], alpha=0.6, c='blue', label='Predictions', s=100)
    plt.scatter(df['tips'], df['tips'], alpha=0.6, c='pink', label='Actual Values', s=100)
    plt.plot([min(df['tips']), max(df['tips'])], [min(df['tips']), max(df['tips'])], color='red', linewidth=1, label='y=x line')
    plt.xlabel('Actual Tips')
    plt.ylabel('Predicted Tips')
    plt.title('Actual vs Predicted Tips')
    plt.legend()
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


if __name__ == '__main__':
    client = bauplan.Client()
    parser = argparse.ArgumentParser(description='Generate correlation heatmap.')
    parser.add_argument('--branch', type=str, required=True, help='Reference parameter for bauplan query.')
    args = parser.parse_args()
    save_corr_heatmap(ref_branch=args.branch, client=client)
    save_actual_vs_predicted_plot(ref_branch=args.branch, client=client)
    print('all done: check out your data visualization')
