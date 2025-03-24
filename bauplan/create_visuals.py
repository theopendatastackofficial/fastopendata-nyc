import bauplan
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import os
import sys


def get_repo_root() -> str:
    """Get the root directory of the git repository."""
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Keep going up until we find .git directory or reach root
    while current_dir != os.path.dirname(current_dir):  # Stop at filesystem root
        if os.path.isdir(os.path.join(current_dir, '.git')):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    raise RuntimeError("Could not find git repository root")


def save_corr_heatmap(
    ref_branch: str,
    client: bauplan.Client,
    filename: str = "heatmap.png",
    figsize: tuple = (10, 8),
    cmap: str = 'coolwarm'
) -> None:
    """Generate and save a correlation heatmap for the taxi training dataset."""
    print("Starting correlation heatmap generation...")
    
    repo_root = get_repo_root()
    output_dir = os.path.join(repo_root, "data", "bauplan_ml_outputs")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Ensured output directory exists at: {output_dir}")
    
    print("Querying taxi training dataset...")
    df = client.query("SELECT * FROM bauplan.taxi_training_dataset", ref=ref_branch).to_pandas()
    print("Calculating correlation matrix...")
    corr_matrix = df.corr()
    
    print("Generating heatmap plot...")
    plt.figure(figsize=figsize)
    sns.heatmap(corr_matrix, annot=True, cmap=cmap, linewidths=0.5)
    plt.title('Heatmap of Feature Correlations')
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, filename)
    plt.savefig(output_path)
    plt.close()
    print(f"Heatmap saved to: {output_path}")


def save_actual_vs_predicted_plot(
    ref_branch: str,
    client: bauplan.Client,
    filename: str = "actual_vs_predicted.png",
    figsize: tuple = (10, 6)
) -> None:
    """Generate and save a scatter plot comparing actual vs predicted tips."""
    print("Starting actual vs predicted plot generation...")
    
    repo_root = get_repo_root()
    output_dir = os.path.join(repo_root, "data", "bauplan_ml_outputs")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Ensured output directory exists at: {output_dir}")
    
    print("Querying tip predictions dataset...")
    df = client.query("SELECT * FROM bauplan.tip_predictions", ref=ref_branch).to_pandas()
    
    print("Generating scatter plot...")
    plt.figure(figsize=figsize)
    plt.scatter(df['tips'], df['predictions'], alpha=0.6, c='blue', label='Predictions', s=100)
    plt.scatter(df['tips'], df['tips'], alpha=0.6, c='pink', label='Actual Values', s=100)
    plt.plot([min(df['tips']), max(df['tips'])], [min(df['tips']), max(df['tips'])], 
             color='red', linewidth=1, label='y=x line')
    plt.xlabel('Actual Tips')
    plt.ylabel('Predicted Tips')
    plt.title('Actual vs Predicted Tips')
    plt.legend()
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, filename)
    plt.savefig(output_path)
    plt.close()
    print(f"Actual vs Predicted plot saved to: {output_path}")


def main():
    """Main function to parse arguments and generate visualizations."""
    print("Starting visualization script...")
    
    client = bauplan.Client()
    print("Bauplan client initialized")
    
    parser = argparse.ArgumentParser(description='Generate data visualizations for ML analysis.')
    parser.add_argument('--branch', type=str, required=True, 
                       help='Reference parameter for bauplan query.')
    args = parser.parse_args()
    print(f"Parsed arguments with branch: {args.branch}")
    
    save_corr_heatmap(ref_branch=args.branch, client=client)
    save_actual_vs_predicted_plot(ref_branch=args.branch, client=client)
    print("All visualizations completed successfully!")


if __name__ == '__main__':
    main()