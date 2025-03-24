# A machine learning pipeline

In this example, we demonstrate how to organize and run a simple machine learning project using Bauplan. We’ll create and execute a pipeline that:

Ingests raw data from the TLC NY taxi dataset.

Transforms the data into a training dataset with the appropriate features.

Trains a Linear Regression model to predict the tip amount for taxi rides.

Writes the predictions to an Iceberg table for further analysis.

Additionally, we’ll leverage the Bauplan SDK to create visualization of both the dataset and the generated predictions interactively. 

## Load extra dependencies 
```shell
uv sync --extra bauplan
cd bauplan
```

## Run the pipeline in the cloud
Make sure you have your bauplan token right to run stuff in the sandbox. 
If you don't have one, go here and generate an api token `https://dashboard.use1.aprod.bauplanlabs.com/`


```shell
bauplan run --cache off --preview head
```
This will run a pipeline that takes the open dataset from the NY taxi rides and trains a model to predict the tip amount of future trips.
The pipeline is composed of three nodes: 

```mermaid
graph LR
    A[taxi_training_dataset] --> B[train_regression_model] --> C[tip_predictions]
```
The project will prepare a training dataset with Pandas, train a regression model using scikit-learn and write a table with the predictions `tip_predictions`.

## Visualize the result

```shell 
python create_visuals.py --branch christianc.fast_open_data
```
This script will save two png files in the `data/bauplan_ml_outputs` folder. One is a heatmap for the visualization of the training dataset, the other is visualized the actual data vs the predicted. 
