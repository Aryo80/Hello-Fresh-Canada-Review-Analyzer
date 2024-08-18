import warnings
from evidently.ui.workspace import Workspace, WorkspaceBase
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore")
from prefect import flow
from prefect import task

import mlflow
import datetime
from graph_generation import  generate_plot
from graph_generation import generate_barplot
from graph_generation import generate_wordcloud
from graph_generation import generate_multiple_barplots

from model_experiment import train_evaluate_rf_model
from model_experiment import vectorize_data
from model_experiment import predict_current_data_batch
from model_experiment import train_tune_svm_model

from cleaning_data import remove_duplicates_and_handle_missing_values
from cleaning_data import preprocess_dataframe 
from cleaning_data import apply_sentiment_analysis
from cleaning_data import categorize_compound_scores
from cleaning_data import split_current_referance_data
from cleaning_data import ingest_data


from report_generation import create_monthly_data_quality_report
from report_generation import create_project
from report_generation import create_monthly_data_drift_test_suite

# Set the MLflow tracking URI to your desired backend (e.g., SQLite, local filesystem)
mlflow.set_tracking_uri("sqlite:///mlflow.db")
# Set the experiment name; this will create a new experiment if it doesn't exist
mlflow.set_experiment("hello-fresh-canada")
reference_date = '2024-01-01'

WORKSPACE = "hello_fresh_canada"
PROJECT_NAME = "hello_fresh_canada"
PROJECT_DESCRIPTION = "Test project using Bank Marketing dataset"

def create_dashboard_project(workspace: str,current,reference):
    ws = Workspace.create(workspace)
    project = create_project(ws,PROJECT_NAME, PROJECT_DESCRIPTION)

    months = current['month'].unique()

    for month in months:
        print(month)
        predict_current_data_batch(current,month)
        report = create_monthly_data_quality_report(month,current,reference)
        if report is not None:
            ws.add_report(project.id, report)

        suite = create_monthly_data_drift_test_suite(month,current,reference)
        if suite is not None:
            ws.add_report(project.id, suite)

        print(f"Added report and suite for month {month}")
    print(f"Project created in workspace: {WORKSPACE}")
    
@flow
def end_to_end_model_orchestration_flow(input_file):
    df = ingest_data(input_file)
    df = remove_duplicates_and_handle_missing_values(df)
    df = preprocess_dataframe(df)
    df = apply_sentiment_analysis(df)
    plot_path = generate_plot(df)
    plot_word = generate_wordcloud(df)
    barplot_path = generate_barplot(df)
    multiple_barplots_path = generate_multiple_barplots(df)
    categorization = categorize_compound_scores(df)


    reference, current = split_current_referance_data(df, reference_date)

    X, y, vectorizer = vectorize_data(reference)
    rf_model, rf_cm = train_evaluate_rf_model(X, y,reference)

#
    create_dashboard_project(WORKSPACE,current,reference)
    return df, multiple_barplots_path

# Example usage
if __name__ == "__main__":
     end_to_end_model_orchestration_flow('data/Hello_Fresh_ca.csv')