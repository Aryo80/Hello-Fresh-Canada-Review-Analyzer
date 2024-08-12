import datetime
from prefect import task
import pandas as pd
import numpy as np
from evidently.report import Report
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric
from evidently.test_suite import TestSuite
from evidently.test_preset import DataDriftTestPreset
from evidently.ui.dashboards import CounterAgg, DashboardPanelCounter, DashboardPanelPlot, PanelValue, PlotType, ReportFilter, DashboardPanelTestSuite, TestSuitePanelType
from evidently.renderers.html_widgets import WidgetSize
from evidently.ui.workspace import Workspace, WorkspaceBase
from evidently import ColumnMapping
import warnings
from evidently import metrics
from evidently import descriptors
from evidently.tests import *

column_mapping = ColumnMapping(
        target="Rating",
        prediction="prediction",
        numerical_features=["compound_content", "negative_content",
                            "positive_content","compound_header",
                            "word_count_content", "word_count_header","rate","title_count","review_count"],
        text_features=['cleaned_content', "cleaned_header"],
    )
@task
def create_monthly_data_quality_report(month,current,reference):
    current_batch = current[current['month'] == month]
    month = pd.Period(month, freq='M')
    month = month.to_timestamp()
    
    
    # Get the first day of the month
    first_day_of_month = month

    
    if current_batch['Rating'].dropna().empty:
        print(f"Skipping month {month} due to empty 'Rating' column")
        return None
    
    print(f"Processing month: {month}")

    report = Report( 
        metrics=[
            metrics.DatasetSummaryMetric(),
            metrics.DatasetDriftMetric(),
            DatasetDriftMetric(),
            ColumnDriftMetric(column_name="Rating"),
            ColumnDriftMetric(column_name="compound_content"),
            ColumnDriftMetric(column_name="negative_content"),
            ColumnDriftMetric(column_name="positive_content"),
            ColumnDriftMetric(column_name="compound_header"),
            ColumnDriftMetric(column_name="word_count_content"),
            ColumnDriftMetric(column_name="word_count_header"),
            metrics.ColumnDriftMetric(column_name="cleaned_content"),
            metrics.ColumnDriftMetric(column_name="cleaned_header"),
            metrics.ColumnCategoryMetric(column_name="Rating", category=1),
            metrics.ColumnCategoryMetric(column_name="Rating", category=5),
            metrics.ColumnSummaryMetric(column_name="prediction"),
            metrics.ColumnSummaryMetric(column_name="rate"),
            metrics.ColumnSummaryMetric(column_name="review_count"),
            metrics.ColumnSummaryMetric(column_name="title_count"),
            metrics.ColumnSummaryMetric(
                column_name=descriptors.Sentiment(display_name="Sentiment").for_column("cleaned_content")
            ),
        ],
        timestamp=first_day_of_month,  # Adjust if necessary
    )

    report.run(reference_data=reference, current_data=current_batch, column_mapping=column_mapping)
    return report
@task
def create_monthly_data_drift_test_suite(month,current,reference):
    current_batch = current[current['month'] == month]
    month = pd.Period(month, freq='M')
    month = month.to_timestamp()
    
    # Get the first day of the month
    first_day_of_month = month


    

    # Filter the current data for the given month
    
    
    if current_batch.empty:
        print(f"Skipping month {month} due to empty data")
        return None
    
    if current_batch['Rating'].dropna().empty:
        print(f"Skipping month {month} due to empty 'Rating' column")
        return None

    print(f"Processing test suite for month: {month}")
    print(f"Number of rows in current_batch: {len(current_batch)}")

    try:
        suite = TestSuite(
            tests=[
                TestNumberOfColumnsWithMissingValues(),
                TestNumberOfRowsWithMissingValues(),
                TestNumberOfConstantColumns(),
                TestNumberOfDuplicatedRows(),
                TestNumberOfDuplicatedColumns(),
                TestColumnsType(),
                TestNumberOfDriftedColumns(),
            ],
            timestamp = first_day_of_month,  # Ensure it's a datetime object
            
            tags=[]
        )

        suite.run(reference_data=reference, current_data=current_batch)
        return suite

    except Exception as e:
        print(f"An error occurred while creating the test suite: {e}")
        return None
@task
def create_project(workspace: WorkspaceBase,PROJECT_NAME,PROJECT_DESCRIPTION):
    project = workspace.create_project(PROJECT_NAME)
    project.description = PROJECT_DESCRIPTION
    
    project.dashboard.add_panel(
        DashboardPanelCounter(
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            agg=CounterAgg.NONE,
            title="HELLO FRESH CANADA",
        )
    )
    # counters
    project.dashboard.add_panel(
        DashboardPanelCounter(
            title="Model Calls",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            value=PanelValue(
                metric_id="DatasetSummaryMetric",
                field_path=metrics.DatasetSummaryMetric.fields.current.number_of_rows,
                legend="count",
            ),
            text="count",
            agg=CounterAgg.SUM,
            size=WidgetSize.HALF,
        )
    )
    project.dashboard.add_panel(
        DashboardPanelCounter(
            title="Share of Drifted Features",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            value=PanelValue(
                metric_id="DatasetDriftMetric",
                field_path="share_of_drifted_columns",
                legend="share",
            ),
            text="share",
            agg=CounterAgg.LAST,
            size=WidgetSize.HALF,
        )
    )
    # Add a dashboard panel for the mean of the "prediction" column
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title='Review Rating Trend',
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(
                    metric_id="ColumnSummaryMetric",
                    metric_args={"column_name.name": "prediction"},  # Ensure this matches your data column name
                    field_path="current_characteristics.mean",
                    legend='Predicted (daily mean)',
            ),
                PanelValue(
                        metric_id="ColumnSummaryMetric",
                        metric_args={"column_name.name": "rate"},  # Ensure this matches your data column name
                        field_path="current_characteristics.mean",
                        legend='Target (daily mean)',
            ),
        ],
        plot_type=PlotType.LINE,
        size=WidgetSize.HALF,  # Adjust size as needed (e.g., HALF, FULL)
         )
    )
    # Precision
    # project.dashboard.add_panel(
    #     DashboardPanelPlot(
    #         title="Model Precision",
    #         filter=ReportFilter(metadata_values={}, tag_values=[]),
    #         values=[
    #             PanelValue(
    #                 metric_id="ClassificationQualityMetric",
    #                 field_path="current.precision",
    #                 legend="precision",
    #             ),
    #         ],
    #         plot_type=PlotType.LINE,
    #         size=WidgetSize.FULL,
    #     )
    # )
     #Average review sentiment
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title=" Review sentiment",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(
                    metric_id="ColumnSummaryMetric",
                    metric_args={
                        "column_name": descriptors.Sentiment(display_name="Sentiment").for_column("cleaned_content")
                    },
                    field_path="current_characteristics.mean",
                    legend="sentiment (mean)",
                ),
            ],
            plot_type=PlotType.LINE,
            size=WidgetSize.HALF,
        )
    )
    #Review  Vs Title Text Count 
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title='Review  Vs Title Text Count',
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(
                    metric_id="ColumnSummaryMetric",
                    metric_args={"column_name.name": "review_count"},
                    field_path="current_characteristics.mean",
                    legend='Review Text',
                ),
                PanelValue(
                    metric_id="ColumnSummaryMetric",
                    metric_args={"column_name.name": "title_count"},
                    field_path="current_characteristics.mean",
                    legend='Title Text',
            ),
        ],
        plot_type=PlotType.LINE,
        size=WidgetSize.HALF,
        )
    )
    # Rate of 1 and 5 
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title='Share of reviews ranked "1" and "5"',
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(
                    metric_id="ColumnCategoryMetric",
                    metric_args={"column_name.name": "Rating", "category": 1},
                    field_path="current.category_ratio",
                    legend='share of "1"',
                ),
                PanelValue(
                    metric_id="ColumnCategoryMetric",
                    metric_args={"column_name.name": "Rating", "category": 5},
                    field_path="current.category_ratio",
                    legend='share of "5"',
            ),
        ],
        plot_type=PlotType.LINE,
        size=WidgetSize.HALF,
        )
    )


    project.dashboard.add_panel(
        DashboardPanelPlot(
            title="Rating vs Text Review and Title Drift",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(
                    metric_id="ColumnDriftMetric",
                    metric_args={"column_name.name": "Rating"},
                    field_path=ColumnDriftMetric.fields.drift_score,
                    legend="Rating Review  Drift",
                ),
                PanelValue(
                    metric_id="ColumnDriftMetric",
                    metric_args={"column_name.name": "compound_content"},
                    field_path=ColumnDriftMetric.fields.drift_score,
                    legend="Sentiment Drift (Text Review) ",
                ),
                PanelValue(
                    metric_id="ColumnDriftMetric",
                    metric_args={"column_name.name": "compound_header"},
                    field_path=ColumnDriftMetric.fields.drift_score,
                    legend="Sentiment Drift (Title) ",
                ),
            ],
            plot_type=PlotType.LINE,
            size=WidgetSize.HALF
        )
    )
    # review texts (domain classifier ROC AUC)

    values = []
    for col in ["cleaned_header", "cleaned_content"]:
        values.append(
            PanelValue(
                metric_id="ColumnDriftMetric",
                metric_args={"column_name.name": col},
                field_path=metrics.ColumnDriftMetric.fields.drift_score,
                legend=col,
            ),
        )
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title="Data Drift: review texts (domain classifier ROC AUC) ",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=values,
            plot_type=PlotType.LINE,
            size=WidgetSize.HALF,
        )
    )
    # Word Count Pannel 

    for col in ["word_count_content", "word_count_header"]:
        print(col)

    
    values = []
    leg_cnt = -1
    legends = ['Review Text Count','Header Text Count']

    for col in ["word_count_content", "word_count_header"]:
        leg_cnt+= 1
        values.append(
            PanelValue(
                metric_id="ColumnDriftMetric",
                metric_args={"column_name.name": col},
                field_path=metrics.ColumnDriftMetric.fields.drift_score,
                legend=f"{legends[leg_cnt]}",
            ),
        )
            
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title="Data Drift: numerical features (Wasserstein distance)",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=values,
            plot_type=PlotType.LINE,
            size=WidgetSize.HALF,
        )
    )
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title="Dataset Drift",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(
                    metric_id="DatasetDriftMetric",
                    field_path="share_of_drifted_columns",
                    legend="Drift Share"
                ),
            ],
            plot_type=PlotType.BAR,
            size=WidgetSize.HALF
        )
    )

    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="Data Drift Tests",
            filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
            size=WidgetSize.HALF
        )
    )

    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="Data Drift Tests: Detailed",
            filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
            size=WidgetSize.HALF,
            panel_type=TestSuitePanelType.DETAILED
        )
    )

    project.save()
    return project


