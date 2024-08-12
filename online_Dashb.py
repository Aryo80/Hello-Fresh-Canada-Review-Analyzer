import datetime
import os.path
import time
import pandas as pd

from requests.exceptions import RequestException
from sklearn import datasets

# Importing modules from evidently package
from evidently.collector.client import CollectorClient
from evidently.collector.config import CollectorConfig, IntervalTrigger, ReportConfig

from evidently.test_suite import TestSuite
from evidently.test_preset import DataQualityTestPreset

from evidently.ui.dashboards import DashboardPanelTestSuite
from evidently.ui.dashboards import ReportFilter
from evidently.ui.dashboards import TestFilter
from evidently.ui.dashboards import TestSuitePanelType
from evidently.renderers.html_widgets import WidgetSize
from evidently.ui.workspace import Workspace
import pandas as pd

# Setting up constants
COLLECTOR_ID = "default"
COLLECTOR_TEST_ID = "default_test"

PROJECT_NAME = "Online monitoring as a service"
WORKSACE_PATH = "Analytics Vidhya Evidently Guide"

# Creating a client
client = CollectorClient("http://localhost:8001")

# Loading data
df =pd.read_csv("DelayedFlights.csv")
ref_data=df[:5000]
batch_size=200
curr_data=df[5000:7000]

# Function to create a test suite
def test_suite():
    suite= TestSuite(tests=[DataQualityTestPreset()],tags=[])
    suite.run(reference_data=ref_data, current_data=curr_data)
    return ReportConfig.from_test_suite(suite)  

# Function to setup workspace
def workspace_setup():
    ws = Workspace.create(WORKSACE_PATH)
    project = ws.create_project(PROJECT_NAME)
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="Data Drift Tests",
            filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
            size=WidgetSize.HALF
        )
    )
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="Data Drift Tests",
            filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
            size=WidgetSize.HALF,
            panel_type=TestSuitePanelType.DETAILED
        )
    )
    project.save()

    # Function to setup config
def setup_config():
    ws = Workspace.create(WORKSACE_PATH)
    project = ws.search_project(PROJECT_NAME)[0]

    test_conf = CollectorConfig(trigger=IntervalTrigger(interval=5),
                                report_config=test_suite(), project_id=str(project.id))

    client.create_collector(COLLECTOR_TEST_ID, test_conf)
    client.set_reference(COLLECTOR_TEST_ID, ref_data)  


# Function to send data
def send_data():
    print("Start sending data")
    for i in range(2):
        try:
            data = curr_data[i * batch_size : (i + 1) * batch_size]
            client.send_data(COLLECTOR_TEST_ID, data)
            print("sent")
        except RequestException as e:
            print(f"collector service is not available: {e.__class__.__name__}")
        time.sleep(1)     

# Main function
def main():
    workspace_setup()
    setup_config()
    send_data()
# Running the main function
if __name__ =='__main__':
    main()