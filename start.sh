#!/bin/bash

# Update package lists and install tmux
sudo apt-get update
sudo apt-get install -y tmux

# Start a new tmux session named 'my_session' in detached mode
tmux new-session -d -s my_session

# Create a virtual environment and install dependencies
tmux send-keys -t my_session 'python -m venv myenv' C-m
tmux send-keys -t my_session 'source myenv/bin/activate' C-m
tmux send-keys -t my_session 'pip install -r requirements.txt' C-m

# Split the window into four equal vertical panes
# First split horizontally
tmux split-window -h -t my_session

# Split the new right pane horizontally
tmux split-window -h -t my_session:0.1

# Split the left pane vertically
tmux split-window -v -t my_session:0

# Split the new bottom-left pane vertically
tmux split-window -v -t my_session:0.0

# Set layout to tiled to distribute panes evenly
tmux select-layout -t my_session tiled

# Run Evidently UI in the first pane
tmux send-keys -t my_session:0.0 'source myenv/bin/activate; evidently ui --workspace hello_fresh_canada' C-m

# Run MLflow UI in the second pane
tmux send-keys -t my_session:0.1 'source myenv/bin/activate; mlflow ui --backend-store-uri sqlite:///mlflow.db --port 8585' C-m

# Run Prefect service in the third pane
tmux send-keys -t my_session:0.2 'source myenv/bin/activate; prefect server start' C-m

# Create a new window for running the app
tmux new-window -t my_session:1 'source myenv/bin/activate; python app.py'

# Attach to the tmux session
tmux attach-session -t my_session