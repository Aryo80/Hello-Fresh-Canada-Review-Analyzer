# Check if the virtual environment exists, if not, create it
if [ ! -d "myenv" ]; then
  python -m venv myenv
  source myenv/bin/activate
  pip install -r requirements.txt
fi

# Update package lists and install tmux if not installed
sudo apt-get update
sudo apt-get install -y tmux

# Activate the virtual environment and install setuptools if needed
source myenv/bin/activate
pip install setuptools

# Start a new tmux session named 'my_session' in detached mode
tmux new-session -d -s my_session

# Split the window into four equal vertical panes
tmux split-window -h -t my_session
tmux split-window -h -t my_session:0.1
tmux split-window -v -t my_session:0
tmux split-window -v -t my_session:0.0

# Set layout to tiled to distribute panes evenly
tmux select-layout -t my_session tiled

# Run Evidently UI in the first pane
tmux send-keys -t my_session:0.0 'source myenv/bin/activate && evidently ui --workspace hello_fresh_canada --port 8000' C-m

# Run MLflow UI in the second pane
tmux send-keys -t my_session:0.1 'source myenv/bin/activate && mlflow ui --backend-store-uri sqlite:///mlflow.db --port 8585' C-m

# Run Prefect service in the third pane with explicit port 4200
tmux send-keys -t my_session:0.2 'source myenv/bin/activate && prefect server start --port 4200' C-m

# Create a new window for running the app
tmux new-window -t my_session:1 'source myenv/bin/activate && python app.py'

# Attach to the tmux session
tmux attach-session -t my_session
