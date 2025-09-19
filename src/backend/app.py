import sys
import os
from src.backend import app  # Import the app from __init__.py

# Add project root and src to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
src_path = os.path.join(project_root, 'src')
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import routes after app is defined
from .routes import *

if __name__ == "__main__":
    app.run(debug=True, port=5000)