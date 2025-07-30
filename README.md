# InfluxDB-data-evolution
In this repo, you find extraction of (current value and producing_water"bool" value) from influxDB and display them on a local webserver graph

#  Clone the repo 
    git clone https://github.com/H11iye/InfluxDB-data-evolution.git
#  Move to influxdb-data-evolution directory

    cd InfluxDB-data-evolution

# Create a virtual environement 

    python -m venv venv
  
# Activate the virtual environement
    cd venv/Scripts/activate

# Install the required packages 
    pip install influxdb-client pandas python-dotenv

# Extract data from influxdb by running this command 

    python .\extract_Influx_data.py

# Start a Local Web Server

    python -m http.server 8000

# Open in Browser

    http://localhost:8000/influxdb_DATA_graph.html
  
