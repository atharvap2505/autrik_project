# Autrik Project - Designing a DJIFlightRecord ETL Pipeline and showing key metrics on Grafana

## Overview

This project provides an end-to-end ETL (Extract, Transform, Load) pipeline for processing DJI flight log data, storing it in ClickHouse, and visualizing it with Grafana.

---

## ETL Pipeline

### 1. Extract

- **Script:** [`extract.sh`](extract.sh)
- **Description:**  
  Extracts raw DJI flight logs from `.txt` files in the `input_logs` directory, processes them using a Dockerized tool, and outputs cleaned `.json` files to `parsed_logs/`.

### 2. Transform

- **Script:** [`transform.py`](transform.py)
- **Description:**  
  Reads the parsed JSON logs, flattens and normalizes the data, and outputs two types of CSVs to `transformed_logs/`:
  - `summary.csv`: Summary of each flight.
  - `*_info.csv`: Detailed time-series info for each flight.

### 3. Load

- **Script:** [`load.py`](load.py)
- **Description:**  
  Loads the transformed CSVs into ClickHouse:
  - Creates a database `autrik` and tables `flight_summary` and `flight_info` with appropriate types (including millisecond-precision timestamps).
  - Inserts only new records, avoiding duplicates.
  - Handles missing or invalid datetime values robustly.

### 4. Orchestration

- **Script:** [`etl.sh`](etl.sh)
- **Description:**  
  Runs the entire ETL pipeline in sequence, halts if one of the running files returns an error:
  ```bash
  sh etl.sh
  ```

---

## Dockerfile

- The [`Dockerfile`](Dockerfile) builds an Ubuntu-based image for the DJI flight record parsing tool. Rest of the folders are its dependencies.

---

## ClickHouse Table Schema

- **flight_summary:** Stores summary data for each flight.
- **flight_info:** Stores detailed time-series data, partitioned by `flight_id` and ordered by `flight_id` and `timestamp` with millisecond precision.

---

## Usage

### 1. Install ClickHouse and Grafana

- **ClickHouse:**  
  Please follow the official installation instructions for your operating system at:  
  [https://clickhouse.com/docs/en/getting-started/install/](https://clickhouse.com/docs/en/getting-started/install/)

- **Grafana:**  
  Please follow the official installation instructions for your operating system at:  
  [https://grafana.com/docs/grafana/latest/setup-grafana/installation/](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)


---

### 2. Configure ClickHouse as a Data Source in Grafana

1. **Start Grafana** (if not already running):
   ```bash
   sudo systemctl start grafana-server
   ```

2. **Open Grafana** in your browser:  
   [http://localhost:3000](http://localhost:3000)  
   (Default login: `admin` / `admin`)

3. **Add ClickHouse as a Data Source:**
   - Go to **Settings** (gear icon) → **Data Sources** → **Add data source**.
   - Search for **ClickHouse** and select it.
   - Set the following:
     - **URL:** `http://localhost:8123`
     - **Default Database:** `autrik`
     - **User/Password:** (as configured in your ClickHouse instance)
   - Click **Save & Test** to verify the connection.

---

### 3. Run the Repository ETL Pipeline

1. Clone the repository.
2. Ensure that you have the `pandas` and `clickhouse-connect` Python modules installed:
   ```bash
   pip install pandas clickhouse-connect
   ```
3. In [`load.py`](load.py), configure your ClickHouse user credentials if needed.
4. Build the Docker image for the DJI flight record parser, refer to official `DJI_SDK` repository (linked at the end) for more information on how to get your own `SDK_KEY`.
   ```bash
   docker build --build-arg SDK_KEY=your_app_key -t pf .
   ```
5. Place the files that need to be processed in the `input_logs` directory.
6. Run the ETL pipeline:
   ```bash
   sh etl.sh
   ```
7. Data will be available in ClickHouse for analysis.

---

### 4. Import the Dashboard

1. Go to **Dashboards** → **Import** in Grafana.
2. Upload or paste the contents of [`dashboard.json`](dashboard.json).
3. When prompted, select the ClickHouse data source you just configured.
4. Click **Import**.
5. For analysis of different flights, you can change the filters in the Grafana SQL query.

You should now be able to view the metrics dashboard.

---

## Grafana Dashboard

Below are screenshots of the Grafana dashboard built on top of the ClickHouse data:

<p float="left">
  <img src="Images/Screenshot from 2025-06-30 15-30-26.png" width="400"/>
  <img src="Images/Screenshot from 2025-06-30 15-31-07.png" width="400"/>
  <img src="Images/Screenshot from 2025-06-30 15-31-29.png" width="400"/>
  <img src="Images/Screenshot from 2025-06-30 15-31-46.png" width="400"/>
  <img src="Images/Screenshot from 2025-06-30 15-32-01.png" width="400"/>
</p>

---

## Dashboard JSON

The full Grafana dashboard configuration is available in [`dashboard.json`](dashboard.json). You can import this JSON file directly into Grafana to recreate the dashboard panels and layout.

---

## References

- DJI Flight Record Parsing Library: [https://github.com/dji-sdk/FlightRecordParsingLib](https://github.com/dji-sdk/FlightRecordParsingLib)  
  _This repository was used as the basis for the Dockerfile used for the extraction process._