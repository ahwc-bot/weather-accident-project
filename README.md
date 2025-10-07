# 🚦 Traffic Incidents & Weather Analytics Project

This project simulates a real-world BI workflow by integrating traffic collision data from Toronto Police Services and weather data from Open-Meteo. It demonstrates my ability to build a reproducible, end-to-end data pipeline — covering ingestion, transformation, validation, and visualization — using modern data engineering tools.

🔍 **Key Skills Demonstrated**:
- API integration and data ingestion with Python
- dbt modeling with staging, intermediate, and mart layers
- Automated data validation and pipeline monitoring with Pytest and logging
- Project structure aligned with gold-layer principles in lakehouse architecture

👤 **Role**: Sole developer and architect — responsible for all ingestion, modeling, testing, and dashboard design.

🧱 **Next Steps**:
- Finalize mart layer and dimensional models
- Build interactive dashboards in Tableau Public and Power BI (in development)
- Explore paginated reports and Direct Lake mode in Microsoft Fabric

---

## **Project Structure**

### **1. Raw & Intermediate Data**
- **`raw_incidents`** and **`weather_cache`**: Source tables storing raw incident and weather data.  
- **`stg_tps_incidents`**: Staging model for cleaned TPS incident data.  
- **`int_enriched_incidents`**: Intermediate model with derived fields and validation.  
- **`mart_incidents_flat`**: Flat mart table prepared for visualization (current focus).

### **2. Scripts**
- **`scripts/export_for_tableau.py`**: Prepares data from the mart layer for Tableau Public.  
- **`scripts/fetch_tps_incidents.py`**, **`scripts/build_weather_cache.py`**: Scripts for fetching and caching source data from external APIs.

### **3. Data Sources**
- Toronto Police Service (TPS) incidents API  
- Open-Meteo (OM) weather API  

### **4. Visualizations**
- **Tableau Public Dashboard (in development):**  
  An interactive dashboard showing traffic and weather incidents with KPIs and filters.  
  - Will highlight trends, severity, and conditions.  
  - Demonstrates end-to-end pipeline from raw data to visualization.

### **5. Testing & Utilities**
- Unit tests written with `pytest`.  
- Logging utilities for monitoring pipeline execution.

---

## **Phased Approach**
This project is intentionally open to the public in its current phase to showcase a working pipeline and visualization for demonstration purposes. Additional enhancements will be added iteratively.  

**Phase 1 (Current):**  
- Raw → Staging → Intermediate → Flat Mart → Tableau Public dashboard  

**Phase 2 (Upcoming):**  
- Dimensional models (star/snowflake schemas)  
- Airflow DAG for pipeline orchestration  
- Semantic models and dashboards in Power BI Desktop  

---

## **Tech Stack**
- **PostgreSQL** as the data warehouse  
- SQL / dbt for data modeling  
- Python for data processing and ETL scripts  
- Tableau Public for interactive dashboards  
- (Future) Airflow for orchestration  
- (Future) Power BI for semantic modeling  

---

## **Usage**
1. Clone the repository:  

   ```bash
   git clone https://github.com/yourusername/traffic-weather-incidents.git
   ```
2. Follow the SQL scripts in `models/` to build the mart layer.  
3. Run `scripts/export_for_tableau.py` to prepare data for Tableau.  

> **Note:** This repository is intended for demo purposes. Running the full pipeline requires setting up PostgreSQL, dbt, Python dependencies, and API access.  
>  
> For recruiters or hiring managers: feel free to explore the models, scripts, and structure to understand the pipeline design and engineering approach.


---

## **License**
This project is open-source; see `LICENSE` for details.
