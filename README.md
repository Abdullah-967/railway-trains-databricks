# 🚄 NS Trains: Databricks Performance Pipeline

A comprehensive data engineering project analyzing passenger train performance across the Dutch railway network. This pipeline processes historical stop and service data to generate actionable insights into delays, cancellations, and platform changes.

## 🏗️ Project Structure

- **[`data/`](file:///c:/Users/PC/Downloads/code_DE/ns_trains_databricks/data/)**: Documentation on data sources and a detailed data dictionary.
- **[`pipeline_code/`](file:///c:/Users/PC/Downloads/code_DE/ns_trains_databricks/pipeline_code/)**: The core Databricks logic, organized into a Medallion architecture.
- **[`visuals/`](file:///c:/Users/PC/Downloads/code_DE/ns_trains_databricks/visuals/)**: Screenshots and a demo video of the final performance dashboard.

## ⚙️ Data Architecture (Medallion)

We use a multi-layered approach to transform raw data into insights:

1.  **Bronze (Raw)**: Raw CSV ingestion with schema evolution and basic sanitization.
2.  **Silver (Cleaned)**: Data typing, cleaning, and enrichment. Includes derived on-time flags (threshold <= 5 min) and performance classification.
3.  **Gold (Business)**: Optimized dimensional models (`fact_stops`, `dim_station`) and daily performance aggregations for reporting.

## 📊 Insights & Dashboards
The pipeline feeds a dashboard that tracks KPIs like:
- **Arrival/Departure On-Time %**
- **Cancellation Rates**
- **Platform Change Severity**
- **Peak Hour Performance** (Morning vs. Evening Rush)

![Project Demo Video](file:///c:/Users/PC/Downloads/code_DE/ns_trains_databricks/visuals/dashboard_vid.mp4)

Check out the [visuals folder](file:///c:/Users/PC/Downloads/code_DE/ns_trains_databricks/visuals/) for more breakdowns.

## 🔗 Data Source
Data is curated from the NS API by [Rijden de Treinen](https://www.rijdentreinen.nl/open-data). You can find more details in [how_to_get_data.md](file:///c:/Users/PC/Downloads/code_DE/ns_trains_databricks/data/how_to_get_data.md).
