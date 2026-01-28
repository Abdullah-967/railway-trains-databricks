# How to Get Data

The railway stations and passenger train services data in this project are sourced from **Rijden de Treinen** ([rijdentreinen.nl](https://www.rijdentreinen.nl/open-data)).

### Data Origin
- The station names, coordinates, and codes are directly extracted from the **NS API**.
- The passenger train services (since 2019) are based on real-time data from NS, including live departure/arrival times and service updates.

### Curation
The data is curated and provided by Rijden de Treinen as compressed CSV files (Gzip). This project uses these extracts for building the data pipeline in Databricks.
