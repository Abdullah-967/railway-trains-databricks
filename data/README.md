# NS Trains Dataset Documentation

This directory contains the dataset used for the NS Trains Databricks pipeline. The data is sourced from [Rijden de Treinen](https://www.rijdentreinen.nl/), which curates and extracts station and service information from the NS API.

## Dataset Overview

The dataset consists of two main components:
1. **Railway Stations**: Information about railway stations in the Netherlands and surrounding countries.
2. **Passenger Train Services**: Historical data on train services, stops, delays, and cancellations since 2019.

---

## Railway Stations Dataset

This dataset contains metadata about railway stations.

### Columns

| Column | Description |
| :--- | :--- |
| **rdt_id** | Unique identifier used by Rijden de Treinen. |
| **code** | Unique station code (abbreviation). Up to 8 characters. |
| **uic** | International unique identifier (UIC code). Dutch stations start with 84. |
| **name_short** | Short version of the station name. |
| **name_medium** | Medium length version of the station name. |
| **name_long** | Long (full) version of the station name. |
| **slug** | Unique identifier in lowercase without spaces (URL slug). |
| **country** | Country code (NL, D, B, F, A, CH, GB). |
| **type** | Category of the station (e.g., megastation, intercitystation, stoptreinstation). |
| **geo_lat** | Geographical latitude in decimal degrees. |
| **geo_lng** | Geographical longitude in decimal degrees. |

---

## Passenger Train Services Dataset

This dataset contains a record of all passenger train services in the Netherlands since 2019. Each row represents a stop at a station.

### Columns

| Column | Description |
| :--- | :--- |
| **Service:RDT-ID** | Unique identifier for the service. Unique per service per date. |
| **Service:Date** | Scheduled service date (not necessarily actual date). |
| **Service:Type** | Service type (e.g., Intercity, Sprinter, ICE International). |
| **Service:Company** | Operator (e.g., NS, Arriva). |
| **Service:Train number** | Unique train/service number for the date. |
| **Service:Completely cancelled** | True if all stops for this service were cancelled. |
| **Service:Partly cancelled** | True if one or more stops were cancelled. |
| **Service:Maximum delay** | Highest delay (in minutes) observed for any stop in the service. |
| **Stop:RDT-ID** | Unique identifier for each individual stop. |
| **Stop:Station code** | Abbreviation of the station name. |
| **Stop:Station name** | Full name of the station. |
| **Stop:Arrival time** | Scheduled arrival time (RFC 3339). Empty for origin stations. |
| **Stop:Arrival delay** | Arrival delay in minutes. |
| **Stop:Arrival cancelled** | True if the arrival at this stop was cancelled. |
| **Stop:Departure time** | Scheduled departure time (RFC 3339). Empty for terminal stations. |
| **Stop:Departure delay** | Departure delay in minutes. |
| **Stop:Departure cancelled** | True if the departure at this stop was cancelled. |
| **Stop:Platform change** | True if the platform used differs from the planned platform. |
| **Stop:Planned platform** | Originally scheduled platform. |
| **Stop:Actual platform** | Platform actually used for the service. |
