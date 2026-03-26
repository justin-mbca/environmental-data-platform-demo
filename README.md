# Azure-Style Environmental Data Platform

This project simulates an enterprise environmental/toxicology data platform using Azure Data Lake, Databricks, and medallion architecture concepts.

## Architecture Overview

- **Bronze Layer:** Raw data ingestion (CSV, JSON, API)
- **Silver Layer:** Cleaned and validated data
- **Gold Layer:** Curated, analytics-ready datasets

Data flows through these layers, with validation, logging, and metadata tracking at each stage. The structure simulates Azure Data Lake folders and Databricks jobs using local Python scripts.

## Pipeline Stages

1. **Ingestion:** Load data from files and mock API
2. **Validation:** Schema, range, and quality checks
3. **Transformation:** Standardize, clean, and integrate data
4. **Curation:** Produce analytics-ready datasets with metadata

## Cloud Simulation
- Azure Data Lake: Local folders (`data/bronze`, `data/silver`, `data/gold`)
- Databricks: Python pipeline scripts
- Key Vault: `config/key_vault.json`
- RBAC: `config/rbac.json`


## Dashboard

Run the Streamlit dashboard to visualize environmental trends and data quality:

```bash
pip install -r requirements.txt
streamlit run scripts/dashboard.py
```


## Data Quality Log & Data Provenance

The pipeline automatically logs data validation, quality checks, and provenance information to `logs/data_quality.log` at each stage. This log helps track:

- Schema validation results
- Range and value checks
- Duplicate detection
- Errors and warnings during ETL
- Data source and transformation lineage

**Data provenance** is maintained by recording the origin, validation status, and transformation history of each dataset. This ensures traceability and auditability for all data processed through the platform.

- See `logs/data_quality.log` for quality and provenance logs
- See `metadata.json` for dataset lineage and versioning

---

## Getting Started
- Run `pipeline.py` to execute the full ETL pipeline
- See `logs/` for data quality and error logs
- See `metadata.json` for lineage and versioning



## Cloud Architecture Diagram (Azure + Databricks)

```mermaid
flowchart TD
  %% Ingestion Layer
  subgraph Ingestion
    A1[CSV / JSON / API / Streaming] --> ADLS1[ADLS Bronze Layer]
    EH[Event Hubs / IoT Hub] --> ADLS1
  end

  %% Processing Layer
  subgraph Processing
    ADLS1 --> DBX1[Databricks - Validate & Clean]
    DBX1 --> ADLS2[ADLS Silver Layer]
    ADLS2 --> DBX2[Databricks - Transform & Integrate]
    DBX2 --> ADLS3[ADLS Gold Layer]
  end

  %% Orchestration & Security
  subgraph Orchestration_and_Security
    DF[Azure Data Factory] -.-> DBX1
    DF -.-> DBX2
    KV[Key Vault] -.-> DBX1
    KV -.-> DF
    AAD[Azure AD] -.-> DBX1
    AAD -.-> DF
  end

  %% Analytics & Governance
  subgraph Analytics_and_Governance
    ADLS3 --> PBI[Power BI / Streamlit]
    ADLS3 --> SYN[Azure Synapse]
    ADLS3 --> PUR[Azure Purview]
    DBX2 --> MON[Azure Monitor / Log Analytics]
  end
```


## ETL Pipeline Diagram (Mermaid)

```mermaid
flowchart LR
	A[CSV, JSON, EPA API] --> B(Bronze: Raw Data)
	B --> C(Validation & Cleaning)
	C --> D(Silver: Cleaned Data)
	D --> E(Transformation & Integration)
	E --> F(Gold: Curated Data)
	F --> G[Dashboard/Analytics]
	B -->|Logs| H[Logs]
	C -->|Data Quality| H
	F -->|Metadata/Lineage| I[metadata.json]
```

![Medallion Architecture](docs/architecture.png)

---

For more details, see inline documentation and comments in each script.
