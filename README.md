# CRE Data Platform — Newmark Assignment

A Commercial Real Estate Data Platform built on Azure Databricks with Unity Catalog, combining NYC Tax Parcel data with geospatial enrichment datasets to support property hierarchy, event tracking, and proximity analytics.

---

## Deliverables

| Deliverable | Location |
|---|---|
| ERD / Schema Diagram | [`erd.png`](./erd.png) |
| SQL DDL | [`ddl.sql`](./ddl.sql) |
| Design Write-up | [`writeup.md`](./writeup.md) |
| Walkthrough Video | _Link to be uploaded_ |

---

## Data Sources

| Dataset | Source |
|---|---|
| NYC Tax Parcel Assessment (10.5M rows) | NYC Department of Finance |
| MapPLUTO — Parcel Polygons (856K rows) | [ArcGIS Hub](https://www.arcgis.com/home/item.html?id=1564ace0b4f44318ac39920737f9bd07) |
| NYC Hotels Properties Citywide | [NYC Open Data](https://data.cityofnewyork.us/City-Government/Hotels-Properties-Citywide/tjus-cn27/about_data) |
| DOHMH NYC Restaurant Inspection Results | [NYC Open Data](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j) |

---

## Architecture

```
Landing (raw files)
    ↓
Bronze (raw ingested tables — no transformation)
    ↓
Silver (cleaned, joined, enriched)
    ↓
Gold (star schema — analytics ready)
```

**Infrastructure:** Azure Databricks + ADLS Gen2 + Unity Catalog  
**Storage account:** `crmdataplatform` | **Container:** `cre`  
**Catalog:** `cre_catalog` | **Schemas:** `landing`, `bronze`, `silver`, `gold`

---

## Notebooks

| Notebook | Layer | Description |
|---|---|---|
| [`infra.ipynb`](./infra.ipynb) | Setup | Provisions catalog, schemas, and volumes on ADLS Gen2 |
| [`bronze/stage_native_data.ipynb`](./bronze/stage_native_data.ipynb) | Bronze | Ingests NYC Tax Parcel TSV and MapPLUTO GeoJSON into bronze |
| [`bronze/stage_enrichment_data.ipynb`](./bronze/stage_enrichment_data.ipynb) | Bronze | Ingests hotel properties and restaurant inspection CSVs into bronze |
| [`silver/load_dim_and_fact.ipynb`](./silver/load_dim_and_fact.ipynb) | Silver | Builds property hierarchy dims (`dim_site`, `dim_building`, `dim_floor`, `dim_owner`) and fact tables (`fact_tax_assessment`, `fact_sale_event`) |
| [`silver/enrichment_pipeline.ipynb`](./silver/enrichment_pipeline.ipynb) | Silver | Builds `dim_hotel`, `dim_restaurant`, and `fact_site_amenity` via hybrid H3 + Haversine spatial join |
| [`gold/load_to_gold.ipynb`](./gold/load_to_gold.ipynb) | Gold | Promotes all dims and facts to Gold star schema |
| [`ddl.sql`](./ddl.sql) | Gold | Explicit schema contracts for all Gold tables — run before `load_to_gold` |
| [`geospatial_analytics.ipynb`](./geospatial_analytics.ipynb) | Analytics | 7 geospatial analytical queries against the Gold layer |

---

## Gold Layer Schema

**Dimensions**
- `dim_property` - conformed site-level property dimension with polygon, H3, and aggregated building metrics
- `dim_building` - one row per building, multi-building parcels synthetically split
- `dim_floor` - synthetic floors generated from `num_stories`
- `dim_owner` - unique property owners with sha2 hash ID
- `dim_date` - calendar spine 2000-2035
- `dim_event_type` - static lookup for TAX and SALE event categories
- `dim_hotel` - hotel properties deduplicated to one row per parcel with H3 index
- `dim_restaurant` - restaurant entities with cuisine type and H3 index

**Facts**
- `fact_tax_assessment` - periodic snapshot, grain: PARID + YEAR + PERIOD, all 5 AV snapshots
- `fact_sale` - transaction fact, inferred ownership transfers from owner name changes
- `fact_site_amenity` - pre-computed site-to-amenity proximity via hybrid H3 k-ring + Haversine

---

## Key Design Decisions

- **DDL-first** - Gold schema defined explicitly before any data is written
- **Synthetic building split** - multi-building parcels exploded to one row per building, flagged with `data_quality = SYNTHETIC_SPLIT`
- **Inferred sales** - ownership transfers derived from LAG on OWNER field, flagged `source_type = INFERRED`
- **Hybrid spatial join** - H3 k-ring (k=4) for candidate filtering, Haversine for exact distance in metres
- **Pre-computed proximity** - `relationship_type` stored at pipeline time so queries never perform spatial math at runtime
- **Re-runnable enrichment** - Delta `MERGE INTO` ensures new data increments without duplication