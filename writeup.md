# CRE Data Platform — Design Write-up

---

## i. How I Designed the Hierarchy & Events

The property hierarchy follows the standard CRE model: **Site → Building → Floor → Space**, with each level carrying a persistent ID that survives pipeline refreshes.

**Site** maps directly to a NYC tax parcel (BBL). It stores the parcel polygon sourced from MapPLUTO (ArcGIS Hub) as a WKT string, a lat/lon centroid, an H3 index, and SCD columns to track history. If a parcel is split or merged, new site IDs are created and `parent_site_id` preserves lineage.

**Building** presented a source data challenge — the tax parcel file records one row per BBL regardless of how many buildings sit on it. For parcels where `NUM_BLDGS > 1`, I synthetically explode the record into N rows, divide area metrics equally, and flag each row `data_quality = 'SYNTHETIC_SPLIT'` so analysts always know what is sourced vs inferred.

**Floor** is fully synthetic. Using `BLD_STORY`, I generate one row per floor per building using `sequence()` + `explode()`, typed as `ground`, `upper`, or `top`. Floor IDs are persistent and ready for real sub-building data when it becomes available.

**Space** is out of scope — no source data exists — but the design intent is clear: spaces subdivide floors into suites or units, extending the same ID convention downward.

For events, I modelled two types from actual data:

**TaxEvent** is a periodic snapshot — every parcel gets a row every year. The grain is `PARID + YEAR + PERIOD`, capturing the full tentative → combined → final → current assessment lifecycle in a single row with five assessed value snapshot groups.

**SaleEvent** is a transaction fact derived by detecting year-over-year owner name changes using a LAG window function. When the OWNER field changes between consecutive years on the same PARID, a sale is inferred. Every row is flagged `source_type = 'INFERRED'` and sale price is intentionally absent — it does not exist in the source. ACRIS would be the right dataset to enrich this in future.

**DebtEvent and LeaseEvent** are recognised as important but out of scope — no source data is available for either.

---

## ii. How I Modelled Geospatial Enrichment (Polygon vs H3)

Every site stores its parcel **polygon as WKT** — the precise representation that captures exact parcel boundaries and supports area calculations. This is the authoritative geometry layer, sourced from MapPLUTO.

For fast approximate operations, every site centroid, hotel, and restaurant also gets an **H3 index at resolution 9**. At this resolution each hexagon covers roughly 174 metres. H3 reduces spatial joins to string lookups — no geometry math, just a hash — at the cost of precision. Two points in the same cell could be anywhere from adjacent to 170 metres apart.

The two representations serve different purposes and are used together deliberately:
- **Polygon** — precise boundary, used for storage and exact spatial queries
- **H3** — fast approximate filter, used in the enrichment pipeline and heatmap analytics

Neither replaces the other. Polygon is the source of truth; H3 is the performance layer.

---

## iii. How I Linked Sites to Nearby Hotels & Restaurants

Two enrichment datasets were used:
- **NYC Hotels Properties Citywide** — NYC Open Data ([link](https://data.cityofnewyork.us/City-Government/Hotels-Properties-Citywide/tjus-cn27/about_data))
- **DOHMH NYC Restaurant Inspection Results** — NYC Open Data ([link](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j))

Both are loaded into `dim_hotel` and `dim_restaurant` in Silver, each with an H3 index computed from their lat/lon coordinates.

The linkage to sites is handled by a **hybrid spatial join** stored in `fact_site_amenity`:

1. **H3 k-ring expansion** — each site is expanded to k=4 rings of H3 neighbours (~1km radius, ~61 cells). This replaces a full cross join with a fast set lookup.
2. **H3 join** — amenities whose H3 index falls within a site's neighbourhood become candidates.
3. **Haversine on candidates** — exact distance in metres is computed for every candidate pair using the Haversine formula.
4. **Classification** — each pair is tagged with a `relationship_type`: `within_100m`, `within_300m`, `within_500m`, or `within_1km`.

The result is a pre-computed proximity table. Queries never perform spatial math at runtime — proximity filtering is a simple `WHERE` clause on a stored string column. The pipeline uses Delta `MERGE INTO` making it fully re-runnable without duplication.

---

## iv. Example Analytical Queries Supported by the Model

**Sites within 300m of a hotel**
Filter `fact_site_amenity` where `amenity_type = 'hotel'` and `relationship_type IN ('within_100m', 'within_300m')`. Returns exact distance alongside property and hotel details.

**Rank sites by number of nearby hotels within 1km**
Aggregate `fact_site_amenity` by `site_id` where `amenity_type = 'hotel'`, count amenities per site, order descending.

**Recent ownership transfers near hotel clusters**
Join `fact_sale` to `fact_site_amenity` on `site_id`, filter by `amenity_type = 'hotel'`, `relationship_type = 'within_300m'`, and `sale_year >= 2020`. Surfaces investment activity near hospitality zones.

**Amenity density heatmap by H3 cell**
Group sites and amenities by `h3_index`, count distinct hotels and restaurants per cell. The H3 index is directly consumable by mapping tools — each hexagon becomes a tile coloured by amenity density.

**Walkability score per property**
A weighted composite across distance bands — within 100m scores 4 points, 300m scores 3, 500m scores 2, 1km scores 1. Aggregated per site to produce a single amenity accessibility score per property.