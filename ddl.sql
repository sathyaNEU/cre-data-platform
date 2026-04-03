-- ══════════════════════════════════════════════════════════
-- GOLD LAYER DDL
-- ══════════════════════════════════════════════════════════

-- ── dim_property ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_property (
    property_id             STRING          NOT NULL,
    parid                   STRING,
    boro                    STRING,
    block                   STRING,
    lot                     STRING,
    zip_code                STRING,
    zoning                  STRING,
    street_name             STRING,
    house_num_lo            STRING,
    house_num_hi            STRING,
    latitude                DOUBLE,
    longitude               DOUBLE,
    h3_index                STRING,
    polygon_wkt             STRING,
    lot_area                DOUBLE,
    lot_front               DOUBLE,
    lot_depth               DOUBLE,
    land_area               DOUBLE,
    corner                  STRING,
    lot_irreg               STRING,
    parent_site_id          STRING,
    num_buildings           BIGINT,
    total_gross_sqft        DOUBLE,
    total_units             BIGINT,
    newest_year_built       INT,
    total_office_sqft       DOUBLE,
    total_retail_sqft       DOUBLE,
    total_residential_sqft  DOUBLE,
    total_garage_sqft       DOUBLE,
    effective_from          DATE,
    effective_to            DATE,
    is_current              BOOLEAN
)
USING DELTA
COMMENT 'Conformed property dimension. One row per site (current snapshot). Joins dim_site + aggregated building metrics.';

-- ── dim_owner ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_owner (
    owner_id                STRING          NOT NULL,   -- sha2(owner_name, 256)
    owner_name              STRING,
    first_seen_year         INT,
    last_seen_year          INT,
    effective_from          DATE,
    effective_to            DATE,
    is_current              BOOLEAN
)
USING DELTA
COMMENT 'Unique property owners derived from tax records. ID is sha2 hash of cleaned owner name.';

-- ── dim_date ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_date (
    date_id                 INT             NOT NULL,   -- yyyyMMdd integer e.g. 20240101
    full_date               DATE,
    year                    INT,
    quarter                 INT,
    month                   INT,
    month_name              STRING,
    day_of_month            INT,
    day_of_week             INT,                        -- 1=Sun, 7=Sat
    day_name                STRING,
    is_weekend              BOOLEAN,
    week_of_year            INT,
    fiscal_year             INT,                        -- Jul-Jun fiscal year
    yyyymm                  INT
)
USING DELTA
COMMENT 'Calendar dimension. Spine from 2000-01-01 to 2035-12-31.';

-- ── dim_event_type ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_event_type (
    event_type_id           INT             NOT NULL,
    event_category          STRING,                     -- TAX | SALE | DEBT | LEASE
    event_type_name         STRING,
    description             STRING
)
USING DELTA
COMMENT 'Static lookup for event categories and types across all fact tables.';

-- ── dim_building ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_building (  
    building_id             STRING          NOT NULL,   -- site_id + "_" + seq
    site_id                 STRING,                     -- FK → dim_property
    bldg_class              STRING,
    bld_front               DOUBLE,
    bld_depth               DOUBLE,
    bld_ext                 STRING,
    num_stories             INT,
    num_bldgs               INT,
    year_built              INT,
    year_built_flag         STRING,
    year_alt1               INT,
    year_alt2               INT,
    gross_sqft              DOUBLE,
    residential_sqft        DOUBLE,
    office_sqft             DOUBLE,
    retail_sqft             DOUBLE,
    loft_sqft               DOUBLE,
    factory_sqft            DOUBLE,
    warehouse_sqft          DOUBLE,
    storage_sqft            DOUBLE,
    garage_sqft             DOUBLE,
    other_sqft              DOUBLE,
    num_units               INT,
    coop_apts               INT,
    condo_number            STRING,
    coop_num                STRING,
    apt_no                  STRING,
    rec_type                INT,
    building_in_progress    STRING,
    data_quality            STRING,                     -- SOURCE | SYNTHETIC_SPLIT
    effective_from          DATE,
    effective_to            DATE,
    is_current              BOOLEAN
)
USING DELTA
COMMENT 'Building dimension. One row per building. Multi-building parcels are synthetically split and flagged with data_quality = SYNTHETIC_SPLIT.';

-- ── dim_floor ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_floor (
    floor_id                STRING          NOT NULL,   -- building_id + "_F" + floor_number
    building_id             STRING,                     -- FK → dim_building
    site_id                 STRING,                     -- FK → dim_property
    floor_number            INT,
    floor_type              STRING                      -- ground | upper | top
)
USING DELTA
COMMENT 'Floor dimension. Synthetic — one row per floor generated from num_stories in dim_building.';


-- ══════════════════════════════════════════════════════════
-- ENRICHMENT DIMENSIONS
-- ══════════════════════════════════════════════════════════

-- ── dim_hotel ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_hotel (
    hotel_id                STRING          NOT NULL,   -- sha2(PARID)
    parid                   STRING,                     -- links to dim_property
    owner_name              STRING,
    bldg_class              STRING,                     -- H2, H3, HB etc.
    street_number           STRING,
    street_name             STRING,
    zip_code                STRING,
    borough                 STRING,
    latitude                DOUBLE,
    longitude               DOUBLE,
    h3_index                STRING,
    nta_name                STRING,
    load_date               DATE
)
USING DELTA
COMMENT 'Hotel properties. One row per hotel parcel — deduplicated to latest year.';

-- ── dim_restaurant ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.dim_restaurant (
    restaurant_id           STRING          NOT NULL,   -- sha2(CAMIS)
    camis                   STRING,                     -- NYC DOH unique restaurant ID
    name                    STRING,
    borough                 STRING,
    street_number           STRING,
    street_name             STRING,
    zip_code                STRING,
    cuisine_type            STRING,
    latitude                DOUBLE,
    longitude               DOUBLE,
    h3_index                STRING,
    nta_code                STRING,
    load_date               DATE
)
USING DELTA
COMMENT 'Restaurants derived from NYC DOH inspection records. One row per restaurant (latest inspection).';

-- ══════════════════════════════════════════════════════════
-- FACT TABLES
-- ══════════════════════════════════════════════════════════

-- ── fact_tax_assessment ───────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.fact_tax_assessment (
    assessment_id           STRING          NOT NULL,   -- sha2(PARID_YEAR_PERIOD)
    site_id                 STRING,                     -- FK → dim_property
    owner_id                STRING,                     -- FK → dim_owner
    date_id                 INT,                        -- FK → dim_date
    event_type_id           INT,                        -- FK → dim_event_type
    tax_year                INT,
    period                  INT,
    tax_class               STRING,
    tax_class_prior_year    STRING,
    tax_class_tentative     STRING,
    tax_class_final         STRING,
    bldg_class              STRING,
    zoning                  STRING,
    roll_section            INT,
    -- Prior year snapshot
    py_mkt_land             DOUBLE,
    py_mkt_total            DOUBLE,
    py_act_land             DOUBLE,
    py_act_total            DOUBLE,
    py_trn_land             DOUBLE,
    py_trn_total            DOUBLE,
    py_txb_total            DOUBLE,
    -- Tentative snapshot
    ten_mkt_land            DOUBLE,
    ten_mkt_total           DOUBLE,
    ten_act_land            DOUBLE,
    ten_act_total           DOUBLE,
    ten_trn_land            DOUBLE,
    ten_trn_total           DOUBLE,
    ten_txb_total           DOUBLE,
    -- Combined snapshot
    cbn_mkt_land            DOUBLE,
    cbn_mkt_total           DOUBLE,
    cbn_act_land            DOUBLE,
    cbn_act_total           DOUBLE,
    cbn_trn_land            DOUBLE,
    cbn_trn_total           DOUBLE,
    cbn_txb_total           DOUBLE,
    -- Final snapshot
    fin_mkt_land            DOUBLE,
    fin_mkt_total           DOUBLE,
    fin_act_land            DOUBLE,
    fin_act_total           DOUBLE,
    fin_trn_land            DOUBLE,
    fin_trn_total           DOUBLE,
    fin_txb_total           DOUBLE,
    -- Current snapshot
    cur_mkt_land            DOUBLE,
    cur_mkt_total           DOUBLE,
    cur_act_land            DOUBLE,
    cur_act_total           DOUBLE,
    cur_trn_land            DOUBLE,
    cur_trn_total           DOUBLE,
    cur_txb_total           DOUBLE,
    -- Building metrics at time of filing
    gross_sqft              DOUBLE,
    residential_sqft        DOUBLE,
    office_sqft             DOUBLE,
    retail_sqft             DOUBLE,
    hotel_sqft              DOUBLE,
    loft_sqft               DOUBLE,
    factory_sqft            DOUBLE,
    warehouse_sqft          DOUBLE,
    storage_sqft            DOUBLE,
    garage_sqft             DOUBLE,
    other_sqft              DOUBLE,
    num_stories             INT,
    num_bldgs               INT,
    num_units               INT,
    year_built              INT,
    -- Flags
    building_in_progress    STRING,
    new_drop_flag           INT,
    py_tax_flag             STRING,
    ten_tax_flag            STRING,
    fin_tax_flag            STRING,
    cur_tax_flag            STRING,
    load_date               DATE
)
USING DELTA
COMMENT 'Tax assessment fact. Grain: PARID + YEAR + PERIOD. All 5 AV snapshots per filing.';

-- ── fact_sale ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.fact_sale (
    sale_event_id           STRING          NOT NULL,   -- sha2(PARID_YEAR)
    property_id             STRING,                     -- FK → dim_property
    seller_owner_id         STRING,                     -- FK → dim_owner
    buyer_owner_id          STRING,                     -- FK → dim_owner
    date_id                 INT,                        -- FK → dim_date
    event_type_id           INT,                        -- FK → dim_event_type
    sale_year               INT,
    prior_ownership_year    INT,
    seller_name             STRING,
    buyer_name              STRING,
    tax_class               STRING,
    bldg_class              STRING,
    zoning                  STRING,
    source_type             STRING,                     -- INFERRED
    load_date               DATE
)
USING DELTA
COMMENT 'Sale event fact. Grain: one row per inferred ownership transfer. Source type INFERRED — derived from owner name changes in tax records. sale_price, deed_type, document_id are NULL across entire dataset — these fields do not exist in NYC tax parcel source. Populate from ACRIS when available.';

-- ══════════════════════════════════════════════════════════
-- ENRICHMENT FACT
-- ══════════════════════════════════════════════════════════

-- ── fact_site_amenity ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS cre_catalog.gold.fact_site_amenity (
    relationship_id         STRING          NOT NULL,   -- sha2(site_id + amenity_id)
    site_id                 STRING,                     -- FK → dim_property
    amenity_id              STRING,                     -- FK → dim_hotel or dim_restaurant
    amenity_type            STRING,                     -- hotel | restaurant
    distance_meters         DOUBLE,                     -- exact Haversine distance
    relationship_type       STRING,                     -- within_100m | within_300m | within_500m | within_1km
    h3_same_cell            BOOLEAN,                    -- true if site and amenity share same H3 cell
    compute_date            DATE,                       -- when distance was calculated
    load_date               DATE
)
USING DELTA
COMMENT 'Pre-computed site-to-amenity proximity. Hybrid H3 k_ring filter + Haversine exact distance. Enables fast proximity queries without spatial math at query time.';--