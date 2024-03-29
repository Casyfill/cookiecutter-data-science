WITH bbl_buildings AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY bbl ORDER BY created_at DESC) AS rn
    FROM STREETEASY.ANALYTICS.STREETEASY_MYSQL_BUILDINGS
), 
tc AS (
  SELECT t.id as trxn_id
       , t.closing_price_in_cents / 100 as closing_price
       , t.closing_date
       , ML.PROPERTY_MATCHING.clean_unit_string(t.addr_unit) as cleaned_unit
       , t.addr_street
       , t.addr_zip
       , t.addr_city
       , t.addr_state
       , COALESCE(g.response:address_point:lat, g.response:centroid_lot:lat)::float as addr_lat
       , COALESCE(g.response:address_point:lon, g.response:centroid_lot:lon)::float as addr_lon
       , g.response:bbl::varchar as bbl
       , g.response:building_bbl::varchar as building_bbl
       , g.response:rdid::varchar as rdid
       , g.response:pad:bin as bin
       , g.response:pad:block as block
       , CASE
            WHEN g.response:pad:coopnum::int IS NOT NULL THEN 'Coop'
            WHEN g.response:pad:condoflag::boolean THEN 'Condo'
            ELSE 'Other'
        END as lottype_cat
        , ST_POINT(addr_lon, addr_lat) as point
        , b.id as building_id
        , b.community_id as complex
        , t.sale_id as correct_sale_id   -- correct match, Y;
  FROM STREETEASY.ANALYTICS.STREETEASY_MYSQL_EXPERTS_TRANSACTIONS t
  LEFT JOIN ML.PROPERTY_MATCHING.GEOCODED_TRANSACTIONS_GEOREF g ON t.id = g.trxn_id
  LEFT JOIN bbl_buildings b ON b.bbl = g.response:building_bbl::integer AND rn=1
  WHERE 1
  AND status = 1
  AND addr_lon IS NOT NULL
  AND addr_lat IS NOT NULL
  AND addr_lon BETWEEN -74.257839 AND -73.719633 --https://boundingbox.klokantech.com/
  AND addr_lat BETWEEN 40.486608 AND 40.923248
), cs AS (
  SELECT s.id as sale_id,
       s.price,
       s.status,
       s.closed_at,
       s.pending_at,
       s.area_id,
       s.addr_street,
       s.addr_lon,
       s.addr_lat,
       s.addr_zip,
       s.addr_city,
       s.building_id,
       COALESCE(s.closed_at, s.pending_at)::date as active_date,
       FEATURESTORE.SALES.UNITTYPE_CAT(s.unittype) as unittype_cat,
       ML.PROPERTY_MATCHING.clean_unit_string(s.normalized_addr_unit) as cleaned_unit,
       b.community_id as complex,
       b.bbl as building_bbl,
       b.census_block as block,
       b.bin as bin,
       g.x,
       g.y,
       g.distance,
       g.azimuth,
       ST_POINT(s.addr_lon, s.addr_lat) as point,
       p.source as bbl -- property bbl 
  FROM "STREETEASY"."ANALYTICS"."STREETEASY_MYSQL_SALES" s
  LEFT JOIN "STREETEASY"."ANALYTICS"."STREETEASY_MYSQL_BUILDINGS" b on s.building_id = b.id
  LEFT JOIN "FEATURESTORE"."BUILDINGS"."AREAS" a on s.building_id = a.building_id
  LEFT JOIN "FEATURESTORE"."BUILDINGS"."GEO_FEATURES" g ON s.building_id = g.id
  LEFT JOIN  "STREETEASY"."ANALYTICS"."STREETEASY_MYSQL_PROPERTIES" p on s.property_id = p.id
  WHERE 1
  AND s.enabled = 1
  AND s.status IN (2, -1, -2, -3, -4, -5)  -- basycally, non-active; NOTE: for inference, will have to include active as well!
  AND a.borough IN (100,200,300,400,500)
 AND s.addr_lon IS NOT NULL
 AND s.addr_lat IS NOT NULL
 AND s.addr_lon BETWEEN -74.257839 AND -73.719633 --https://boundingbox.klokantech.com/
 AND s.addr_lat BETWEEN 40.486608 AND 40.923248
), c AS (
    SELECT
        cs.sale_id,
        tc.trxn_id,
        tc.closing_price as trxn_price,
        cs.price as listing_price,
        cs.status,
        tc.correct_sale_id,
        tc.lottype_cat,
        cs.unittype_cat,
        ST_DISTANCE(cs.point, tc.point)::int as spatial,
        DATEDIFF(day, tc.closing_date::date,  cs.active_date) as days,  -- last minus first
        tc.closing_price - cs.price AS price_diff,
        ((tc.closing_price / cs.price ) - 1) as price_pct_diff,
        EDITDISTANCE(cs.addr_street, tc.addr_street, 15) as partial_street_score,
        EDITDISTANCE(cs.cleaned_unit, tc.cleaned_unit, 15) as partial_unit_score,
        (spatial * 1 + ABS(days)*1 + ABS(price_pct_diff) * 100 + partial_unit_score * 10 )::int as total,
        (cs.addr_zip = tc.addr_zip) as same_zipcode,
        cs.cleaned_unit AS listing_unit,
        tc.cleaned_unit AS closing_unit,
        cs.addr_street as listing_street,
        tc.addr_street as closing_street,
        tc.addr_zip as trxn_zip,
        tc.addr_city as trxn_city,
        tc.addr_state as trxn_state,
        cs.addr_zip as listing_zip,
        cs.addr_city listing_city,
        cs.area_id as area_id,
        cs.x,
        cs.y,
        cs.distance,
        cs.azimuth,
        ABS(cs.building_id - tc.building_id) < 10 as building_in_10,
        IFNULL( cs.complex = tc.complex, FALSE) as same_complex,
        IFNULL( cs.building_id = tc.building_id, FALSE) as same_building_id,
        IFNULL( cs.block = tc.block, FALSE) AS same_block,
        IFNULL(cs.bin = tc.bin, FALSE) as same_bin,
        IFNULL(cs.building_bbl = tc.building_bbl, FALSE) as same_building_bbl,
        IFNULL(cs.bbl = tc.bbl, FALSE) as same_bbl,
        IFNULL(tc.lottype_cat ilike cs.unittype_cat, FALSE) as same_unittype,
        IFNULL(cs.cleaned_unit ilike tc.cleaned_unit, FALSE) as same_unit,
    FROM tc left join cs ON tc.correct_sale_id = cs.sale_id 
), scount AS (
   select sale_id,
          COUNT(*) as candidates
   FROM c
   GROUP BY sale_id
), ccount AS (
   select trxn_id,
          COUNT(*) as candidates
   FROM c
   GROUP BY trxn_id
)
SELECT
    c.*
    , scount.candidates::INT as candidates
    , ccount.candidates::INT as trxn_candidates
FROM c
LEFT JOIN scount ON c.sale_id = scount.sale_id
LEFT JOIN ccount ON c.trxn_id = ccount.trxn_id