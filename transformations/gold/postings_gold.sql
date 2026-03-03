CREATE OR REPLACE VIEW real_estate.gold.fact_postings AS (
  SELECT
    p.id,
    p.title,
    to_date(p.posting_datetime) as `posting_date`,
    p.type,
    p.city,
    p.location,
    p.microlocation,
    p.street,
    p.price_amount,
    p.currency,
    p.area_amount,
    p.measurement_unit,
    p.rooms,
    p.poster,
    p.heating,
    p.furnished,
    p.floor,
    p.floor_total,
    p.payment_type,
    p.description,
    c.year,
    c.month,
    c.day_of_month,
    c.day_of_week,
    c.month_year,
    c.is_weekday,
    c.is_weekend,
    ad.`Depozit` as `deposit`,
    ad.`Salonski` as `salon_style`,
    ad.`Za studente` as `for_students`,
    ad.`Za pušače` as `smoker_friendly`,
    ad.`Dozvoljeni kućni ljubimci` as `pet_friendly`,
    ad.`Penthouse` as `penthouse`,
    ad.`Duplex` as `duplex`,
    ad.`Potkrovlje` as `loft`,
    ad.`Nije poslednji sprat` as `not_last_floor`,
    ad.`Smeštaj za radnike` as `for_workers`,
    ad.`Nije stan u kući` as `not_house`,
    ad.`Odmah useljiv` as `immediately_usable`,
    ot.`Lođa` as `loggia`,
    ot.`Lift` as `elevator`,
    ot.`Video nadzor` as `video_surveillance`,
    ot.`Francuski balkon` as `french_balcony`,
    ot.`Terasa` as `terrace`,
    ot.`Interfon` as `intercom`,
    ot.`Telefon` as `phone`,
    ot.`Podrum` as `basement`,
    ot.`Klima` as `air_conditioning`,
    ot.`Garaža` as `garage`,
    ot.`Topla voda` as `hot_water`,
    ot.`Internet` as `internet`,
    ot.`Sa baštom` as `with_garden`,
    ot.`KATV` as `cable_tv`,
    ot.`Kamin` as `fireplace`,
    ot.`Parking` as `parking`
  FROM
    real_estate.silver.postings p
  LEFT JOIN (
    SELECT * FROM (
  SELECT 
    b.posting_id, 
    b.additional_tag_name, 
    1 AS has_tag
  FROM real_estate.silver.postings_additional_tags b
)
PIVOT (
  MAX(has_tag)
  FOR additional_tag_name IN (
    'Depozit',
    'Salonski',
    'Za studente',
    'Za pušače',
    'Dozvoljeni kućni ljubimci',
    'Penthouse',
    'Duplex',
    'Za nepušače',
    'Potkrovlje',
    'Nije poslednji sprat',
    'Smeštaj za radnike',
    'Nije stan u kući',
    'Odmah useljiv'
  ))
  ) AS ad ON p.id = ad.posting_id
  LEFT JOIN (
    SELECT * FROM (
  SELECT 
    b.posting_id, 
    b.other_tag_name, 
    1 AS has_tag
  FROM real_estate.silver.postings_other_tags b
)
PIVOT (
  MAX(has_tag)
  FOR other_tag_name IN (
    'Lođa',
    'Lift',
    'Video nadzor',
    'Francuski balkon',
    'Terasa',
    'Interfon',
    'Telefon',
    'Podrum',
    'Klima',
    'Garaža',
    'Topla voda',
    'Internet',
    'Sa baštom',
    'KATV',
    'Kamin',
    'Parking'
  )) ) AS ot ON p.id = ot.posting_id
  JOIN real_estate.silver.calendar AS c ON to_date(p.posting_datetime) = c.date
);