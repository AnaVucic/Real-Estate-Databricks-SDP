CREATE OR REPLACE VIEW real_estate.gold.fact_postings_novi_beograd AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Novi Beograd'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_vracar AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Vračar'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_zvezdara AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Zvezdara'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_palilula AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Palilula'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_rakovica AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Rakovica'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_grocka AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Grocka'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_cukarica AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Čukarica'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_stari_grad AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Stari grad'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_zemun AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Zemun'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_surcin AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Surčin'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_savski_venac AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Savski venac'
);

CREATE OR REPLACE VIEW real_estate.gold.fact_postings_vozdovac AS (
  SELECT * FROM
  real_estate.gold.fact_postings
  WHERE location = 'Opština Voždovac'
);

