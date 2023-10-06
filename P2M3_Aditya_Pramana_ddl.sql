-- Membuat database
CREATE DATABASE db_phase2;

--Membuat tabel
CREATE TABLE public.table_M3
(
    id integer,
    diagnosis character varying(1),
    radius_mean numeric,
    texture_mean numeric,
    perimeter_mean numeric,
    area_mean numeric,
    smoothness_mean numeric,
    compactness_mean numeric,
    concavity_mean numeric,
    "concave points_mean" numeric,
    symmetry_mean numeric,
    fractal_dimension_mean numeric,
    radius_se numeric,
    texture_se numeric,
    perimeter_se numeric,
    area_se numeric,
    smoothness_se numeric,
    compactness_se numeric,
    concavity_se numeric,
    "concave points_se" numeric,
    symmetry_se numeric,
    fractal_dimension_se numeric,
    radius_worst numeric,
    texture_worst numeric,
    perimeter_worst numeric,
    area_worst numeric,
    smoothness_worst numeric,
    compactness_worst numeric,
    concavity_worst numeric,
    "concave points_worst" numeric,
    symmetry_worst numeric,
    fractal_dimension_worst numeric,
    PRIMARY KEY (id)
);

COPY public.table_M3(
    id,
    diagnosis,
    radius_mean,
    texture_mean,
    perimeter_mean,
    area_mean,
    smoothness_mean,
    compactness_mean,
    concavity_mean,
    "concave points_mean",
    symmetry_mean,
    fractal_dimension_mean,
    radius_se,
    texture_se,
    perimeter_se,
    area_se,
    smoothness_se,
    compactness_se,
    concavity_se,
    "concave points_se",
    symmetry_se,
    fractal_dimension_se,
    radius_worst,
    texture_worst,
    perimeter_worst,
    area_worst,
    smoothness_worst,
    compactness_worst,
    concavity_worst,
    "concave points_worst",
    symmetry_worst,
    fractal_dimension_worst
)-- copy main table to input from file csv pandas


--Memasukan isi data kedalam tabel 
FROM 'C:\HCK-07\Phase 2\p2-ftds007-hck-m3-aditpramna\P2M2_Aditya-Pramana_data_raw.csv'
DELIMITER ',' 	-- delimiter in csv
CSV HEADER;

SELECT * FROM public.table_M3;