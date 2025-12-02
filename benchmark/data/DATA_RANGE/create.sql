copy (select * from range(5000000) t(i)) to 'data/DATA_RANGE/5m.parquet';
;
copy (select * from range(100000000) t(i)) to 'data/DATA_RANGE/100m.parquet';
