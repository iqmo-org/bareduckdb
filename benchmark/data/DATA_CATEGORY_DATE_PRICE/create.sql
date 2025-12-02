copy (select category || '_cat' as category, date, price from range(today()-interval 1 year, today(), interval 1 day) z(date), range(10) t(category), range(1000) t(price)) to 'data/DATA_CATEGORY_DATE_PRICE/36m.parquet';
;
copy (select category || '_cat' as category, date, price from range(today()-interval 1 year, today(), interval 1 day) z(date), range(10) t(category), range(100000) t(price)) to 'data/DATA_CATEGORY_DATE_PRICE/3_6b.parquet';
