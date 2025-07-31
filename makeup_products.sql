SELECT * FROM makeup_db.makeup_products;

DELETE FROM makeup_db.makeup_products
WHERE `Product Name` IS NULL OR TRIM(`Product Name`) = '';


UPDATE makeup_db.makeup_products
SET Price = REPLACE(Price, '$', '');

ALTER TABLE makeup_db.makeup_products
MODIFY Price DECIMAL(10, 2);

UPDATE makeup_db.makeup_products
SET Rating = NULL
WHERE Rating < 0 OR Rating > 5;
UPDATE makeup_db.makeup_products
SET
  `Product Name` = TRIM(`Product Name`),
  Size = TRIM(Size),
  `Skin Concerns` = TRIM(`Skin Concerns`),
  `Key Ingredients` = TRIM(`Key Ingredients`),
  `Skin Types` = TRIM(`Skin Types`);

UPDATE makeup_db.makeup_products
SET `Skin Concerns` = 'Unknown'
WHERE `Skin Concerns` IS NULL OR TRIM(`Skin Concerns`) = '';

UPDATE makeup_db.makeup_products
SET `Key Ingredients` = 'Unknown'
WHERE `Key Ingredients` IS NULL OR TRIM(`Key Ingredients`) = '';

UPDATE makeup_db.makeup_products
SET `Skin Types` = 'Unknown'
WHERE `Skin Types` IS NULL OR TRIM(`Skin Types`) = '';

DELETE FROM makeup_db.makeup_products
WHERE Price IS NULL OR Price = 0;

-- Business queries

SELECT 'Dry Skin' AS concern, AVG(Rating) AS avg_rating, COUNT(*) AS count
FROM makeup_db.makeup_products
WHERE `Skin Concerns` LIKE '%Dry Skin%'
UNION
SELECT 'Sensitive Skin', AVG(Rating), COUNT(*) FROM makeup_products WHERE `Skin Concerns` LIKE '%Sensitive Skin%'
UNION
SELECT 'Acne', AVG(Rating), COUNT(*) FROM makeup_products WHERE `Skin Concerns` LIKE '%Acne%'
ORDER BY avg_rating DESC;

SELECT `Product Name`, `Key Ingredients`
FROM makeup_products
WHERE `Key Ingredients` LIKE '%Coenzyme Q10%' OR
      `Key Ingredients` LIKE '%Ceramides%'
ORDER BY `Product Name`;

SELECT `Product Name`, Price, Rating, `Skin Concerns`
FROM makeup_products
WHERE `Skin Concerns` LIKE '%Aging Skin%'
  AND Rating >= 4.5
  AND Price < 40;
  
  
SELECT `Product Name`, `Skin Types`, Review
FROM makeup_products
WHERE `Skin Types` IS NOT NULL
ORDER BY Review DESC
LIMIT 5;





