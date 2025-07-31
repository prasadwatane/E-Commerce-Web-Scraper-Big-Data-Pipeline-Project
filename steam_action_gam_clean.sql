SELECT * FROM steam_db.steam_action_games_clean;
-- First view the clean table (optional)
SELECT * FROM steam_db.steam_action_games_clean;

-- Then perform the INSERT with complete SELECT statement
INSERT INTO steam_action_games_clean (
	game_id,
    title, 
    release_date, 
    release_year, 
    price, 
    is_free, 
    image_url,
    price_category
)
SELECT
	NULL, -- Let AUTO_INCREMENT handle this
    TRIM(REPLACE(REPLACE(Title, '®', ''), '™', '')) AS title,
    CASE 
        WHEN `Release Date` REGEXP '[0-9]{1,2} [A-Za-z]{3}, [0-9]{4}' THEN
            STR_TO_DATE(`Release Date`, '%d %b, %Y')
        ELSE NULL
    END AS release_date,
    CASE 
        WHEN `Release Date` REGEXP '[0-9]{1,2} [A-Za-z]{3}, [0-9]{4}' THEN
            CAST(SUBSTRING_INDEX(`Release Date`, ', ', -1) AS UNSIGNED)
        ELSE NULL
    END AS release_year,
    CASE 
        WHEN `Final Price` = 'Free' OR `Final Price` IS NULL OR `Final Price` = '' THEN NULL
        ELSE CAST(REPLACE(REPLACE(`Final Price`, ',', '.'), '€', '') AS DECIMAL(10,2))
    END AS price,
    CASE 
        WHEN `Final Price` = 'Free' THEN TRUE
        ELSE FALSE
    END AS is_free,
    CASE 
        WHEN `Image URL` IS NULL OR `Image URL` = '' THEN NULL
        ELSE `Image URL`
    END AS image_url,
    CASE 
        WHEN `Final Price` = 'Free' THEN 'Free'
        WHEN `Final Price` IS NULL OR `Final Price` = '' THEN 'Unknown'
        WHEN CAST(REPLACE(REPLACE(`Final Price`, ',', '.'), '€', '') AS DECIMAL(10,2)) < 10 THEN 'Under 10€'
        WHEN CAST(REPLACE(REPLACE(`Final Price`, ',', '.'), '€', '') AS DECIMAL(10,2)) < 30 THEN '10-30€'
        WHEN CAST(REPLACE(REPLACE(`Final Price`, ',', '.'), '€', '') AS DECIMAL(10,2)) < 50 THEN '30-50€'
        ELSE '50€+'
    END AS price_category
FROM games  
WHERE Title IS NOT NULL AND Title != '';


-- This query identifies the top 10 most expensive games on the platform, which can help with competitive pricing analysis.
SELECT 
    title,
    price,
    release_year,
    price_category
FROM 
    steam_action_games_clean
WHERE 
    is_free = 0
    AND price IS NOT NULL
ORDER BY 
    price DESC
LIMIT 10;

-- This query provides a comparison between free-to-play and paid games, showing their distribution and average price for paid games.
SELECT 
    CASE 
        WHEN is_free = 1 THEN 'Free-to-Play'
        ELSE 'Paid'
    END AS game_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM steam_action_games_clean), 2) AS percentage,
    ROUND(AVG(CASE WHEN is_free = 0 THEN price ELSE NULL END), 2) AS avg_price_paid
FROM 
    steam_action_games_clean
GROUP BY 
    game_type;
    
-- This query analyzes game release trends over the years, showing the number of free vs. paid games and average prices.
SELECT 
    release_year,
    COUNT(*) AS games_released,
    SUM(CASE WHEN is_free = 1 THEN 1 ELSE 0 END) AS free_games,
    SUM(CASE WHEN is_free = 0 THEN 1 ELSE 0 END) AS paid_games,
    ROUND(AVG(CASE WHEN is_free = 0 THEN price ELSE NULL END), 2) AS avg_price
FROM 
    steam_action_games_clean
GROUP BY 
    release_year
ORDER BY 
    release_year DESC;
    
-- This query shows how games are distributed across different price categories, helping understand the pricing strategy on Steam.
SELECT 
    price_category,
    COUNT(*) AS game_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM steam_action_games_clean WHERE price_category != 'Unknown'), 2) AS percentage
FROM 
    steam_action_games_clean
WHERE 
    price_category != 'Unknown'
GROUP BY 
    price_category
ORDER BY 
    CASE price_category
        WHEN 'Free' THEN 0
        WHEN 'Under 10€' THEN 1
        WHEN '10-30€' THEN 2
        WHEN '30-50€' THEN 3
        WHEN '50€+' THEN 4
        ELSE 5
    END;