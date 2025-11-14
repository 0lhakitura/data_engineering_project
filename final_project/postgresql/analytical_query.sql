-- Analytical Query: Sales Analysis (PostgreSQL)
-- Question: В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду вересня?
-- Translation: In which state were the most TVs purchased by customers aged 20-30 during the first decade of September?

SELECT
    upe.state,
    COUNT(*) AS tv_purchases_count
FROM gold.user_profiles_enriched upe
INNER JOIN silver.sales s
    ON upe.client_id = s.client_id
WHERE 
    LOWER(s.product_name) = 'tv'
    AND upe.age BETWEEN 20 AND 30
    AND s.purchase_date BETWEEN '2022-09-01' AND '2022-09-10'
    AND upe.state IS NOT NULL
GROUP BY upe.state
ORDER BY tv_purchases_count DESC
LIMIT 1;

