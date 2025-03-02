### 1. Распределение клиентов по сферам деятельности

```sql
SELECT 
    job_industry_category,
    COUNT(*) AS client_count
FROM customer
GROUP BY job_industry_category
ORDER BY client_count DESC;
```

###  2. Сумма транзакций за каждый месяц по сферам деятельности
```sql
SELECT 
    DATE_TRUNC('month', t.transaction_date) AS month,
    c.job_industry_category,
    SUM(t.list_price) AS total_transactions
FROM transaction t
JOIN customer c 
    ON t.customer_id = c.customer_id
GROUP BY month, c.job_industry_category
ORDER BY month, c.job_industry_category;
```

### 3. Количество онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT
```sql
SELECT 
    t.brand,
    COUNT(*) AS online_order_count
FROM transaction t
JOIN customer c 
    ON t.customer_id = c.customer_id
WHERE c.job_industry_category = 'IT'
  AND t.order_status = 'confirmed'
  AND t.online_order = 1
GROUP BY t.brand;
```

### 4. Суммарные показатели транзакций по клиентам
```sql
SELECT 
    customer_id,
    SUM(list_price) AS total,
    MAX(list_price) AS max_price,
    MIN(list_price) AS min_price,
    COUNT(*) AS txn_count
FROM transaction
GROUP BY customer_id
ORDER BY total DESC, txn_count DESC;

```
```sql
WITH txn_stats AS (
  SELECT 
      customer_id,
      SUM(list_price) OVER (PARTITION BY customer_id) AS total,
      MAX(list_price) OVER (PARTITION BY customer_id) AS max_price,
      MIN(list_price) OVER (PARTITION BY customer_id) AS min_price,
      COUNT(*) OVER (PARTITION BY customer_id) AS txn_count,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date) AS rn
  FROM transaction
)
SELECT 
    customer_id, total, max_price, min_price, txn_count
FROM txn_stats
WHERE rn = 1
ORDER BY total DESC, txn_count DESC;
```


### 5. Клиенты с минимальной/максимальной суммой транзакций
```sql
SELECT 
    c.first_name, 
    c.last_name,
    t.total
FROM customer c
JOIN (
    SELECT 
        customer_id, 
        SUM(list_price) AS total
    FROM transaction
    GROUP BY customer_id
) t ON c.customer_id = t.customer_id
WHERE t.total = (
    SELECT MAX(total)
    FROM (
        SELECT customer_id, SUM(list_price) AS total
        FROM transaction
        GROUP BY customer_id
    ) sub
);
```


```sql
SELECT 
    c.first_name, 
    c.last_name,
    t.total
FROM customer c
JOIN (
    SELECT 
        customer_id, 
        SUM(list_price) AS total
    FROM transaction
    GROUP BY customer_id
) t ON c.customer_id = t.customer_id
WHERE t.total = (
    SELECT MIN(total)
    FROM (
        SELECT customer_id, SUM(list_price) AS total
        FROM transaction
        GROUP BY customer_id
    ) sub
);
```

### 6. Первые транзакции клиентов с использованием оконных функций
```sql
SELECT 
    transaction_id,
    customer_id,
    transaction_date,
    online_order,
    order_status,
    brand,
    product_line,
    product_class,
    product_size,
    list_price,
    standard_cost
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date) AS rn
    FROM transaction
) sub
WHERE rn = 1;
```

### Клиенты с максимальным интервалом между транзакциями
```sql
WITH txn_intervals AS (
    SELECT
        customer_id,
        transaction_date,
        LAG(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS prev_date
    FROM transaction
),
intervals AS (
    SELECT
        customer_id,
        EXTRACT(DAY FROM (transaction_date - prev_date)) AS diff_days
    FROM txn_intervals
    WHERE prev_date IS NOT NULL
),
max_intervals AS (
    SELECT 
        customer_id, 
        MAX(diff_days) AS max_interval
    FROM intervals
    GROUP BY customer_id
),
global_max AS (
    SELECT MAX(max_interval) AS global_max_interval
    FROM max_intervals
)
SELECT 
    c.first_name, 
    c.last_name, 
    c.job_title, 
    m.max_interval
FROM max_intervals m
JOIN global_max g 
    ON m.max_interval = g.global_max_interval
JOIN customer c 
    ON m.customer_id = c.customer_id;
```
