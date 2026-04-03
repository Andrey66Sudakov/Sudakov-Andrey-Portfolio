-- ============================================================================
-- SQL Analytics Project: Отчетность строительной компании
-- Автор: Андрей Судаков
-- Database: PostgreSQL (stroy схема)
-- Description: Описание: 10 аналитических запросов для строительной компании 
-- ============================================================================

-- Задание 1: Количество проектов, подписанных в 2023 году
SELECT COUNT(*) AS project_count
FROM project
WHERE EXTRACT(YEAR FROM sign_date) = 2023;

-- Задание 2: Общий возраст сотрудников, нанятых в 2022 году
SELECT justify_interval(
    SUM(AGE(CURRENT_DATE, hire_date))
) AS total_age
FROM employee
WHERE EXTRACT(YEAR FROM hire_date) = 2022;

-- Задание 3: Сотрудник с фамилией на 'М' (8 букв), работающий дольше всех
SELECT 
    CONCAT(p.first_name, ' ', p.last_name) AS employee_name,
    e.hire_date
FROM employee e
JOIN person p ON e.person_id = p.person_id
WHERE p.last_name LIKE 'М%' 
    AND LENGTH(p.last_name) = 8
ORDER BY e.hire_date
LIMIT 1;

-- Задание 4: Средний возраст уволенных сотрудников (не на проектах)
SELECT COALESCE(
    AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate))),
    0
) AS average_age
FROM employee e
JOIN person p ON e.person_id = p.person_id
WHERE e.dismissal_date IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM project pr
    WHERE pr.project_manager_id = e.employee_id
    OR e.employee_id = ANY(pr.employees_id)
);

-- Задание 5: Сумма платежей от контрагентов из Жуковский, Россия
SELECT SUM(pp.amount) AS total_payments
FROM project_payment pp
JOIN project p ON pp.project_id = p.project_id
JOIN customer c ON p.customer_id = c.customer_id
JOIN address a ON c.address_id = a.address_id
JOIN city ct ON a.city_id = ct.city_id
JOIN country co ON ct.country_id = co.country_id
WHERE ct.city_name = 'Жуковский'
    AND co.country_name = 'Россия'
    AND pp.fact_transaction_timestamp IS NOT NULL;

-- Задание 6: Максимальный бонус руководителя проекта (1% от стоимости)
SELECT 
    p.project_manager_id AS manager_id,
    CONCAT(per.first_name, ' ', per.last_name) AS manager_name,
    SUM(p.project_cost) * 0.01 AS bonus_amount
FROM project p
JOIN employee e ON p.project_manager_id = e.employee_id
JOIN person per ON e.person_id = per.person_id
WHERE p.status = 'Завершен'
GROUP BY p.project_manager_id, per.first_name, per.last_name
HAVING SUM(p.project_cost) * 0.01 = (
    SELECT MAX(total_bonus)
    FROM (
        SELECT SUM(project_cost) * 0.01 AS total_bonus
        FROM project
        WHERE status = 'Завершен'
        GROUP BY project_manager_id
    ) AS bonuses
)
ORDER BY bonus_amount DESC;

-- Задание 7: Даты пересечения порога 30 млн (накопительный итог авансов)
SELECT DISTINCT ON (DATE_TRUNC('month', plan_payment_date))
    plan_payment_date AS threshold_date
FROM (
    SELECT 
        plan_payment_date,
        SUM(amount) OVER (
            PARTITION BY DATE_TRUNC('month', plan_payment_date)
            ORDER BY plan_payment_date
        ) AS running_total
    FROM project_payment
    WHERE payment_type = 'Авансовый'
) t
WHERE running_total > 30000000
ORDER BY DATE_TRUNC('month', plan_payment_date), plan_payment_date;

-- Задание 8: Рекурсивный обход иерархии подразделений (id=17)
WITH RECURSIVE unit_hierarchy AS (
    SELECT unit_id
    FROM company_structure
    WHERE unit_id = 17
    UNION ALL
    SELECT cs.unit_id 
    FROM company_structure cs
    INNER JOIN unit_hierarchy uh ON cs.parent_id = uh.unit_id
)
SELECT SUM(ep.salary * COALESCE(ep.rate, 1)) AS total_actual_salary
FROM unit_hierarchy uh
JOIN position p ON p.unit_id = uh.unit_id
JOIN employee_position ep ON ep.position_id = p.position_id
WHERE p.is_vacant = false;

-- Задание 9: Скользящее среднее платежей + сравнение со стоимостью проектов
WITH payment_analysis AS (
    SELECT 
        SUM(moving_avg) AS total_moving_avg
    FROM (
        SELECT 
            EXTRACT(YEAR FROM fact_transaction_timestamp) AS payment_year,
            ROW_NUMBER() OVER (
                PARTITION BY EXTRACT(YEAR FROM fact_transaction_timestamp) 
                ORDER BY fact_transaction_timestamp
            ) AS payment_num,
            AVG(amount) OVER (
                ORDER BY fact_transaction_timestamp
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS moving_avg
        FROM project_payment
        WHERE fact_transaction_timestamp IS NOT NULL
    ) numbered_payments
    WHERE payment_num % 5 = 0
),
project_costs AS (
    SELECT 
        EXTRACT(YEAR FROM sign_date) AS project_year,
        SUM(project_cost) AS total_project_cost
    FROM project
    GROUP BY EXTRACT(YEAR FROM sign_date)
)
SELECT 
    pc.project_year,
    pc.total_project_cost
FROM project_costs pc
CROSS JOIN payment_analysis pa
WHERE pc.total_project_cost < pa.total_moving_avg;

-- Задание 10: Материализованное представление для отчётности
CREATE MATERIALIZED VIEW project_report_mv AS
WITH last_payments AS (
    SELECT DISTINCT ON (project_id)
        project_id,
        fact_transaction_timestamp AS last_payment_date,
        amount AS last_payment_amount
    FROM project_payment
    WHERE fact_transaction_timestamp IS NOT NULL
    ORDER BY project_id, fact_transaction_timestamp DESC
),
customer_works AS (
    SELECT 
        c.customer_id,
        STRING_AGG(tw.type_of_work_name, ', ' ORDER BY tw.type_of_work_name) AS work_types
    FROM customer c
    LEFT JOIN customer_type_of_work ctw ON c.customer_id = ctw.customer_id
    LEFT JOIN type_of_work tw ON ctw.type_of_work_id = tw.type_of_work_id
    GROUP BY c.customer_id
)
SELECT 
    p.project_id,
    p.project_name,
    lp.last_payment_date,
    lp.last_payment_amount,
    CONCAT(per.last_name, ' ', per.first_name) AS manager_name,
    c.customer_name,
    cw.work_types
FROM project p
LEFT JOIN last_payments lp ON p.project_id = lp.project_id
LEFT JOIN employee e ON p.project_manager_id = e.employee_id
LEFT JOIN person per ON e.person_id = per.person_id
LEFT JOIN customer c ON p.customer_id = c.customer_id
LEFT JOIN customer_works cw ON c.customer_id = cw.customer_id;

-- ============================================================================
-- Сводка результатов запросов:
-- 1. Проекты, подписанные в 2023 году: [количество]
-- 2. Общий возраст сотрудников, нанятых в 2022 году: количество лет, дней
-- 3. Сотрудник с фамилией на «М»: [имя]
-- 4. Средний возраст уволенных сотрудников: 0 (нет данных)
-- 5. Платежи от контрагентов из Жуковского: 36 335 369,19 руб.
-- 6. Максимальный бонус руководителя: 904 814,22 руб. (ID руководителя: 53)
-- 7. Даты пересечения порога 30 млн: 10 дат
-- 8. Сумма окладов подразделения №17: 3 540 000 руб.
-- 9. Год, где стоимость проектов < скользящего среднего: 2024 (169 млн < 323 млн)
-- 10. Материализованное представление: 112 строк, время выполнения 0,109 с
-- ============================================================================
