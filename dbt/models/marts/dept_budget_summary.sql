-- Department budget summary
-- Aggregates expenditures by department and compares to budget
--
-- This model demonstrates a typical analyst transform:
-- joining staged data and computing metrics

with expenditure_totals as (
    select
        department_id,
        sum(amount) as total_spent,
        count(*) as transaction_count
    from {{ ref('stg_expenditures') }}
    group by department_id
)

select
    d.department_id,
    d.department_name,
    d.budget_amount,
    d.fiscal_year,
    coalesce(e.total_spent, 0) as total_spent,
    coalesce(e.transaction_count, 0) as transaction_count,
    d.budget_amount - coalesce(e.total_spent, 0) as budget_remaining,
    round(coalesce(e.total_spent, 0) / d.budget_amount * 100, 2) as percent_spent,
    -- Pudato version tracking (set via environment variables)
    '{{ var("pudato_logic_version") }}' as _pudato_logic_version,
    '{{ var("pudato_execution_id") }}' as _pudato_execution_id,
    current_timestamp as _processed_at
from {{ ref('stg_departments') }} d
left join expenditure_totals e on d.department_id = e.department_id
