-- Staging model for departments
-- Minimal transformation, mainly type casting and renaming

select
    department_id,
    department_name,
    budget_amount::decimal(15,2) as budget_amount,
    fiscal_year::int as fiscal_year
from {{ ref('raw_departments') }}
