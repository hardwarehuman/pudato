-- Staging model for expenditures
-- Minimal transformation, mainly type casting and renaming

select
    expenditure_id,
    department_id,
    amount::decimal(15,2) as amount,
    category,
    expenditure_date::date as expenditure_date
from "pudato"."main"."raw_expenditures"