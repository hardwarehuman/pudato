
    
    

select
    department_id as unique_field,
    count(*) as n_records

from "pudato"."main"."dept_budget_summary"
where department_id is not null
group by department_id
having count(*) > 1


