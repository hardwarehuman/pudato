
    
    

select
    expenditure_id as unique_field,
    count(*) as n_records

from "pudato"."main"."stg_expenditures"
where expenditure_id is not null
group by expenditure_id
having count(*) > 1


