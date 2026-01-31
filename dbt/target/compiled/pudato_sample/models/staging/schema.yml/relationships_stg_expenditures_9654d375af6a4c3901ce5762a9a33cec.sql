
    
    

with child as (
    select department_id as from_field
    from "pudato"."main"."stg_expenditures"
    where department_id is not null
),

parent as (
    select department_id as to_field
    from "pudato"."main"."stg_departments"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


