
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    department_id as unique_field,
    count(*) as n_records

from "pudato"."main"."dept_budget_summary"
where department_id is not null
group by department_id
having count(*) > 1



  
  
      
    ) dbt_internal_test