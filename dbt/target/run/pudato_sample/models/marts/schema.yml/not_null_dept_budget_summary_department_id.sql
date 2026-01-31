
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select department_id
from "pudato"."main"."dept_budget_summary"
where department_id is null



  
  
      
    ) dbt_internal_test