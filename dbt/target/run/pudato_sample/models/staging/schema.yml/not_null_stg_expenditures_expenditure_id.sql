
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select expenditure_id
from "pudato"."main"."stg_expenditures"
where expenditure_id is null



  
  
      
    ) dbt_internal_test