
  
    

  create  table "analytics"."homework"."iris_processed__dbt_tmp"
  
  
    as
  
  (
    

with import_iris as (
    select *
    from "analytics"."homework"."stg_iris"
)
select
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    -- K Bins Discretization
    
        

    
        NTILE(5) over (order by sepal_length)
    

 as sepal_length_quantile_bin,
        

    
        
        floor(
        (
            sepal_length - (min(sepal_length) over ())
        )
        /
        (
            ((max(sepal_length) over ()) - (min(sepal_length) over ())) / 4::float
        )
        ) + 1
    

 as sepal_length_uniform_bin,
    
        

    
        NTILE(5) over (order by sepal_width)
    

 as sepal_width_quantile_bin,
        

    
        
        floor(
        (
            sepal_width - (min(sepal_width) over ())
        )
        /
        (
            ((max(sepal_width) over ()) - (min(sepal_width) over ())) / 4::float
        )
        ) + 1
    

 as sepal_width_uniform_bin,
    
        

    
        NTILE(5) over (order by petal_length)
    

 as petal_length_quantile_bin,
        

    
        
        floor(
        (
            petal_length - (min(petal_length) over ())
        )
        /
        (
            ((max(petal_length) over ()) - (min(petal_length) over ())) / 4::float
        )
        ) + 1
    

 as petal_length_uniform_bin,
    
        

    
        NTILE(5) over (order by petal_width)
    

 as petal_width_quantile_bin,
        

    
        
        floor(
        (
            petal_width - (min(petal_width) over ())
        )
        /
        (
            ((max(petal_width) over ()) - (min(petal_width) over ())) / 4::float
        )
        ) + 1
    

 as petal_width_uniform_bin,
    
-- Scaling
    
        

    

    
    
    

    
    
    

    (
        sepal_length - 5.8
    )
    /
    (
        6.4 - 5.1
    )

 as sepal_length_robust_scaled,
        

    (sepal_length) / (max(abs(sepal_length)) over ())::FLOAT

 as sepal_length_max_absolute_scaled,
        

    
    (
        ((sepal_length) - (min(sepal_length) over ()))
        /
        ((max(sepal_length) over ()) - (min(sepal_length) over ()))::FLOAT
    )
    *
    (1.0 - 0.0)
    +
    0.0

 as sepal_length_max_min_max_scaled,
    
        

    

    
    
    

    
    
    

    (
        sepal_width - 3.0
    )
    /
    (
        3.3 - 2.8
    )

 as sepal_width_robust_scaled,
        

    (sepal_width) / (max(abs(sepal_width)) over ())::FLOAT

 as sepal_width_max_absolute_scaled,
        

    
    (
        ((sepal_width) - (min(sepal_width) over ()))
        /
        ((max(sepal_width) over ()) - (min(sepal_width) over ()))::FLOAT
    )
    *
    (1.0 - 0.0)
    +
    0.0

 as sepal_width_max_min_max_scaled,
    
        

    

    
    
    

    
    
    

    (
        petal_length - 4.35
    )
    /
    (
        5.1 - 1.6
    )

 as petal_length_robust_scaled,
        

    (petal_length) / (max(abs(petal_length)) over ())::FLOAT

 as petal_length_max_absolute_scaled,
        

    
    (
        ((petal_length) - (min(petal_length) over ()))
        /
        ((max(petal_length) over ()) - (min(petal_length) over ()))::FLOAT
    )
    *
    (1.0 - 0.0)
    +
    0.0

 as petal_length_max_min_max_scaled,
    
        

    

    
    
    

    
    
    

    (
        petal_width - 1.3
    )
    /
    (
        1.8 - 0.3
    )

 as petal_width_robust_scaled,
        

    (petal_width) / (max(abs(petal_width)) over ())::FLOAT

 as petal_width_max_absolute_scaled,
        

    
    (
        ((petal_width) - (min(petal_width) over ()))
        /
        ((max(petal_width) over ()) - (min(petal_width) over ()))::FLOAT
    )
    *
    (1.0 - 0.0)
    +
    0.0

 as petal_width_max_min_max_scaled,
    

    -- Log Transformation
    
        
    
    
        case
            when sepal_length is null or sepal_length + 0 <= 0 then null
            else log10(sepal_length + 0)
        end
    
 as sepal_length_logged,
    
        
    
    
        case
            when sepal_width is null or sepal_width + 0 <= 0 then null
            else log10(sepal_width + 0)
        end
    
 as sepal_width_logged,
    
        
    
    
        case
            when petal_length is null or petal_length + 0 <= 0 then null
            else log10(petal_length + 0)
        end
    
 as petal_length_logged,
    
        
    
    
        case
            when petal_width is null or petal_width + 0 <= 0 then null
            else log10(petal_width + 0)
        end
    
 as petal_width_logged,
    

    -- Binarization
    
        

    

    
        

        
    

    case
        when sepal_length >=
            
                5.8
            
            then 1
        else 0
    end

 as sepal_length_binarized,
    
        

    

    
        

        
    

    case
        when sepal_width >=
            
                3.0
            
            then 1
        else 0
    end

 as sepal_width_binarized,
    
        

    

    
        

        
    

    case
        when petal_length >=
            
                4.35
            
            then 1
        else 0
    end

 as petal_length_binarized,
    
        

    

    
        

        
    

    case
        when petal_width >=
            
                1.3
            
            then 1
        else 0
    end

 as petal_width_binarized,
    

    -- Standardization
    
        

    
    (
        (sepal_length - avg(sepal_length) over ())
        /
        (stddev(sepal_length) over ())::FLOAT
    )
    *
    1
    +
    0

 as sepal_length_standardized,
    
        

    
    (
        (sepal_width - avg(sepal_width) over ())
        /
        (stddev(sepal_width) over ())::FLOAT
    )
    *
    1
    +
    0

 as sepal_width_standardized,
    
        

    
    (
        (petal_length - avg(petal_length) over ())
        /
        (stddev(petal_length) over ())::FLOAT
    )
    *
    1
    +
    0

 as petal_length_standardized,
    
        

    
    (
        (petal_width - avg(petal_width) over ())
        /
        (stddev(petal_width) over ())::FLOAT
    )
    *
    1
    +
    0

 as petal_width_standardized,
    
-- Interactions
    
        
            
            
            

    
        (sepal_length * sepal_width)
    

 as sepal_length_x_sepal_width_interaction,
            

    
        (sepal_length + sepal_width)
    

 as sepal_length_plus_sepal_width_interaction,
        
            
            
            

    
        (sepal_length * petal_length)
    

 as sepal_length_x_petal_length_interaction,
            

    
        (sepal_length + petal_length)
    

 as sepal_length_plus_petal_length_interaction,
        
            
            
            

    
        (sepal_length * petal_width)
    

 as sepal_length_x_petal_width_interaction,
            

    
        (sepal_length + petal_width)
    

 as sepal_length_plus_petal_width_interaction,
        
    
        
            
            
            

    
        (sepal_width * petal_length)
    

 as sepal_width_x_petal_length_interaction,
            

    
        (sepal_width + petal_length)
    

 as sepal_width_plus_petal_length_interaction,
        
            
            
            

    
        (sepal_width * petal_width)
    

 as sepal_width_x_petal_width_interaction,
            

    
        (sepal_width + petal_width)
    

 as sepal_width_plus_petal_width_interaction,
        
    
        
            
            
            

    
        (petal_length * petal_width)
    

 as petal_length_x_petal_width_interaction,
            

    
        (petal_length + petal_width)
    

 as petal_length_plus_petal_width_interaction,
        
    
        
    
species,    
    -- Label Encoding
    

    dense_rank() over (order by species) - 1

 as species_label_encoded,
    -- One Hot Encoding
    

    

    
        
    

    

    

        case
            when species = 'setosa' then 1
            else 0
        end as is_species__setosa,

    

        case
            when species = 'versicolor' then 1
            else 0
        end as is_species__versicolor,

    

        case
            when species = 'virginica' then 1
            else 0
        end as is_species__virginica,

    

    case
        when species is null then 1
        else 0
    end as is_species__


from "analytics"."homework"."stg_iris"
  );
  