do $$
    declare
    movement_rec  record;
    ats_rec  record;
    new_ats integer;
    current_ats integer;

begin
/*   collect all type of movements  */
    drop table follain.vend_history;
 
    create table follain.vend_history 
    as select 
            vend_stock_movements.outlet_id,
            vend_stock_movements.status,
            vend_stock_movements.product_id,
            CASE
                WHEN vend_stock_movements.status = 'RECEIVED'  then received_at
                WHEN vend_stock_movements.status = 'STOCKTAKE_COMPLETE' THEN created_at
            END AS received_at,
            vend_stock_movements.received AS units
      from follain.vend_stock_movements
            where vend_stock_movements.status in ('RECEIVED','STOCKTAKE_COMPLETE')
      union all
        select 
             vend_stock_movements.source_outlet_id,
             vend_stock_movements.status,
             vend_stock_movements.product_id,    
             received_at,        
             vend_stock_movements.count * (-1) 
        from follain.vend_stock_movements
            where vend_stock_movements.status = 'RECEIVED' AND vend_stock_movements.type = 'OUTLET'
      union all
        select outlet_id ,
                'STOCKTAKE_COMPLETE' status,
                product_id,      
                asof_time,
                ats
        from follain.vend_product_inventory 
      union all      
        select
             vend_registers.outlet_id,
             'SALE' status,
             vend_sales.product_id,      
             date_trunc('hour',vend_sales.sale_date),
             sum(vend_sales.quantity*-1)
        from follain.vend_sales , follain.vend_registers 
            where vend_sales.register_id=vend_registers.register_id
            group by date_trunc('hour',vend_sales.sale_date), 
                    vend_sales.product_id, 
                    vend_registers.outlet_id;

/*   rebuild ATS movement using all transactions  */
    truncate follain.vend_product_inventory_rebuild;

    for movement_rec  in select sum(units) units,outlet_id,product_id,status,received_at from follain.vend_history 
        group by outlet_id,product_id,status,received_at  order by outlet_id,product_id,received_at,status loop

        for ats_rec  in select * from follain.vend_product_inventory_rebuild a where a.outlet_id=movement_rec.outlet_id and a.product_id=movement_rec.product_id  and a.asof_time = 
            (select max(asof_time) from follain.vend_product_inventory_rebuild m  where a.outlet_id=m.outlet_id and a.product_id=m.product_id and asof_time <= movement_rec.received_at) loop
        end loop;

        if ats_rec.ats is null then 
            current_ats :=0;
            else 
            current_ats=ats_rec.ats;
        end if;

        case when  movement_rec.status  in ( 'ATS','STOCKTAKE_COMPLETE') then new_ats=movement_rec.units;
            else new_ats :=current_ats +movement_rec.units;
        end case;

        if ats_rec.ats is null or ats_rec.ats<> new_ats then
            if movement_rec.received_at = ats_rec.asof_time
                then
                    update follain.vend_product_inventory_rebuild set ats=new_ats where asof_time=movement_rec.received_at and outlet_id=movement_rec.outlet_id and product_id=movement_rec.product_id;
                else
                if movement_rec.product_id is not null then
                    insert into follain.vend_product_inventory_rebuild (outlet_id,product_id,ats,asof_time) values(movement_rec.outlet_id,movement_rec.product_id,new_ats,movement_rec.received_at);
                end if;
            end if;
        end if;
    end loop;
end;    
$$ LANGUAGE plpgsql;
