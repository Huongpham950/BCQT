CREATE OR REPLACE PROCEDURE "Final_project".fact_monthly_summary_f1_prc(IN v_month_key bigint DEFAULT NULL::bigint)
 LANGUAGE plpgsql
AS $procedure$
-- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Phạm Ngọc HƯơng
    -- Ngày tạo: current_timestamp

    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -----------------------
    -- 4.1: Xử lý tham số và khởi tạo biến
    -- 4.2: Thực hiện các câu lệnh SQL và xử lý logic
    -- 4.3: Xử lý ngoại lệ và ghi log  vstart_time := clock_timestamp()
DECLARE 
    V_month int8;
	V_error_message text;
	v_start_time TIMESTAMP;
	v_end_time TIMESTAMP;
begin 
-- Bước 4.1: Xử lý tham số và khởi tạo biến 
	v_start_time = current_timestamp;
-- Xử lý tham số truyền vào nếu tham số truyền vào là null thì là tháng hiện tại
	if V_month_key is null then V_month  = extract (year from current_timestamp)*100+extract(month from current_timestamp);
	else V_month := V_month_key ;
	end if;
-- Xóa dữ liệu truyền vào ngày V_month_key trong bảng 
   delete from fact_monthly_summary  where month_key  = V_month_key;
-- Bước 4.2: Thực hiện các câu lệnh SQL và xử lý logic
insert into fact_monthly_summary 
-- Tính số lượng ASM theo từng vùng , cần tính thêm bình quân lũy kế theo từng tháng để tính phân bổ từ hội sở về DVML
select 21 as fin_id, area_code, count(distinct employee_id) as value,V_month as month_key 
from th_kpi_asm_data tkad  
where month_report = V_month 
and value is not null 
group by area_code ;
-- Tính các chỉ số lv3 
insert into fact_monthly_summary 
	select  x.fin_id_lv3 as fin_id , x.area_code, 
	       case 
	       when fin_id_lv3 in(1,4) then value + value_head * ty_le_pb_wo_1
	       when fin_id_lv3 =2 then value + value_head * ty_le_pb_wo_2 
	       when fin_id_lv3 =3 then value + value_head * ty_le_pb_psdn 
	       when fin_id_lv3 in(5,20) then value +value_head *ty_le_pb_wo_2_5 
	       when fin_id_lv3  in (12,13,14,15) then value + value_head *ty_le_pb_wo 
	       else value +value_head*ty_le_pb_ns 
	       end as value ,V_month as month_key
	from (
			select a.fin_id_lv3, a.area_code, value,value_head
			from (
					select fin_id_lv3, area_code,sum(amount) as value 
					from balance_txn 
					where month_key <=V_month
					group by area_code , fin_id_lv3
				  ) a
			join 
			     (
					select fin_id_lv3, area_code,sum(amount) as value_head
					from balance_txn 
					where month_key <=V_month
					and dvml_head='HEAD'
					group by area_code , fin_id_lv3
				 ) b on a.fin_id_lv3 =b.fin_id_lv3 
			where a.fin_id_lv3 in (1,2,3,4,5,12,13,14,15,16,17,18,19,20)
		  ) x 
	-- Tính tỷ lệ phân bổ 
	join (
	-- Tính tỷ lệ DNCK bình quân sau write_off nhóm 2 theo khu vực
	           select  area_code, avg(wo_1)/sum(avg(wo_1)) over (partition by 1) as ty_le_pb_wo_1,
	                               avg(wo_2)/sum(avg(wo_2)) over (partition by 1) as ty_le_pb_wo_2,
	                              avg(wo_2_5)/sum(avg(wo_2_5)) over (partition by 1) as ty_le_pb_wo_2_5,
	                              avg(wo)/sum(avg(wo)) over (partition by 1) as ty_le_pb_wo,
	                              sum(psdn)/sum(sum(psdn)) over (partition by 1) as ty_le_pb_psdn
			   from (
								select kpi_month, area_code, 
								sum(
								    case 
								    when max_bucket=1 then outstanding_principal
								    else 0
								    end ) as Wo_1,
								sum(
							    case 
							    when max_bucket=2 then outstanding_principal
							    else 0
							    end) as Wo_2,
							    sum(
							    case 
							    when max_bucket>=2 then outstanding_principal
							    else 0
							    end ) as Wo_2_5,
							    sum(outstanding_principal) as wo, 
							    sum(psdn) as psdn
								from th_fact_kpi_month_raw_data 
								where kpi_month <= V_month
								group by kpi_month,area_code 
				     ) k
			   group by area_code
		  ) y on x.area_code =y.area_code 
	join (
	-- Tỷ lệ ASM nhân sự tính lũy kế
			select area_code ,avg(value)/sum(avg(value)) over (partition by 1) as ty_le_pb_ns
			from (select area_code , month_report ,  count(distinct employee_id) as value
				  from th_kpi_asm_data 
				  where month_report <= V_month
				  and value is not null 
				  group by area_code,month_report  
				 ) x
			group by area_code 
		  ) z on z.area_code =x.area_code ;
-- Tính chỉ số chi phí thuần KDV
insert into fact_monthly_summary
     select  x.fin_id_lv3 as fin_id , area_code, round(value_head*ty_le_pb*1) as value,x.month_key
     from (
			select V_month  as month_key, fin_id_lv3, sum(amount) as value_head
			from balance_txn 
			where month_key <=V_month 
			and fin_id_lv3 in (7,8,9)
			and dvml_head='HEAD'
			group by fin_id_lv3
		  ) x 
	 join (
		-- Tính tỷ lệ phân bổ Thu nhập hoạt động từ thẻ vay ĐVML / ( DT nguồn vốn + lãi thu từ thẻ vay toàn hàng 
		-- Tính DT nguồn vốn
			select a.month_key, area_code, TNHĐ/(DT_von+tot_TNHĐ) as ty_le_pb 
			from (
					select V_month  as month_key,sum(amount) as DT_von
					from balance_txn 
					where month_key <=V_month 
					and fin_id_lv3 = 6
					and dvml_head='HEAD'
					group by fin_id_lv3
				 ) a
			-- TNHĐ thẻ 
			join (
					select month_key, area_code, sum(value) as TNHĐ, sum(sum(value)) over (partition by month_key) as tot_TNHĐ
					from fact_monthly_summary 
					where fin_id in(1,2,3,4,5)
					and month_key = V_month 
					group by month_key, area_code 
				  ) b on a.month_key = b.month_key 
			) y on x.month_key =y.month_key ;
-- Tính các chỉ số tài chỉnh 
insert into fact_monthly_summary 
-- Chỉ số CIR % Tổng chi phí hoạt động/ tổng thu nhập hoạt động
select 22 as fin_id, area_code, -CP/TN as value ,month_key
from (
		select month_key,area_code,
		       sum(case 
		           when fin_id between 1 and 15 then value 
		           else 0 
		           end ) as TN,
		       sum(case 
		           when fin_id between 16 and 19  then value 
		           else 0 
		           end ) as CP
		from fact_monthly_summary 
		where month_key =V_month
		group by area_code,month_key 
		
		) k 
union all 
select 23 as fin_id, area_code, LN/TN as value , month_key
from (
		select month_key,area_code,
		       sum(case 
		           when fin_id between 1 and 20 then value 
		           else 0 
		           end ) as LN,
		       sum(case 
		           when fin_id in (1,2,3,4,5,12) then value 
		           else 0 
		           end ) as TN
		from fact_monthly_summary 
		where month_key =V_month
		group by area_code,month_key 
		) k 

union all 
select  24 as fin_id, area_code, -LN/CP as value,month_key
from (
		select month_key,area_code,
		       sum(case 
		           when fin_id between 1 and 20 then value 
		           else 0 
		           end ) as LN,
		       sum(case 
		           when fin_id in (6,7,8,9) then value 
		           else 0 
		           end ) as CP
		from fact_monthly_summary 
		where month_key =V_month
		group by area_code,month_key
		) k 

union all 
select  25 as fin_id, a.area_code, LNTT/slns_bq as value,V_month as month_key
from (
		select month_key,area_code,
		       sum(case 
		           when fin_id between 1 and 20 then value 
		           else 0 
		           end ) as LNTT
		from fact_monthly_summary 
		where month_key =V_month
		group by area_code,month_key 
	 ) a 
join (
		select  area_code ,avg(value) as SLNS_bq
					from (select area_code , month_report ,  count(distinct employee_id) as value
						  from th_kpi_asm_data 
						  where month_report <= v_month 
						  and value is not null 
						  group by area_code,month_report 
						 ) x
		group by area_code
	 ) b on a.area_code =b.area_code 
-- 26  Tỷ lệ nợ xấu trước WO
union all 
select 26 as fin_id, area_code,
	   (avg(dnx) + avg(wo))/(avg(dnck)+avg(wo)) as value,
	   month_key
   from (
		   select V_month  as month_key,area_code,kpi_month, sum(outstanding_principal) as dnck,
		           sum(case
		           when max_bucket in (3,4,5) then outstanding_principal 
		           else 0
		           end )as dnx,
		           sum(case
		           when left (cast(write_off_month as varchar),4) >= left(cast(V_month  as varchar), 4) then write_off_balance_principal  
		           else 0
		           end) as wo
		  from th_fact_kpi_month_raw_data
		  where kpi_month <=V_month	 
		  group by kpi_month , area_code
	     ) x
  group by area_code,month_key
 union  all
select 27 as fin_id, area_code,
	   (avg(wo))/(avg(dnck)+avg(wo)) as value,
	   month_key
   from (
		   select V_month  as month_key,area_code,kpi_month, sum(outstanding_principal) as dnck,
		           sum(case
		           when left (cast(write_off_month as varchar),4) >= left(cast(V_month  as varchar), 4) then write_off_balance_principal  
		           else 0
		           end) as wo
		  from th_fact_kpi_month_raw_data
		  where kpi_month <=V_month	 
		  group by kpi_month , area_code
	     ) x
  group by area_code,month_key;
  v_end_time := current_timestamp  ;
-- Bước 4.3: Xử lý ngoại lệ và ghi log 
    INSERT INTO log_tracking (procedure_name , start_time , end_time , is_successful ,error_log, rec_created_dt )
    VALUES ('fact_monthly_summary_prc',v_start_time, v_end_time ,TRUE,null, CURRENT_TIMESTAMP);

-- Xử lý ngoại lệ và ghi log 
    EXCEPTION
        WHEN others THEN
              v_error_message := SQLERRM;
             -- Ghi nhận lỗi vào bảng log
            INSERT INTO log_tracking (procedure_name , start_time , end_time , is_successful ,error_log, rec_created_dt )
            VALUES ('fact_monthly_summary_prc',v_start_time,v_end_time , FALSE, v_error_message, CURRENT_TIMESTAMP);
    END;
$procedure$
;
