CREATE OR REPLACE PROCEDURE "Final_project".fact_monthly_report_kpi_asm_prc(IN v_month_key bigint DEFAULT NULL::bigint)
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
    -- 4.2 : Call procedure fact_monthly_report_kpi_asm 
    -- 4.3: Thực hiện các câu lệnh SQL và xử lý logic
    -- 4.4: Xử lý ngoại lệ và ghi log  vstart_time := clock_timestamp()
-- Bước 4.1: Xử lý tham số và khởi tạo biến
DECLARE 
    V_month  int8;
	V_error_message text;
	v_start_time TIMESTAMP;
	v_end_time TIMESTAMP;
begin 
-- 
	v_start_time = current_timestamp;
-- Xử lý tham số truyền vào nếu tham số truyền vào là null thì là tháng hiện tại
	if V_month_key is null then V_month  = extract (year from current_timestamp)*100+extract(month from current_timestamp);
	else V_month := V_month_key ;
	end if;
-- Xóa dữ liệu truyền vào ngày V_month_key trong bảng 
   delete from fact_monthly_report_kpi_asm where month_key  = V_month_key;
-- Bước 4.2 : Call procedure fact_monthly_report_kpi_asm 
  call fact_monthly_summary_prc (V_month);
insert into fact_monthly_report_kpi_asm 
-- Bước  4.3: Thực hiện các câu lệnh SQL và xử lý logic
select V_month as month_key ,*, dense_rank  () over (partition by criteria_type_name order by type_score desc) as type_rank,
dense_rank () over ( order by overall_score desc) as overall_rank
from(
		select x.*, y.criteria_type_name,sum(rank_kpi) over (partition by x.employee_id, y.criteria_type_name) as type_score,
		sum(rank_kpi) over (partition by x.employee_id) as overall_score
		from ( 
		-- Tính chỉ số LTN 
				select area_code, employee_id,kpi_code AS criteria_code, avg(value) as value, 
				rank () over (partition by kpi_code order by avg(value) desc) as rank_kpi
				from th_kpi_asm_data
				where month_report <=V_month
				and value is not null
				group by area_code, employee_id,kpi_code 
				union all 
				-- Tính chỉ số NPL bf WO (dư nợ nhóm 3,4,5+ write _off/(write_off +outstanding
				select a.area_code, b.employee_id, criteria_code,value,
				case 
				when criteria_code ='S04' then dense_rank () over (partition by criteria_code  order by value)
				else dense_rank () over (partition by criteria_code order by value desc)
				end as rank_kpi
				from(
							select x.area_code, 'S04' as criteria_code,
							(avg(dnx) + avg(wo))/(avg(dnck)+avg(wo)) as value
								  from (
									  select kpi_month,area_code , sum(outstanding_principal) as dnck
									  from th_fact_kpi_month_raw_data
									  where kpi_month <=V_month		 
									  group by kpi_month, area_code 
								      ) x 
								  join (
									  select kpi_month, area_code , sum(outstanding_principal) as dnx
									  from th_fact_kpi_month_raw_data 
									  where kpi_month <=V_month
									  and max_bucket in (3,4,5)
									  group by kpi_month, area_code 
								      ) y on x.kpi_month =y.kpi_month and x.area_code =y.area_code 
								  join  (
								      select kpi_month, area_code ,sum(write_off_balance_principal) as wo
									  from th_fact_kpi_month_raw_data 
									  where kpi_month <=V_month
								      and left (cast(write_off_month as varchar),4) >= left(cast(V_month as varchar), 4) 
								      group by kpi_month, area_code 
								      ) z on x.kpi_month =z.kpi_month and x.area_code =z.area_code 
							      group by x.area_code 
						union all 
						-- Tính các chỉ số fin 
						-- Tính CIR
						select area_code, 'F01' as criteria_code, value 
						from fact_monthly_summary 
						where fin_id =10
						and area_code <>'00'
						and month_key =V_month
						union all 
						-- Tính chỉ số margin 
						select area_code,'F02' as criteria_code ,value 
						from fact_monthly_summary 
						where fin_id =11
						and area_code <>'00'
						and month_key =V_month
						-- Tính chỉ số hồ sơ bình quân nhân sự
						union all 
						select area_code, 'F03' as criteria_code,value 
						from fact_monthly_summary 
						where fin_id =12
						and area_code <>'00'
						and month_key =V_month
					) a 
				join ( 
				      select  distinct area_code, employee_id
				      from th_kpi_asm_data
				      where month_report  <=V_month
				      and value is not null
				      ) b on a.area_code =b.area_code 
		     ) x
		join dim_criteria  y on x.criteria_code  =y.criteria_code  
	) k;
v_end_time := current_timestamp  ;
-- 4.3: Xử lý ngoại lệ và ghi log 
    INSERT INTO log_tracking (procedure_name , start_time , end_time , is_successful ,error_log, rec_created_dt )
    VALUES ('fact_monthly_report_kpi_asm_prc',v_start_time, v_end_time ,TRUE,null, CURRENT_TIMESTAMP);

-- Xử lý ngoại lệ và ghi log 
    EXCEPTION
        WHEN others THEN
              v_error_message := SQLERRM;
             -- Ghi nhận lỗi vào bảng log
            INSERT INTO log_tracking (procedure_name , start_time , end_time , is_successful ,error_log, rec_created_dt )
            VALUES ('fact_monthly_report_kpi_asm_prc',v_start_time,v_end_time , FALSE, v_error_message, CURRENT_TIMESTAMP);
    END;
$procedure$
;
