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
  call fact_monthly_summary_f1_prc(V_month);
insert into fact_monthly_report_kpi_asm 
-- Bước  4.3: Thực hiện các câu lệnh SQL và xử lý logic

	-- Tính chỉ số LTN 
select V_month as month_key,area_code, employee_id,criteria_code AS criteria_code, avg(value) as value, 
rank () over (partition by criteria_code  order by avg(value) desc) as rank_kpi
from th_kpi_asm_data
where month_report <=V_month
and value is not null
group by area_code, employee_id,criteria_code 
union all 
-- Tính chỉ số NPL bf WO (dư nợ nhóm 3,4,5+ write _off/(write_off +outstanding
select V_month as month_key, a.area_code, b.employee_id, c.criteria_code,value,
case 
when c.criteria_code ='SC04' then rank () over (partition by c.criteria_code  order by value)
when c.criteria_code ='FN01' then dense_rank () over (partition by c.criteria_code  order by value)
else dense_rank () over (partition by c.criteria_code order by value desc)
end as rank_kpi
from      (
            select area_code, 'SC04' as criteria_code, abs(value) as value  
			from fact_monthly_summary 
			where fin_id =26
			and area_code <>'00'
			and month_key =V_month
			
			union all 
			-- Tính các chỉ số fin 
			-- Tính CIR
			select area_code, 'FN01' as criteria_code, abs(value) as value  
			from fact_monthly_summary 
			where fin_id =22
			and area_code <>'00'
			and month_key =V_month
			union all 
			-- Tính chỉ số margin 
			select area_code,'FN02' as criteria_code ,value 
			from fact_monthly_summary 
			where fin_id =23
			and area_code <>'00'
			and month_key =V_month
			-- Tính chỉ số hiệu suất trên vốn
			union all 
			select area_code, 'FN03' as criteria_code,value 
			from fact_monthly_summary 
			where fin_id =24
			and area_code <>'00'
			and month_key =V_month
			-- Tính chỉ số hồ sơ bình quân nhân sự
			union all 
			select area_code, 'FN04' as criteria_code,value 
			from fact_monthly_summary 
			where fin_id =25
			and area_code <>'00'
			and month_key =V_month
		) a 
	      join ( 
	      select  distinct area_code, employee_id
	      from th_kpi_asm_data
	      where month_report  <=V_month
	      and value is not null
	      ) b on a.area_code =b.area_code 

         join dim_criteria_xlsx  c on a.criteria_code  =c.criteria_code  ;
-- Tính các tiêu chí level 2 
insert into fact_monthly_report_kpi_asm 
select month_key, area_code, employee_id, 'SC' as criteria_code, sum(rank_kpi) as value,
rank() over ( order by sum(rank_kpi)) as rank_kpi
from fact_monthly_report_kpi_asm 
where criteria_code like '%SC%'and month_key =V_month 
group by month_key, area_code, employee_id
union all 
select month_key, area_code, employee_id, 'FN' as criteria_code, sum(rank_kpi) as value,
rank() over ( order by sum(rank_kpi)) as rank_kpi
from fact_monthly_report_kpi_asm 
where criteria_code like '%FN%'and month_key =V_month
group by month_key, area_code, employee_id;
-- Tính điểm tổng
insert into fact_monthly_report_kpi_asm 
select month_key, area_code, employee_id, 'OV' as criteria_code, sum(value) as value,
rank() over ( order by sum(value)) as rank_kpi
from fact_monthly_report_kpi_asm 
where criteria_code in ('SC','FN') and month_key =V_month
group by month_key, area_code, employee_id
order by sum(value);
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
