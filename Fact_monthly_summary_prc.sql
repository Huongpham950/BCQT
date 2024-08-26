CREATE OR REPLACE PROCEDURE "Final_project".fact_monthly_summary_prc(IN V_month_key bigint DEFAULT NULL::bigint)
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
-- Tính số lượng nhân sự fin_id=2  
insert into fact_monthly_summary 
select 2 as fin_id,area_code,count(distinct employee_id) as value,V_month as month_key 
from th_kpi_asm_data 
where month_report <= V_month 
group by area_code ;
insert into fact_monthly_summary 
---I. THU NHẬP TỪ HĐ THẺ 
-- 2.1 Tính chỉ số Lãi trong hạn và phí tăng hạn quá mức  có fin_id =14,17 giả sử tháng cần lấy là V_month 
-- Tính theo rule 1
select
a.fin_id,
a.area_code,
case 
when a.area_code  ='00' then a.value 
else 
round(a.value +
ty_le_pb *a.value_head,2) 
end as value,
a.month_key 
from(  select
       14 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		from (
				select 
				SPLIT_PART(analysis_code , '.', 3) as area_code,
				sum(amount)/1000000 as value,
				V_month as month_key 
				from fact_txn_month_raw_data
				-- lấy theo đầu kế toán với account_code thuộc : 702000030002, 702000030001,702000030102
				where account_code in (702000030002, 702000030001,702000030102)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				group by SPLIT_PART(analysis_code , '.', 3) 
		       ) x
				join
		       (
			    -- Tính tổng cần phân bổ xuống đơn vị mạng lưới 
				select 14 as fin_id,
				sum(amount)/1000000 as value_head, 
				V_month  as month_key 
				from fact_txn_month_raw_data
				-- lấy theo đầu kế toán với account_code thuộc : 702000030002, 702000030001,702000030102
				where account_code in (702000030002, 702000030001,702000030102)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				 and substring(analysis_code,1,4)='HEAD'
				) y
				on x.month_key =y.month_key 
		union all
		select
		17 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		from (
				select 
				SPLIT_PART(analysis_code , '.', 3) as area_code,
				sum(amount)/1000000 as value,
				V_month  as month_key 
				from fact_txn_month_raw_data
				-- lấy theo đầu kế toán với account_code thuộc : 719000030002
				where account_code in (719000030002)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				group by SPLIT_PART(analysis_code , '.', 3) 
		      ) x
			join
	       (
		    -- Tính tổng cần phân bổ xuống đơn vị mạng lưới 
	        select 
			sum(amount)/1000000 as value_head, 
			V_month  as month_key 
			from fact_txn_month_raw_data
			-- lấy theo đầu kế toán với account_code thuộc : 719000030002
			where account_code in (719000030002)
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
			-- Nhóm theo khu vực
			 and substring(analysis_code,1,4)='HEAD'
			) y
			on x.month_key =y.month_key 
		) a
		-- Tính theo rule 2
left join (
-- Tính tỷ lệ DNCK bình quân sau write_off nhóm 1 theo khu vực
           select  area_code, avg(dnck)/sum(avg(dnck)) over (partition by 1) as ty_le_pb 
		   from (
							select kpi_month, area_code, sum(outstanding_principal) as dnck
							from th_fact_kpi_month_raw_data 
							where kpi_month <= V_month
							and max_bucket =1
							group by kpi_month,area_code 
			     ) k
		   group by area_code 
		   )b
on a.area_code =b.area_code 
--Bước 2.2 Lãi quá hạn có fin_id =15 lấy đầu account_code = 702000030012, 702000030112
union all
select
a.fin_id,
a.area_code,
case 
when a.area_code  ='00' then a.value 
else 
round(a.value +
ty_le_pb *a.value_head,2) 
end as value,
a.month_key 
from(  select
       15 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		from (
				select 
				SPLIT_PART(analysis_code , '.', 3) as area_code,
				sum(amount)/1000000 as value,
				V_month as month_key 
				from fact_txn_month_raw_data 
				-- lấy theo đầu kế toán với account_code thuộc : 702000030012, 702000030112
				where account_code in (702000030012, 702000030112)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				group by SPLIT_PART(analysis_code , '.', 3) 
		       ) x
				join
		       (
			    -- Tính tổng cần phân bổ xuống đơn vị mạng lưới 
				select 14 as fin_id,
				sum(amount)/1000000 as value_head, 
				V_month  as month_key 
				from fact_txn_month_raw_data ftmrd 
				-- lấy theo đầu kế toán với account_code thuộc : 702000030012, 702000030112
				where account_code in (702000030012, 7020000301122)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				 and substring(analysis_code,1,4)='HEAD'
				) y
				on x.month_key =y.month_key 
		) a
		-- Tính theo rule 2
left join (
-- Tính tỷ lệ DNCK bình quân sau write_off nhóm 2 theo khu vực
           select  area_code, avg(dnck)/sum(avg(dnck)) over (partition by 1) as ty_le_pb 
		   from (
							select kpi_month, area_code, sum(outstanding_principal) as dnck
							from th_fact_kpi_month_raw_data 
							where kpi_month <= V_month 
							and max_bucket =2
							group by kpi_month,area_code 
			     ) k
		   group by area_code 
		   )b
on a.area_code =b.area_code 

-- Bước 2.3 Phí bảo hiểm có fin_id =16 lấy đầu account_code = 716000000001
union all
select
a.fin_id,
a.area_code,
case 
when a.area_code  ='00' then a.value 
else 
round(a.value +
ty_le_pb *a.value_head,2) 
end as value,
a.month_key 
from(  select
       16 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		from (
				select 
				SPLIT_PART(analysis_code , '.', 3) as area_code,
				sum(amount)/1000000 as value,
				V_month as month_key 
				from fact_txn_month_raw_data ftmrd 
				-- lấy theo đầu kế toán với account_code thuộc : 716000000001
				where account_code in (716000000001)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				group by SPLIT_PART(analysis_code , '.', 3) 
		       ) x
				join
		       (
			    -- Tính tổng cần phân bổ xuống đơn vị mạng lưới 
				select 14 as fin_id,
				sum(amount)/1000000 as value_head, 
				V_month  as month_key 
				from fact_txn_month_raw_data ftmrd 
				-- lấy theo đầu kế toán với account_code thuộc : 716000000001
				where account_code in (716000000001)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				 and substring(analysis_code,1,4)='HEAD'
				) y
				on x.month_key =y.month_key 
		) a
		-- Tính theo rule 2
left join (
-- Tính tỷ lệ số lượng thẻ psdn mỗi khu vực
			select area_code , sum(psdn)/sum(sum(psdn)) over (partition by 1) as ty_le_pb 
			from th_fact_kpi_month_raw_data 
			where kpi_month <= V_month 
			and outstanding_principal  is not null
			group by area_code 
		   )b
on a.area_code =b.area_code 

-- Bước 2.5 Phí thanh toán chậm  fin_id =18 lấy đầu account_code = 719000030003,719000030103,790000030003,790000030103,790000030004,790000030104
-- Tính theo rule 1
union all 
select
a.fin_id,
a.area_code,
case 
when a.area_code  ='00' then a.value 
else 
round(a.value +
ty_le_pb *a.value_head,2) 
end as value,
a.month_key 
from(  select
       18 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		from (
				select 
				SPLIT_PART(analysis_code , '.', 3) as area_code,
				sum(amount)/1000000 as value,
				V_month as month_key 
				from fact_txn_month_raw_data ftmrd 
				-- lấy theo đầu kế toán với account_code thuộc : 719000030003,719000030103,790000030003,790000030103,790000030004,790000030104
				where account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				group by SPLIT_PART(analysis_code , '.', 3) 
		       ) x
				join
		       (
			    -- Tính tổng cần phân bổ xuống đơn vị mạng lưới 
				select 14 as fin_id,
				sum(amount)/1000000 as value_head, 
				V_month  as month_key 
				from fact_txn_month_raw_data ftmrd 
				-- lấy theo đầu kế toán với account_code thuộc : 719000030003,719000030103,790000030003,790000030103,790000030004,790000030104
				where account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				 and substring(analysis_code,1,4)='HEAD'
				) y
				on x.month_key =y.month_key 
		) a
		-- Tính theo rule 2
left join (
-- Tính tỷ lệ DNCK bình quân sau write_off nhóm 2,3,4,5 theo khu vực
           select  area_code, avg(dnck)/sum(avg(dnck)) over (partition by 1) as ty_le_pb 
		   from (
							select kpi_month, area_code, sum(outstanding_principal) as dnck
							from th_fact_kpi_month_raw_data 
							where kpi_month <= V_month 
							and max_bucket in (2,3,4,5)
							group by kpi_month,area_code 
			     ) k
		   group by area_code 
		   )b
on a.area_code =b.area_code  

-- III. Chi phí thuần hoạt động khác
union all
select
a.fin_id,
a.area_code,
case 
when a.area_code  ='00' then a.value 
else 
round(a.value +
ty_le_pb *a.value_head,2) 
end as value,
a.month_key 
from(	
-- Tính DT kinh doanh : 702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,
-- 715000000001,715037000001,719000000001,709000000101,719000000101
        select 
        25 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		from 
		(  
			select 
			SPLIT_PART(analysis_code , '.', 3) as area_code,
			sum(amount)/1000000 as value,
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 
			where account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,
            715000000001,715037000001,719000000001,709000000101,719000000101)
			-- Giả sử tham số truyền vào V_month_
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
			-- Nhóm theo khu vực
			group by SPLIT_PART(analysis_code , '.', 3) 
		) x
		join
		    (
		    -- Tính tổng cần phân bổ xuống DVML
			select 
			sum(amount)/1000000 as value_head, 
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 
			where account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,
            715000000001,715037000001,719000000001,709000000101,719000000101)
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
			-- Nhóm theo khu vực
			 and substring(analysis_code,1,4)='HEAD'
			) y
		 on x.month_key =y.month_key  
		 union all 
      -- a. Tính CP hoa hồng  lấy theo đầu kế toán với account_code thuộc :  816000000001,816000000002,816000000003
        select 
        26 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		
		from 
		(  -- CP hoa hồng
			select 
			SPLIT_PART(analysis_code , '.', 3) as area_code,
			sum(amount)/1000000 as value,
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 816000000001,816000000002,816000000003
			where account_code in (816000000001,816000000002,816000000003)
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month
			-- Nhóm theo khu vực
			group by SPLIT_PART(analysis_code , '.', 3) 
		) x
		join
		    (
		    -- Tính tổng cần phân bổ xuống DVML
			select 
			sum(amount)/1000000 as value_head, 
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 816000000001,816000000002,816000000003
			where account_code in (816000000001,816000000002,816000000003)
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month
			-- Nhóm theo khu vực
			 and substring(analysis_code,1,4)='HEAD'
			) y
		 on x.month_key =y.month_key  
		 union all 
		-- Tính chi phí tuần KD khác fin_id =27 : 809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,
        --899000000003,899000000002,811000000101,819000060001
		select 
        27 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		V_month as month_key
		
		from 
		(
			select 
			SPLIT_PART(analysis_code , '.', 3) as area_code,
			sum(amount)/1000000 as value,
			V_month  as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,
            -- 899000000003,899000000002,811000000101,819000060001
			where account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,
			790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
			-- Giả sử tham số truyền vào V_month_key 
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
			-- Nhóm theo khu vực
			group by SPLIT_PART(analysis_code , '.', 3) 
		) x
		join
		    (
		    -- Tính tổng cần phân bổ xuống DVML
			select 
			sum(amount)/1000000 as value_head, 
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc :
			where account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,
			790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month
			-- Nhóm theo khu vực
			and substring(analysis_code,1,4)='HEAD'
			) y
		 on x.month_key =y.month_key  
       ) a
		-- Tính theo rule 2
left join (
		-- Tính tỷ lệ DNCK bình quân sau write_off 
	       select  area_code, avg(dnck)/sum(avg(dnck)) over (partition by 1) as ty_le_pb 
		   from (
				select kpi_month, area_code, sum(outstanding_principal) as dnck
				from th_fact_kpi_month_raw_data 
				where kpi_month <= V_month 
				group by kpi_month,area_code 
			     ) k
		   group by area_code
	       ) b on a.area_code = b.area_code  

--IV. Chi phí hoạt động
--Cộng các mã có cấu trúc AAA.BB.C.DD.EEE -> phân bổ về từng tỉnh/thành phố theo Số lượng SM của tỉnh đó
union all
select 
a.fin_id,
a.area_code,
case 
when a.area_code ='00' then a.value 
else 
a.value +
ty_le_pb*a.value_head
end as value,
a.month_key
from(	
     -- Tính CP Thuế : 29	LN05001	i.     CP thuế, phí 
        select 
        29 as fin_id,
		x.area_code, 
		x.value, 
		y.value_head,
		x.month_key 
		
		from 
		(  -- CP hoa hồng
			select 
			SPLIT_PART(analysis_code , '.', 3) as area_code,
			round(sum(amount)/1000000,2) as value,
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 831000000001,831000000002,832000000101,832000000001,831000000102
			where account_code in (831000000001,831000000002,832000000101,832000000001,831000000102)
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
			-- Nhóm theo khu vực
			group by SPLIT_PART(analysis_code , '.', 3) 
		) x
		join
		    (
		    -- Tính tổng cần phân bổ xuống DVML
			select 
			round(sum(amount)/1000000,2) as value_head, 
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc :
			where account_code in (831000000001,831000000002,832000000101,832000000001,831000000102)
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
			-- Nhóm theo khu vực
			 and substring(analysis_code,1,4)='HEAD'
			) y
		 on x.month_key =y.month_key		
       -- 30	LN05002	ii.     CP nhân viên lấy theo đầu kế toán với account_code thuộc :  85x
		union all
		select 
        30 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		
		from 
		(  
			select 
			SPLIT_PART(analysis_code , '.', 3) as area_code,
			round(sum(amount)/1000000,2) as value,
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 85x
			where left(cast(account_code as text),2) = '85'
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
			-- Nhóm theo khu vực
			group by SPLIT_PART(analysis_code , '.', 3) 
		) x
		join
		    (
		    -- Tính tổng cần phân bổ xuống DVML
			select 
			round(sum(amount)/1000000,2) as value_head, 
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 85x
			where left(cast(account_code as text),2) = '85'
			-- Giả sử tham số truyền vào V_month_
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
			-- Nhóm theo khu vực
			 and substring(analysis_code,1,4)='HEAD'
			) y
		 on x.month_key =y.month_key
         -- 31	 CP quản lý với đầu  account_code thuộc :  86x
         union all
         select 
        31 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		
		from 
		(  
			select 
			SPLIT_PART(analysis_code , '.', 3) as area_code,
			round(sum(amount)/1000000,2) as value,
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 86x
			where left(cast(account_code as text),2) = '86'
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
			-- Nhóm theo khu vực
			group by SPLIT_PART(analysis_code , '.', 3)
		) x
		join
		    (
		    -- Tính tổng cần phân bổ xuống DVML
			select 
			round(sum(amount)/1000000,2) as value_head, 
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 86x
			where left(cast(account_code as text),2) = '86'
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month
			-- Nhóm theo khu vực
			 and substring(analysis_code,1,4)='HEAD'
			) y
		 on x.month_key =y.month_key
       -- 32 CP tài sản lấy theo đầu kế toán với account_code thuộc :  87x
		   union all
         select 
        32 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 	
		from 
		(  
			select 
			SPLIT_PART(analysis_code , '.', 3) as area_code,
			round(sum(amount)/1000000,2) as value,
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 	
			-- lấy theo đầu kế toán với account_code thuộc : 87x
			where left(cast(account_code as text),2) = '87'
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
			-- Nhóm theo khu vực
			group by SPLIT_PART(analysis_code , '.', 3) 
		) x
		join
		    (
		    -- Tính tổng cần phân bổ xuống DVML
			select 
			round(sum(amount)/1000000,2) as value_head, 
			V_month as month_key 
			from fact_txn_month_raw_data ftmrd 
			-- lấy theo đầu kế toán với account_code thuộc : 87x
			where left(cast(account_code as text),2) = '87'
			-- Giả sử tham số truyền vào V_month
			and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month
			-- Nhóm theo khu vực
			 and substring(analysis_code,1,4)='HEAD'
			) y
		 on x.month_key =y.month_key
	   ) a
left join 
      (select fin_id, area_code, value/sum(value) over (partition by fin_id, month_key)as ty_le_pb ,month_key 
      from fact_monthly_summary fms 
      where fin_id=2
      and month_key =V_month
      ) b
on a.month_key =b.month_key and a.area_code=b.area_code 
  
--- CHI PHÍ DỰ PHÒNG 9 -- Dự phòng cho vay: 790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001,
--882200050101, 882200020101, 882200060001,790000050101, 882200030101
union all 
select
a.fin_id,
a.area_code,
case 
when a.area_code  ='00' then a.value 
else 
round(a.value +
ty_le_pb *a.value_head,2) 
end as value,
a.month_key 
from(  select
        9 as fin_id,
		x.area_code,
		x.value, 
		y.value_head,
		x.month_key 
		from (
				select 
				SPLIT_PART(analysis_code , '.', 3) as area_code,
				sum(amount)/1000000 as value,
				V_month as month_key 
				from fact_txn_month_raw_data ftmrd 
				-- lấy theo đầu kế toán với account_code thuộc : 
				where account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001,
                882200050101, 882200020101, 882200060001,790000050101, 882200030101)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				group by SPLIT_PART(analysis_code , '.', 3) 
		       ) x
				join
		       (
			    -- Tính tổng cần phân bổ xuống đơn vị mạng lưới 
				select 14 as fin_id,
				sum(amount)/1000000 as value_head, 
				V_month  as month_key 
				from fact_txn_month_raw_data ftmrd 
				-- lấy theo đầu kế toán với account_code thuộc : 790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001,
                -- 882200050101, 882200020101, 882200060001,790000050101, 882200030101
				where account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001,
                882200050101, 882200020101, 882200060001,790000050101, 882200030101)
				-- Giả sử tham số truyền vào V_month
				and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month  
				-- Nhóm theo khu vực
				 and substring(analysis_code,1,4)='HEAD'
				) y
				on x.month_key =y.month_key 
		) a
		-- Tính theo rule 2
left join (
-- Tính tỷ lệ DNCK bình quân sau write_off nhóm 2,3,4,5 theo khu vực
           select  area_code, avg(dnck)/sum(avg(dnck)) over (partition by 1) as ty_le_pb 
		   from (
							select kpi_month, area_code, sum(outstanding_principal) as dnck
							from th_fact_kpi_month_raw_data 
							where kpi_month <= V_month 
							and max_bucket in (2,3,4,5)
							group by kpi_month,area_code 
			     ) k
		   group by area_code
		   )b
on a.area_code =b.area_code  
;
-- Tính các chi phí nhóm level 0 và 1 
 
insert into fact_monthly_summary
-- 4	LN01	 a.      Thu nhập từ hoạt động thẻ 
select 4 as fin_id, area_code, sum(value) as value, month_key 
from fact_monthly_summary 
where fin_id in (14,15,16,17,18)
and month_key = V_month
group by month_key , area_code;
-- II. Tính chi phí thuần KDV
-- 
-- -- Bước 2.5 CP vốn CCTG  fin_id =20,21,22 Chi phí CCTG, CP vốn TT2, CP vốn TT1
-- rule phân bổ CP*thu nhập từ thẻ vay theo khu vực/(thu nhập từ thẻ vay toàn hàng+DT nguồn vốn)
insert into fact_monthly_summary
select x.fin_id,y.area_code,
case 
when y.area_code ='00' then x.CTCG_HEAD*(y.total_value /(y.total_value+z.DT_nguon_von)) 
else x.CTCG_HEAD*(y.value/(y.total_value+z.DT_nguon_von))
end as value,
x.month_key
from(
-- Chi phí CCTG (tk kế toán : 803000000001) 
	select V_month as month_key,22 as fin_id, sum(amount)/1000000 as CTCG_HEAD
	from fact_txn_month_raw_data ftmrd 
	where account_code in (803000000001)
	and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
	union all
-- Chí phí CP TT1
	select V_month as month_key,21 as fin_id, sum(amount)/1000000 as CTCG_HEAD
	from fact_txn_month_raw_data ftmrd 
	where account_code in (802000000002,802000000003,802014000001,802037000001)
	and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month 
	union all
-- Chi phí CP TT2
	select V_month as month_key,20 as fin_id, sum(amount)/1000000 as CTCG_HEAD
	from fact_txn_month_raw_data ftmrd 
	where account_code in (801000000001,802000000001)
	and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month
	) x 
join(
-- Thu nhập từ hoạt đông từ thẻ vay DVML 
	select month_key,area_code,value, sum(value)over (partition by month_key) as total_value
	from fact_monthly_summary fms
	where fin_id = 4       
	-- Giả sử tham số truyền vào V_month
    and month_key = V_month
	) y on x.month_key=y.month_key 
join (
-- Doanh thu nguồn vốn toàn hàng 
	select V_month as month_key,  sum(amount)/1000000 as DT_nguon_von 
	from fact_txn_month_raw_data ftmrd2 
	where account_code  in (702000040001,702000040002,703000000001,703000000002,703000000003,703000000004, 
	                         721000000041,721000000037,721000000039,721000000013,721000000014,721000000036,723000000014, 
				             723000000037,821000000014,821000000037,821000000039,821000000041,821000000013,821000000036,
					         823000000014,823000000037,741031000001,741031000002,841000000001,841000000005,841000000004,
					         701000000001,701000000002,701037000001,701037000002,701000000101)
	-- Giả sử tham số truyền vào V_month
	and extract(year from transaction_date)*100 + extract(month from transaction_date) <=V_month
	 ) z on y.month_key =z.month_key;
-- 5	LN02	 b.     Chi phí thuần KDV  
insert into fact_monthly_summary
select  5 as fin_id, area_code, sum(value) as value, month_key 
from fact_monthly_summary 
where fin_id in (19,20,21,22)
and month_key = V_month
group by month_key, area_code 
-- 6	LN03	 c.      Chi phí thuần hoạt động khác  
union all
select 6 as fin_id, area_code, sum(value) as value, month_key 
from fact_monthly_summary 
where fin_id between 23 and 28 
and month_key = V_month
group by month_key , area_code 
-- 8	LN05	e.     Tổng chi phí hoạt động
union all
select 8 as fin_id, area_code, sum(value) as value, month_key 
from fact_monthly_summary 
where fin_id between 29 and 32 
and month_key = V_month
group by month_key , area_code ;
-- Tổng thu nhập hoạt đông fin_id=7
insert into fact_monthly_summary 
select 7 as fin_id, area_code, sum(value) as value, month_key 
from fact_monthly_summary 
where fin_id in (4,5,6)
and month_key = V_month
group by month_key , area_code ;
-- 1	LN	1.     Lợi nhuận trước thuế
insert into fact_monthly_summary 
select 1 as fin_id, area_code, sum(value) as value, month_key 
from fact_monthly_summary 
where fin_id in (7,8,9)
and month_key =V_month
group by month_key , area_code;
insert into fact_monthly_summary 
-- 10	FN01a.CIR (%)= tổng chi phí hoạt động/tổng thu nhập hoạt đông*100
select 10 as fin_id,x.area_code,
case 
when x.TN =0 then 0
else y.CP*100/x.TN
end as value,
x.month_key 
from    (
		 select  area_code, value as TN, month_key
		 from fact_monthly_summary 
		 where fin_id in (7)
		 and month_key =V_month 
		 ) x
left join (
	     select  area_code, value as CP, month_key
		 from fact_monthly_summary 
		 where fin_id in (8)
		 and month_key =V_month
		   ) y
on x.area_code=y.area_code 
-- 11	FN02	b.     Margin (%)= Lợi nhuận trc thuế/(thu nhập từ hoạt động thẻ cộng doanh thu)
union all
select 11 as fin_id,x.area_code,
case 
when y.TN =0 then 0
else x.LNTT*100/y.TN
end as value,
x.month_key 
from    (
		 select  area_code, sum(value) as LNTT, month_key
		 from fact_monthly_summary 
		 where fin_id in (1)
		 and month_key =V_month
		 group by month_key , area_code 
		 ) x
left join (
	     select  area_code, sum(value) as TN, month_key
		 from fact_monthly_summary 
		 where fin_id in (4,19,23,24,25)
		 and month_key =V_month
		 group by month_key , area_code 
		   ) y
on x.area_code=y.area_code 
-- 12	FN03	c.     Hiệu suất trên/vốn (%)= Chi phí thuần KDV/lợi nhuận trước thuế 
union all 
select 12 as fin_id,x.area_code,
case 
when y.TN =0 then 0
else x.LNTT*100/y.TN
end as value,
x.month_key 
from    (
		 select  area_code, sum(value) as LNTT, month_key
		 from fact_monthly_summary 
		 where fin_id in (1)
		 and month_key =V_month
		  group by month_key , area_code 
		 ) x
left join (
	     select  area_code, sum(value) as TN, month_key
		 from fact_monthly_summary 
		 where fin_id in (4,19,23,24,25)
		 and month_key =V_month
		 group by month_key , area_code 
		   ) y
on x.area_code=y.area_code 
-- 13	FN04	d.     Hiệu suất BQ/ Nhân sự=LNTT/SLNS
union all 
select 13 as fin_id,x.area_code,
case 
when x.SLNS =0 then 0
else y.LNTT*100/x.SLNS
end as value,
x.month_key 
from    (
		 select  area_code, value as SLNS, month_key
		 from fact_monthly_summary 
		 where fin_id in (2)
		 and month_key =V_month
		 ) x
left join (
	     select  area_code, value as LNTT, month_key
		 from fact_monthly_summary 
		 where fin_id in (1)
		 and month_key =V_month
		   ) y
on x.area_code=y.area_code ;
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
