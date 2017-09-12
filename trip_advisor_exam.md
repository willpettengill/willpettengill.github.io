## Question 3-2: 
Create Table 3-9, the Customer Master augmented with the best-guess DUNS number, from Table 3-8 Customer Master with Sparse Data.  Select the DUNS number based on the closest match by city, state, and country in that order.

### Table 3-8: Customer Master with Sparse Data

|Customer Site ID | Parent Customer | City | State | Country | DUNS|
|1|GE| Boston| MA| USA |123123123 |
|2|GE |Boston |MA |USA| null|
|3| GE |Worcester| MA| USA| null|
|4|GE|Syracuse |NY |USA |456456456 |
|5 |GE |Syracuse |NY |USA |null|
|6 |HSBC |London | null|  UK |789789789 |
|7 |HSBC |null |null |UK |null|

### Table 3-9:  Customer Master w/Best Guess DUNS Numbers

|Customer Site ID |Parent Customer |City |State |Country |DUNS|
|1|GE|Boston|MA|USA|123123123|
|2|GE|Boston|MA|USA|123123123|
|3|GE|Worcester|MA|USA|123123123|
|4|GE|Syracuse|NY|USA|456456456|
|5|GE|Syracuse|NY|USA|456456456|
|6|HSBC|London|null|UK|789789789|
|7|HSBC|null|null|UK|789789789|


# Answer:

``` SQL
with t2 as (select parent_customer, city, state, country, max(duns) duns
from t38
where duns is not null
group by 1,2,3,4),
t3 as (select parent_customer, state, country, max(duns) duns
from t38
where duns is not null
group by 1,2,3),
t4 as (select parent_customer, country, max(duns) duns
from t38
where duns is not null
group by 1,2)

select t38.Customer_Site_ID, t38.parent_customer, t38.city, t38.state, t38.country, coalesce(t2.duns, t3.duns, t4.duns) as duns
from table_3_8 as t38
left join t2 on t38.parent_customer=t38.parent_customer and t38.city=t2.city and t38.state=t2.state and t38.country = t2.country
left join t3 on t38.parent_customer=t3.parent_customer and t38.state=t3.state and t38.country = t3.country
left join t4 on t38.parent_customer=t4.parent_customer and t38.country=t4.country
order by site_id asc
```

## Question 3-3: 
Calculate the employee attrition Table 3-11 from the Employee master, Table 3-10

### Table 3-10: Employee Master


|Employee ID|Start Date|Termination Date|Performance Level|

|1|1/1/2007|3/1/2007|Low|
|2|6/5/2004|null|Medium|
|3|4/3/2003|null|High|
|4|9/1/2002|4/15/2007|Medium|
|5|4/6/2007|11/1/2007|Low|
|6|7/1/2007|null|High|
|7|3/2/2005|8/1/2007|Low|


### Table 3-11

|Period|Total Employees at end of quarter|High Performers|Medium Performers|Low Performers|Total Attrition|Low Performer Attrition|Medium Performer Attrition|High Performer Attrition |
|---|:------------:|:------------:|:------------:|:------------:|:------------:|:------------:|:------------:|:------------:|
|Q1 2007 |4 |1 |2 |1 |1 |1 |0 |0 |
|Q2 2007 |4 |1 |1 |2 |1 |0 |1 |0 |
|Q3 2007 |4 |2 |1 |1 |1 |1 |0 |0 |
|Q4 2007 |3 |2 |1 |0 |1 |1 |0 |0 |

### Answer:
``` SQL
with t2 as (
select 
employee_id,
date_part('quarter',termination_date) as tq,
CASE WHEN date_part('year',start_date) = 2007 then date_part('quarter',start_date) else 1 end as sq,
Case When performance_level = 'Low' then True else False end as Low_performer,
Case When performance_level = 'Medium' then True else False end as medium_performer,
Case When performance_level = 'High' then True else False end as High_performer
from Table 3-10),

t3 as (
select *
from
(select employee_id from Table 3-10) a
cross join
(select date_part('quarter',termination_date) as quarter
from Table 3-10 
where date_part('year',termination_date) = 2007) b),

t4 as (
select t3.employee_id, t3.quarter, 
first_value(a.sq) over (partition by t3.employee_id order by a.sq) as start_q,
first_value(t2.tq) over (partition by t3.employee_id order by t2.tq) as end_q,
b.low_performer, b.medium_performer, b.high_performer 
from t3
left join t2 on t2.tq=t3.quarter and t2.employee_id=t3.employee_id
left join t2 a on a.sq=t3.quarter and a.employee_id=t3.employee_id
left join t2 b on b.employee_id=t3.employee_id              
order by t3.employee_id asc, t3.quarter asc)

select quarter, 
sum(CASE WHEN quarter >= start_q and quarter < coalesce(end_q, 10) then 1 else 0 end) as total_employees_at_end_of_quarter,
sum(CASE WHEN quarter >= start_q and quarter < coalesce(end_q, 10) and high_performer then 1 else 0 end) as high_performers,
sum(CASE WHEN quarter >= start_q and quarter < coalesce(end_q, 10) and medium_performer then 1 else 0 end) as medium_performers,
sum(CASE WHEN quarter >= start_q and quarter < coalesce(end_q, 10) and low_performer then 1 else 0 end) as low_performers,
sum(CASE WHEN quarter = end_q then 1 else 0 end) as total_attrition,
sum(CASE WHEN quarter = end_q and low_performer then 1 else 0 end) as low_performer_attrition,
sum(CASE WHEN quarter = end_q and medium_performer then 1 else 0 end) as medium_performer_attrition,
sum(CASE WHEN quarter = end_q and high_performer then 1 else 0 end) as high_performer_attrition
from t4
group by 1
```