## Question 3-2: 
Create Table 3-9, the Customer Master augmented with the best-guess DUNS number, from Table 3-8 Customer Master with Sparse Data.  Select the DUNS number based on the closest match by city, state, and country in that order.

### Table 3-8: Customer Master with Sparse Data

|Customer Site ID | Parent Customer | City | State | Country | DUNS|
|1|GE| Boston| MA| USA |123123123 |
|2|GE |Boston |MA |USA| (null)|
|3| GE |Worcester| MA| USA| (null)|
|4|GE|Syracuse |NY |USA |456456456 |
|5 |GE |Syracuse |NY |USA |(null)|
|6 |HSBC |London | (null)|  UK |789789789 |
|7 |HSBC |(null) |(null) |UK |(null)|

### Table 3-9:  Customer Master w/Best Guess DUNS Numbers

|Customer Site ID |Parent Customer |City |State |Country |DUNS|
|1|GE|Boston|MA|USA|123123123|
|2|GE|Boston|MA|USA|123123123|
|3|GE|Worcester|MA|USA|123123123|
|4|GE|Syracuse|NY|USA|456456456|
|5|GE|Syracuse|NY|USA|456456456|
|6|HSBC|London|(null)|UK|789789789|
|7|HSBC|(null)|(null)|UK|789789789|


# Answer:

``` SQL
with t2 as (select parent_customer, city, state, country, max(duns) duns
from t38
where duns is not (null)
group by 1,2,3,4),
t3 as (select parent_customer, state, country, max(duns) duns
from t38
where duns is not (null)
group by 1,2,3),
t4 as (select parent_customer, country, max(duns) duns
from t38
where duns is not (null)
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
|2|6/5/2004|(null)|Medium|
|3|4/3/2003|(null)|High|
|4|9/1/2002|4/15/2007|Medium|
|5|4/6/2007|11/1/2007|Low|
|6|7/1/2007|(null)|High|
|7|3/2/2005|8/1/2007|Low|


### Table 3-11

|Period|Total Employees at end of quarter|High Performers|Medium Performers|Low Performers|Total Attrition|Low Performer Attrition|Medium Performer Attrition|High Performer Attrition |
|-------|:------------:|:------------:|:------------:|:------------:|:------------:|:------------:|:------------:|:------------:|
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

### Question 3-4: 
Convert a sales hierarchy.  Table 3-12 lists the data structure for the sales hierarchy.  The table is organized so that the Role is a unique key.  Each role is assigned to one person.  If the role covers more than one District (or Region or Geo), then those columns are left blank.  The parent role defines the hierarchy.  The objective is to convert this structure into Table 3-13.  In Table 3-13, the Role is again a key, but this time it is augmented with the people who populate the Geo/Region/District for that individual.  Note that there may be districts with the same name that are managed by different people (e.g., NY).  In this case we would like to distinguish these two districts by adding a count to the district as in the Unique District column of Table 3-13.  Also note that one individual may have two different roles (as in the case for Marc Benioff).

#### Table 3-12: 

|Role|Parent Role|Person|Geo|Region|District|
|Rep-1|DM-1|Lou Gertsner|AME|West|CA|
|Rep-2|DM-1|Steve Ballmer|AME|West|CA|
|DM-1|RVP-1|Marc Benioff|AME|West|CA|
|RVP-1|GEO-1|Ray Ozzie|AME|West|(null)|
|WW|(null)|Larry Ellison|AME|(null)|(null)|
|GEO-1|WW|Bill Gates|AME|(null)|(null)|
|Rep-3|DM-1|Vinod Khosla|AME|West|CA|
|Rep-4|DM-1|Marc Benioff|AME|West|CA|
|DM-2|RVP-2|Mark Hurd|AME|East|NY|
|RVP-2|GEO-1|Mike Ruttgers|AME|East|(null)|
|Rep-5|DM-2|Sergey Brin|AME|East|NY|
|DM-3|RVP-2|Larry Page|AME|East|NY|
|Rep-6|DM-3|Scott McNealy|AME|East|NY|
|Rep-7|DM-3|Sam Palmisano|AME|East|NY|
|Rep-8|DM-3|Joe Tucci|AME|East|NY|

#### Table 3-13

|WW Person|Geo Person|Region Person|District Person|Role|Person|Geo|Region|District|Unique District|
|Larry Ellison|Bill Gates|Ray Ozzie|Marc Benioff|Rep-1|Lou Gertsner|AME|West|CA|CA|
|Larry Ellison|Bill Gates|Ray Ozzie|Marc Benioff|Rep-2|Steve Ballmer|AME|West|CA|CA|
|Larry Ellison|Bill Gates|Ray Ozzie|Marc Benioff|DM-1|Marc Benioff|AME|West|CA|CA|
|Larry Ellison|Bill Gates|Ray Ozzie|(null)|RVP-1|Ray Ozzie|AME|West|(null)|(null)|
|Larry Ellison|(null)|(null)|(null)| WW |Larry Ellison|(null)|(null)|(null)|(null)|
|Larry Ellison|Bill Gates|(null)|(null)|GEO-1|Bill Gates|AME|(null)|(null)|(null)|
|Larry Ellison|Bill Gates|Ray Ozzie|Marc Benioff|Rep-3|Vinod Khosla|AME|West|CA|CA|
|Larry Ellison|Bill Gates|Ray Ozzie|Marc Benioff|Rep-4|Marc Benioff|AME|West|CA|CA|
|Larry Ellison|Bill Gates|Mike Ruttgers|Mark Hurd|DM-2|Mark Hurd|AME|East|NY|NY - 1|
|Larry Ellison|Bill Gates|Mike Ruttgers|(null)|RVP-2|Mike Ruttgers|AME|East|(null)|(null)|
|Larry Ellison|Bill Gates|Mike Ruttgers|Mark Hurd|Rep-5|Sergey Brin|AME|East|NY|NY -1|
|Larry Ellison|Bill Gates|Mike Ruttgers|Larry Page|DM-3|Larry Page|AME|East|NY|NY -2|
|Larry Ellison|Bill Gates|Mike Ruttgers|Larry Page|Rep-6|Scott McNealy|AME|East|NY|NY -2|
|Larry Ellison|Bill Gates|Mike Ruttgers|Larry Page|Rep-7|Sam Palmisano|AME|East|NY|NY -2|
|Larry Ellison|Bill Gates|Mike Ruttgers|Larry Page|Rep-8|Joe Tucci|AME|East|NY|NY -2|


### Answer
```SQL
Answer:
WITH RECURSIVE ww AS (
 SELECT role, parent_role, person,'' as sub, 1 as level
 FROM t1
 WHERE
 role = 'WW'
 UNION all
 SELECT
 e.role,
 e.parent_role, s.person as sub,
 e.person, s.level +1 as level
 FROM
ww s
 INNER JOIN t1 e ON s.role = e.parent_role
),

geo AS (
 SELECT role, parent_role, person,'' as sub, 1 as level
 FROM t1
 WHERE
 role = 'GEO-1'
 UNION all
 SELECT
 e.role,
 e.parent_role, s.person as sub,
 e.person, s.level +1 as level
 FROM
geo s
 INNER JOIN t1 e ON s.role = e.parent_role
),

rvp AS (
 SELECT role, parent_role, person,'' as sub, 1 as level
 FROM t1
 WHERE
 role like '%RVP%'
 UNION all
 SELECT
 e.role,
 e.parent_role, s.person as sub,
 e.person, s.level +1 as level
 FROM
rvp s
 INNER JOIN t1 e ON s.role = e.parent_role
),
dm AS (
 SELECT role, parent_role, person,'' as sub, 1 as level
 FROM t1
 WHERE
 role like '%DM%'
 UNION all
 SELECT
 e.role,
 e.parent_role, s.person as sub,
 e.person, s.level +1 as level
 FROM
dm s
 INNER JOIN t1 e ON s.role = e.parent_role
),
uniq_dist as (
select person, district, concat(district,'-', row_number() over (partition by district)) as unique_district
from
(select person, district
from t1
where role like '%DM%'
group by person, district) d)

SELECT ww.person as worldwide_person, geo.person as geo_person, rvp.person as region_person, dm.person as district_person,
ww.role as role, 
t1.person as person, 
t1.geo, t1.region, t1.district, d.unique_district
FROM ww
LEFT join t1 using(role)
LEFT JOIN geo using(role)
LEFT join rvp using(role)
LEFT join dm using(role)
LEFT join uniq_dist d on d.district = t1.district and d.person=dm.person
```

### Question 3-5: 
Provide the SQL code for the query(ies) to calculate the total number of tests running each Tuesday from 1/1/2016-2/1/2016. The inputs can be found in table 4-3. Desired result can be found in Table 4-4.

#### Table 3-14

|id|test_name|start_date|end_date|
|1|Test 1|1/1/16|1/10/16|
|2|Test 2|1/1/16|1/25/16|
|3|Test 3|1/6/16|1/10/16|
|4|Test 4|1/6/16|1/31/16|
|5|Test 5|1/11/16|1/25/16|
|6|Test 6|1/11/16|2/1/16|
|7|Test 7|1/15/16|1/21/16|
|8|Test 8|1/19/16|1/20/16|
|9|Test 9|1/19/16|1/22/16|
|10|Test 10|1/22/16|1/25/16|
|11|Test 11|1/22/16|1/28/16|
|12|Test 12|1/25/16|2/1/16|
|13|Test 13|1/28/16|2/1/16|

#### Table 3-15

|date|number_of_active_test|
|1/5/16|2|
|1/12/16|6|
|1/19/16|7|
|1/26/16|7|

#### Answer:
``` SQL
with t2 as (select date_trunc('day',dd)::date as date
from generate_series('2016-01-01'::date, '2016-02-01'::date, '1 day'::interval) dd
where extract(dow from dd) =2)
select t2.date, sum(case when t2.date >= start_date and t2.date <= end_date then 1 else 0 end) as number_of_active_tests
from t2
cross join Table 3-14 as t1
group by t2.date
order by 1 asc
```

Question 3-6: Provide the SQL code for the query(ies) to calculate for each location how far away in km the nearest neighbor is. 

#### Table 3-1

|location_id|location_name|lattitude|longitude|

|321678|Chinatown Cafe|42.347479|-71.062537|
|321983|City Table|42.349034|-71.07955|
|482305|An Tain|42.35836|-71.053032|6
|46829|Adams Bed & Breakfast|42.346059|-71.087909|
|777353|Aisling Bed and Breakfast|42.337396|-71.074549|
|1954989|Italian Express Pizzeria|42.367298|-71.035713|
|1993528|MET Back Bay|42.351107|-71.077586|
|6765701|Bonapita|42.355417|-71.058854|
|7906434|Boston Food Tours |42.363171|-71.05632|
|10027342|Caffe Bene|42.342252|-71.084461|



#### Table 3-17

|location_id| location_name |distance_in_km_to_nearest_neighbor_location|
|----|:---------:|----------------------------------|
|321678|Chinatown Cafe|0.9331|
|321983|City Table|0.2814|
|482305|An Tain|0.5796|
|646829|Adams Bed & Breakfast|0.5094|
|777353|Aisling Bed and Breakfast|0.9774|
|1954989|Italian Express Pizzeria|1.7357|
|1993528|MET Back Bay|0.2814|
|6765701|Bonapita|0.5796|
|7906434|Boston Food Tours |0.5993|
|10027342|Caffe Bene|0.5094|

#### Answer:
``` SQL
select t1.location_id, t1.location_name,
min(111.045*DEGREES(ACOS(COS(RADIANS(t1.lattitude)) * COS(RADIANS(t2.lattitude)) *
             COS(RADIANS(t1.longitude) - RADIANS(t2.longitude)) +
             SIN(RADIANS(t1.lattitude)) * SIN(RADIANS(t2.lattitude))))) as distance
from t1
cross join t1 as t2 
where t1.location_name != t2.location_name
group by 1,2
```