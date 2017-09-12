## Question 3-2: 
Create Table 3-9, the Customer Master augmented with the best-guess DUNS number, from Table 3-8 Customer Master with Sparse Data.  Select the DUNS number based on the closest match by city, state, and country in that order.

Table 3-8: Customer Master with Sparse Data
|Customer Site ID | Parent Customer | City | State | Country | DUNS |
|1|GE| Boston| MA| USA |123123123 |
|2|GE |Boston |MA |USA| null|
|3| GE |Worcester| MA| USA| null|
|4|GE|Syracuse |NY |USA |456456456 |
|5 |GE |Syracuse |NY |USA |null|
|6 |HSBC |London | null|  UK |789789789 |
|7 |HSBC |null |null |UK |null|

Table 3-9:  Customer Master w/Best Guess DUNS Numbers
Customer Site ID Parent Customer City State Country DUNS

|1|GE|Boston|MA|USA|123123123|
|2|GE|Boston|MA|USA|123123123|
|3|GE|Worcester|MA|USA|123123123|
|4|GE|Syracuse|NY|USA|456456456|
|5|GE|Syracuse|NY|USA|456456456|
|6|HSBC|London|null|UK|789789789|
|7|HSBC|null|null|UK|789789789|


# Answer:

```with t2 as (select parent_customer, city, state, country, max(duns) duns
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
order by site_id asc```



