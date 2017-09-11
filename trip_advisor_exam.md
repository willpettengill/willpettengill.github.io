Question 3-2: Create Table 3-9, the Customer Master augmented with the best-guess DUNS number, from Table 3-8 the bare Customer Master.  Select the DUNS number based on the closest match by city, state, and country in that order.


| Customer Site ID | Parent Customer | City | State | Country | DUNS |
|1 |GE| Boston| MA| USA |123123123 |
|2 |GE |Boston |MA |USA|'null'|
|3| GE |Worcester| MA| USA
|4 ||GE ||Syracuse ||NY ||USA ||456456456 |

5
GE
Syracuse
NY
USA


6
HSBC
London


UK
789789789
7
HSBC




UK



Table 3-9:  Customer Master w/Best Guess DUNS Numbers
Customer Site ID Parent Customer City State Country DUNS
1
GE
Boston
MA
USA
123123123
2
GE
Boston
MA
USA
123123123
3
GE
Worcester
MA
USA
123123123
4
GE
Syracuse
NY
USA
456456456
5
GE
Syracuse
NY
USA
456456456
6
HSBC
London


UK
789789789
7
HSBC




UK
789789789
