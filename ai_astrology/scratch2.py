from astrology import Dog
import astrology_data_pipeline as adp
import pandas as pd


df=pd.read_csv('survey.csv')
udf=pd.read_csv('users.csv')

Willy = Dog(udf.birthdate[0], '2:19:00 AM', '02461')
Julia = Dog('03/03/1988', '7:47:00 PM', '20007')
