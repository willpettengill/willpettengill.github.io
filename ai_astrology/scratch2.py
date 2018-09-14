import astrology
import smtplib
from ai_astrology.astrology  import Dog
#import astrology_data_pipeline as adp
import pandas as pd
import json
import os, sys
#sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

df=pd.read_csv('ai_astrology/survey.csv')
udf=pd.read_csv('ai_astrology/users.csv')
Willy = Dog(udf.birthdate[0], '2:19:00 AM', '02461')
Julia = Dog('03/03/1988', '7:47:00 PM', '20007')
#{smptp: 'smtp.gmail.com'}



email()