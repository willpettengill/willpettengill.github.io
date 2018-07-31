from astrology import Dog
import astrology_data_pipeline as adp
import pandas as pd
import json
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

df=pd.read_csv('survey.csv')
udf=pd.read_csv('users.csv')

Willy = Dog(udf.birthdate[0], '2:19:00 AM', '02461')
Julia = Dog('03/03/1988', '7:47:00 PM', '20007')



import astrology
import astrology_data_pipeline as adp 
import smtplib

#{smptp: 'smtp.gmail.com'}


def email():
	smtpObj = smtplib.SMTP('smtp.gmail.com', 587)
	smtpObj.set_debuglevel(1)


	smtpObj.ehlo()

	smtpObj.starttls()



	fromaddr = 'starlightstellab@gmail.com'
	toaddrs = 'wwpettengill@gmail.com'
	msg = 'Subject: So long.\nITS THAT GIRL STELLA B'
	smtpObj.login(fromaddr, 'Gemini69')
	smtpObj.sendmail(fromaddr, toaddrs, msg)
	smtpObj.quit()


email()