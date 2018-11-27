#scheduler

import astrology_data_pipeline as adp 
import astrology as ast 

if __name__ == "__main__":
	#df, udf, sheet, data, w, ww = adp.transform()
	if adp.ping():
		adp.main()
		ast.main()	
	else:
		pass

