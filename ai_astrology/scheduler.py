#scheduler

import astrology_data_pipeline as adp 
import astrology as ast 

if __name__ == "__main__":
	#df, udf, sheet, data, w, ww = adp.transform()
	if adp.ping():
		print('success')
		adp.main()
		ast.main()	
	else:
		pass

* * * * * source activate py36 && bash /home/willpettengill.github.io/ai_astrology/main.bash && source deactivate