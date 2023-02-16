from mrjob.job import MRJob
import time

class partA1(MRJob):
       
	def mapper(self,_,line):
		try:
			fields=line.split(',')
			if len(fields)==9:
				count=int(fields[8])
				time_epoch=int(fields[7])
				month=time.strftime("%m", time.gmtime(time_epoch))
				year=time.strftime("%y", time.gmtime(time_epoch))
				yield((month,year),count)
		except:
			pass

	def combiner(self,features,values):
		yield(features,sum(values))

	def reducer(self,features,values):
		yield(features,sum(values))

if __name__=='__main__':
        partA1.run()
