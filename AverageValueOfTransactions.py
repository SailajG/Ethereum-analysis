from mrjob.job import MRJob
import time

class partA2(MRJob):

	def mapper(self,_,line):
		try:
			fields=line.split(',')
			if len(fields)==7:
				value=int(fields[3])
				time_epoch=int(fields[6])
				month=time.strftime("%m",time.gmtime(time_epoch))
				year=time.strftime("%y",time.gmtime(time_epoch))
				yield((month,year),(value,1))
		except:
			pass

	def combiner(self,features,values):
		count=0
		total=0
		for v in values:
			count+=v[1]
			total+=v[0]
		yield(features,(total,count))

	def reducer(self,features,values):
		count=0
		total=0
		for v in values:
			count+=v[1]
			total+=v[0]
		yield(features,total/count)

if __name__=='__main__':
	partA2.run()
				
