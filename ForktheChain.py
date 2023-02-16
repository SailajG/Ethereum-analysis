from mrjob.job import MRJob
import time

class partDfork(MRJob):

	def mapper(self,_,line):
		try:
			fields=line.split(',')
			if len(fields)==7:
				price=int(fields[5])
				time_epoch=int(fields[6])
				day=time.strftime("%d",time.gmtime(time_epoch))
				month=time.strftime("%m",time.gmtime(time_epoch))
				year=time.strftime("%y",time.gmtime(time_epoch))
				if(month=="10" and year=="17"):
					yield(day,(price,1))
		except:
			pass

	def combiner(self,key,values):
		count=0
		total=0
		for v in values:
			count+=v[1]
			total+=v[0]
		yield(key,(total,count))

	def reducer(self,key,values):
		count=0
		total=0
		for v in values:
			count+=v[1]
			total+=v[0]
		yield(key,total/count)

if __name__=='__main__':
	partDfork.run()
