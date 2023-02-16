from mrjob.job import MRJob
from mrjob.step import MRStep

class partC(MRJob):
	def mapper1(self,_,line):
		fields=line.split(',')
		try:
			if len(fields)==9:
				miner=fields[2]
				size=int(fields[4])
				yield(miner,size)

		except:
			pass

	def reducer1(self,key,value):
		yield(key,sum(value))

	def mapper2(self,key,value):
		yield(None,(key,value))

	def reducer2(self,_,values):
		sorted_values=sorted(values,reverse=True,key=lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield(value[0],value[1])
			i+=1
			if i>=10:
				break

	def steps(self):
		return [MRStep(mapper=self.mapper1,reducer=self.reducer1), MRStep(mapper=self.mapper2,reducer=self.reducer2)]

if __name__ == '__main__':
	partC.run()
