from mrjob.job import MRJob
from mrjob.step import MRStep

class partB(MRJob):

	def mapper1(self,_,line):
		try:
			if(len(line.split(','))==7):
				fields=line.split(',')
				address=fields[2]
				value=int(fields[3])
				yield(address,(value,1))
			elif(len(line.split(','))==5):
				fields=line.split(',')
				address=fields[0]
				yield(address,(None,2))
		except:
			pass

	def reducer1(self,addresses,values):
		allvalues=[]
		flag=False
		for value in values:
			if value[1]==1:
				allvalues.append(value[0])
			elif value[1]==2:
				flag=True
		if (flag!=False and len(allvalues)!=0):
			yield(addresses,sum(allvalues))

	def mapper2(self, addresses,values):
		yield (None,(addresses,values))

	def reducer2(self,_,values):
		sorted_values=sorted(values,reverse=True,key=lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield(value[0],value[1])
			i+=1
			if i>=10:
				break
	def steps(self):
		return[MRStep(mapper=self.mapper1,reducer=self.reducer1), MRStep(mapper=self.mapper2,reducer=self.reducer2)]

if __name__=='__main__':
	partB.run()
