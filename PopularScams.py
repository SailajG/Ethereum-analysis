from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class partDscams(MRJob):
	def mapper1(self,_,lines):
		try:
			fields=lines.split(',')
			if len(fields)==7:
				address=fields[2]
				value=int(fields[3])
				yield(address,(value,0))
			
			else:
				line=json.loads(lines)
				keys=line["result"]

				for i in keys:
					record=line["result"][i]
					category=record["category"]
					status=record["status"]
					addresses=record["addresses"]

					for j in addresses:
						yield(j,((category,status),1))

		except:
			pass

	def reducer1(self,keys,values):
		val=0
		features=None

		for k in values:
			if k[1]==0:
				val=val+k[0]
			else:
				features=k[0]

		if features is not None:
			yield(features,val)

	def mapper2(self,key,value):
		yield(key,value)

	def reducer2(self, key, value):
		yield(key,sum(value))

	def steps(self):
		return [MRStep(mapper=self.mapper1,reducer=self.reducer1), MRStep(mapper=self.mapper2,reducer=self.reducer2)]

if __name__ == '__main__':
	partDscams.run()
