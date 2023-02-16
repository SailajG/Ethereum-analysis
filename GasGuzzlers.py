import pyspark
import time

sc = pyspark.SparkContext()


def is_good_line_transactions(line):
	try:
		fields = line.split(',')
		if len(fields)!= 7:
			return False
		float(fields[5])
		float(fields[6])
		return True
	except:
		return False

def is_good_line_contracts(line):
	try:
		fields = line.split(',')
		if len(fields) != 5:
			return False
		float(fields[3])
		return True

	except:
		return False

def is_good_line_blocks(line):
	try:
		fields = line.split(',')
		if len(fields)!=9:
			return False

		float(fields[0])
		float(fields[3])
		float(fields[7])
		return True

	except:
		return False


linestransactions = sc.textFile('/data/ethereum/transactions')
linescontracts = sc.textFile('/data/ethereum/contracts')
linesblocks = sc.textFile('/data/ethereum/blocks')

cleanlinestransactions = linestransactions.filter(is_good_line_transactions)
cleanlinescontracts = linescontracts.filter(is_good_line_contracts)
cleanlinesblocks = linesblocks.filter(is_good_line_blocks)

field1 = cleanlinestransactions.map(lambda i: (float(i.split(',')[6]), float(i.split(',')[5])))
convert1 = field1.map(lambda (a,b): (time.strftime("%y.%m", time.gmtime(a)), (b,1)))
result1 = convert1.reduceByKey(lambda (a1, b1), (a2, b2): (a1+a2, b1+b2)).map(lambda j: (j[0], (j[1][0]/j[1][1])))

save1 = result1.sortByKey(ascending=True)
save1.saveAsTextFile('GasPoutput')


lines = cleanlinescontracts.map(lambda k: (k.split(',')[3], 1))


field2 = cleanlinesblocks.map(lambda b: (b.split(',')[0], (int(b.split(',')[3]), int(b.split(',')[6]), time.strftime("%y.%m", time.gmtime(float(b.split(',')[7]))))))
convert2 = field2.join(lines).map(lambda (id, ((a, b, c), d)): (c, ((a,b), d)))
result2 = convert2.reduceByKey(lambda ((a1,b1), c1) , ((a2, b2), c2): ((a1 + a2, b1 + b2), c1+c2)).map(lambda z: (z[0], (float(z[1][0][0]/z[1][1]), z[1][0][1]/ z[1][1])))

save2 = result2.sortByKey(ascending=True)
save2.saveAsTextFile('ComplexGasUoutput')
