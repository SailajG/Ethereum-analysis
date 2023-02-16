import pyspark

def transact(line):
        try:
                fields = line.split(',')
                if len(fields)!=7:
                        return False
                int(fields[3])
                return True

        except:
                return False

def contract(line):
        try:
                fields = line.split(',')
                if len(fields)!=5:
                        return False
                return True
        except:
                return False

sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions")
actualtransactions = transactions.filter(transact)
maptransactions = actualtransactions.map(lambda i : (i.split(',')[2], int(i.split(',')[3])))
aggregatetransactions = maptransactions.reduceByKey(lambda c,d : c+d)
contracts = sc.textFile("/data/ethereum/contracts")
actualcontracts = contracts.filter(contract)
mapcontracts = actualcontracts.map(lambda j: (j.split(',')[0], None))
joined = aggregatetransactions.join(mapcontracts)

top10 = joined.takeOrdered(10, key = lambda l: -l[1][0])


with open('Comparisonoutput.txt', 'w') as f:
        for value in top10:
                f.write("{}:{}\n".format(value[0],value[1][0]))
