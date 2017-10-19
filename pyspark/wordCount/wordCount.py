from pyspark import SparkContext
from operator import add


def main():
    sc = SparkContext(appName= "wordsCount")
    lines = sc.textFile('words.txt')
    counts = lines.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))
    sc.stop()

if __name__ =="__main__":
    main()