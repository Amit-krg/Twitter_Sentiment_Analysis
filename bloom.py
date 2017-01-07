
from pyspark import SparkContext
from operator import add
import mmh3

def applyHash(bf, tweet):
    flag=0
    #print(tweet)
    #print(str(tweet[5]))
   #print(tweet)
    count=0
    for word in tweet.split():
        #print(words)
        if (bf.lookup(word.lower()) == 1):
            #print('true\n')
            count+=1;
    if count>=2:
        return tweet
    else:
        return None


#featurelist_path=r"D:\spark-2.0.1-bin-hadoop2.7\spark-2.0.1-bin-hadoop2.7\bin\source\feature_list.csv"

'''def filterTweet(rdd):
    bf= BloomFilter(500000,7)
    return rdd.map(lambda x:applyHash(bf,x)).filter(lambda s: len(s)>0)'''


class BloomFilter:
    
    def __init__(self,path,size=40000, hash_count=7):
        self.size = size
        self.hash_count = hash_count
        self.path=path
        self.bit_array = [None]*size
        for i in range(size):
                self.bit_array[i] = 0

        #initialize the hashmap for every object created
        sc_class = SparkContext(appName="Bloom_filter")        
        trainingdata = sc_class.textFile(self.path).collect()
        for line in trainingdata:
            #print(line)
            word_list=line.split(',')
            for words in word_list:
                if(words!=''):
                    #print("amit test:",words,type(words),end=" ")
                    words = words.replace(" ",'')
                    words=words.strip().encode('utf-8')
                    #print(words)
                    #print("word 1 %s")
                    self.add(words.lower())

        sc_class.stop()

    def add(self, string):
        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            self.bit_array[result] = 1
            
    def lookup(self, string):
        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            if self.bit_array[result] == 0:
                return 0# "Nope"
        return 1

#"Probably"
if __name__ == '__main__':
    #bf = BloomFilter(500000, 7)
    politicalfeature_path=r"D:\spark-2.0.1-bin-hadoop2.7\spark-2.0.1-bin-hadoop2.7\bin\source\feature_list_sports.csv"
    testdata_path=r"D:\spark-2.0.1-bin-hadoop2.7\spark-2.0.1-bin-hadoop2.7\bin\BigDataProject\training_second.csv"

    #create a bloomfilter object trained with political words
    bf_politics=BloomFilter(politicalfeature_path,500000,7)
    sc_main= SparkContext(appName="bloom_politics")

    '''#working on feature data to generate bloom hash
    trainingdata=sc.textFile(featurelist_path).collect()

    for line in trainingdata:
        word_list=line.split(',')
        for words in word_list:
            if(words!=''):
                words=words.strip().encode('utf-8')
                #print(words)
                bf.add(words.lower())'''

    #Work on testdata to takeout relevant tweets based on bf object
    testdata=sc_main.textFile(testdata_path) \
                .map(lambda s:s.split(",", 4)[3]) \
                .map(lambda x:applyHash(bf_politics,x)) \
                .filter(lambda s: s != None) \
                .collect()    
    
    for tweet in testdata:
        print(str(tweet.encode('utf-8')))

    #.map(lambda s:s.split()) \

    	




