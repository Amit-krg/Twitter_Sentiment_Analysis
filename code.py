

import numpy as np
import matplotlib.pyplot as plt
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark import SparkContext
import mmh3
import os
import re

Training_file1 = "/home/amit/Documents/BigDataProject/training_first.csv"
Training_file2 = "/home/amit/Documents/BigDataProject/training_second.csv"
feature_list = "/home/amit/Documents/BigDataProject/total_features.csv"
Training_file3 = "/home/amit/Documents/BigDataProject/tweets_train"

# This method calculates the TF-IDF of given tweets and return vector of 
# TF-IDF value
# @arg - RDD containing one tweet per line

sc = SparkContext(appName = "app")

size = 500000
hash_count = 7
bit_array = [None]*size
ops_type = 0
pos = []
neg = []

def add(data):
	for seed in range(hash_count):
		result = mmh3.hash(data, seed) % size
		bit_array[result] = 1
			
# This method takes each line from RDD and checks on each word of
# the tweet to find out if it belongs to sports or politics domain
def lookup(data):
	for seed in range(hash_count):
		result = mmh3.hash(data, seed) % size
		if bit_array[result] == 1:
			return 1
		else:
			return 0

#  This method takes RDD and returns RDD with only those tweets
#  that are believed to be of politics or sports domain
def filter_data( data):
	result = []
	temp = 0

	for word in data:
		temp = lookup(word)
		if(temp == 1):
			return data
	return None

def tf_idf(data):

	# This hashes each of the word / feature using MurmurHash 3 and generates
	# an index for each, then calculates TF for each index
	# TODO : Need to check best numFeatures
	tf = HashingTF(numFeatures=10000).transform(data.map(lambda x : x.split()))

	# TF-IDF is calculated to understand how important a word is to a particular
	# document

	idf = IDF().fit(tf)
	tfidf = idf.transform(tf)

	return tfidf

def SVM(label, tweet):
	# Get the labels of the training data
	# float(x[0][1]) is because after split we get '"4"' and x[0][1] is "4"
	labels = label.map(lambda x : float(x[1]))

	######### Calculating TF-IDF for the features ###########

	# calling TF-IDF method
	tf_idf_training = tf_idf(tweet)

	######## Final training data used by classifier #########

	training = labels.zip(tf_idf_training).map(lambda x: LabeledPoint(x[0], x[1]))

	model = SVMWithSGD.train(training, iterations=500)

	return model


def Naive_Bayes(label, tweet):
	# Get the labels of the training data
	# float(x[0][1]) is because after split we get '"4"' and x[0][1] is "4"

	labels = label.map(lambda x : float(x[1]))

	######### Calculating TF-IDF for the features ###########

	# calling TF-IDF method
	tf_idf_training = tf_idf(tweet)

	######## Final training data used by classifier #########

	training = labels.zip(tf_idf_training).map(lambda x: LabeledPoint(x[0], x[1]))

	model = NaiveBayes.train(training)

	return model

################# FROM A DIFF FILE - Need to figure out how to call the function here ##########


stopWords = []
STOP_WORDS = "/home/amit/Documents/BigDataProject/stopWords" 

#sc = SparkContext(appName = "Myapp")

def pre_process_helper(data):

	# converting to lower
	data = data.lower()

	# Removing URL's
	data = re.sub('((www\.[^\s]+)|(https?://[^\s]+))','',data)

	# Remove all words that start with one or more special characters
	data = re.sub(r'[@!#~^&$/_=+,.<>|{}?%*-]+([\w]+)', r'\1', data)

	# Removing all special charactes that appear after one or more letters
	# Trying not to remove any emoticon
	data = re.sub(r'([\w]+)[@!#~^&$/_=+,.<>|{})?%*-]+',r'\1',data)

	# removing multiple spaces if any
	data = re.sub('[\s]+', ' ', data)

	# Trimming the tweet for white spaces at begining and end
	data = data.strip()

	data = pre_process_features(data)

	return data;

def get_stop_words():
	fp = open(STOP_WORDS, 'r')
	line = fp.readline()

	while line:
		word = line.strip()
		stopWords.append(word)
		line = fp.readline()
	fp.close()

def pre_process_features(data):
	global ops_type

	features = []
	result = []

	features = data.split()
	# pattern to check if there are repeated characters 
	# before or after the word
	pattern = re.compile(r"(.)\1{1,}", re.DOTALL)

	for i in range(0,len(features)):
		features[i] = pattern.sub(r"\1", features[i])
		features[i] = re.sub(r'[^\x00-\x7f]',r'', features[i])
		if features[i] not in stopWords:
			result.append(features[i])
	

	if(ops_type == 1):
		result = filter_data(result)

	if (result != None):
		data = " ".join(result)
	else:
		data = None
	return data

# Function that performs all the pre processing
# @arg : RDD with tweets
def pre_process_start(data):
	get_stop_words();
	data = data.map(lambda x : pre_process_helper(x)).filter(lambda x : x != None)
	return data;

# This method is used to train the two classifiers and return the trained model
# @arg : type = "compare" if we need models to just compare b/w themselves
#		 type = "predict" if we need model to predict actual data

def train_sentiment_models(type, sc):
	# Loading the training set, taking only classified label and tweet
	data_1 = sc.textFile(Training_file1). \
			map(lambda x : x.split(",", 5)).map(lambda x : (x[0], x[5]))

	# Loading data from second training file
	data_2 = sc.textFile(Training_file2). \
			map(lambda x : x.split(",", 3)).map(lambda x : (x[1], x[3]))

	data = data_1.union(data_2)

	# If we are trying to compare the two classifiers, only then we 
	# need to use 70% as training data and 30% in testing data, else
	# full data is used to train the model
	if (type == "compare"):
		print "Comparing the classifiers"
		# Randomly using 70% of the data as training set and 30% as test set
		training, test = data.randomSplit([0.7, 0.3], seed = 0)
	elif (type == "predict"):
		print "Predicting the outcocome"
		training = data

	training_label = training.map(lambda x: x[0])
	training_tweet = training.map(lambda x: x[1])

	training_tweet = pre_process_start(training_tweet)

	model_naive = Naive_Bayes(training_label, training_tweet)
	model_svm = SVM(training_label, training_tweet)
	
	if (type == "compare"):
		return(model_naive, model_svm, training, test)
	else:
		return(model_naive, model_svm)

def train_domain_model(type, sc):
	# Loading the training set, taking only classified label and tweet
	data = sc.textFile(Training_file3) \
			.map(lambda x : x.split(",", 1)).map(lambda x : (x[0], x[1]))


	training = data
	training_label = training.map(lambda x: x[0])
	training_tweet = training.map(lambda x: x[1])

	training_tweet = pre_process_start(training_tweet)

	model_naive = Naive_Bayes(training_label, training_tweet)
	model_svm = SVM(training_label, training_tweet)
	
	return(model_naive, model_svm)


# This method is used to test the two models.
# @arg : model -> (naive_model, svm_model, training data, test_data)

def test_models(model):
	model_naive = model[0]
	model_svm = model[1]
	training_data = model[2]
	test_data = model[3]

	# Picking up just the tweet from (label, tweet)
	test_tweet = test_data.map(lambda x : x[1])

	# Pre-process the test data as well
	test_tweet = pre_process_start(test_tweet)

	test_actual_label = test_data.map(lambda x : float(x[0][1]))

	tf_idf_test = tf_idf(test_tweet)

	try:
		predicted_result_naive = model_naive.predict(tf_idf_test)
		predicted_result_SVM = model_svm.predict(tf_idf_test)
	except EOFError:
		print "Caught EOF error"

	result_naive = test_actual_label.zip(predicted_result_naive)
	result_svm = test_actual_label.zip(predicted_result_SVM)

	# return result of naive bayes prediction, svm prediction, 
	# training data on which it was predicted and test data used
	# to compare
	return (result_naive, result_svm, training_data, test_data)

# This method is used to classify new tweet
def predict_results(model, data, type, classifier):

	model_naive = model[0]
	model_svm = model[1]


	# Pre-process the test data as well
	data = pre_process_start(data)

	tf_idf_test = tf_idf(data)

	# Predicting the result of the data 
	try:
		if(classifier == "both" or classifier == "naive" ):
			predicted_result_naive = model_naive.predict(tf_idf_test)
			predicted_result_naive = predicted_result_naive.map(lambda x : int(x))
		if(classifier == "both" or classifier == "svm" ):
			predicted_result_SVM = model_svm.predict(tf_idf_test)
	except EOFError:
		print "Caught EOF error"
		
	ret_value = ()

	#Classifying tweets in domains (sports and politics)
	if (type == 'domain'):
		naive_zip = predicted_result_naive.zip(data)
		SVM_zip = predicted_result_SVM.zip(data)
	
		naive_sports = naive_zip.filter(lambda x: x[0] == 1).map(lambda x: x[1])
		naive_politics = naive_zip.filter(lambda x: x[0] == 0).map(lambda x: x[1])

		SVM_sports = SVM_zip.filter(lambda x: x[0] == 1).map(lambda x: x[1])
		SVM_politics = SVM_zip.filter(lambda x: x[0] == 0).map(lambda x: x[1])
		
		ret_value = (naive_sports,naive_politics,SVM_sports,SVM_politics)
		
		print "Total Number of tweets classified: ", predicted_result_naive.count()
		print "Domain Classification statistics : "
		print ""
		print "Naive Bayes Classifier	: "
		print ""
		print "Number of sports tweets		: ", naive_sports.count()
		print "Number of politics tweets	: ", naive_politics.count()		
		print ""
		print "SVM Classifier 		: "
		print ""
		print "Number of sports tweets		: ", SVM_sports.count()
		print "Number of politics tweets	: ", SVM_politics.count()
		print ""
	
	#Classifying domain tweets' sentiment (positive and negative)
	if (type == 'sentiment'):
		
		if (classifier == "naive"):
			test_data_naive = predicted_result_naive.map(lambda x : (float(x), 1)).reduceByKey(lambda a, b: a + b).collect()
			print "Total Number of tweets classified: ", predicted_result_naive.count()
			print "Sentiment Classification statistics : "
			print ""
			print "Total number of Positive tweets	: ", test_data_naive[0][1]
			print "Total number of Negative tweets	: ", test_data_naive[1][1]
		elif (classifier == "svm"):
			test_data_svm = predicted_result_SVM.map(lambda x : (float(x), 1)).reduceByKey(lambda a, b: a + b).collect()
			print "Total Number of tweets classified: ", predicted_result_SVM.count()
			print "Sentiment Classification statistics : "
			print ""
			print "Total number of Positive tweets	: ", test_data_svm[0][1]
			print "Total number of Negative tweets	: ", test_data_svm[1][1]

	return ret_value

# This method is used to compare the results of 2 models
# @ arg : result from test_model method

def compare_results(result):
	# result of naive and svm contains (actual data, predicted data)
	result_naive = result[0];
	result_svm = result[1];
	# Training contains ("0",Tweet)
	training_data = result[2]
	test_data = result[3]
	test_data_count = test_data.count()


	actual_data = test_data.map(lambda x : (float(x[0][1]), 1)).reduceByKey(lambda a, b: a + b).collect()
	test_data_naive = result_naive.map(lambda x : (float(x[1]), 1)).reduceByKey(lambda a, b: a + b).collect()
	test_data_svm = result_svm.map(lambda x : (float(x[1]), 1)).reduceByKey(lambda a, b: a + b).collect()

	print ""
	print "Number of training tweets			: ",training_data.count()
	print "Number of test tweets				: ",test_data_count
	print ""
	print "							Negative sentiment		Positive sentiment"
	print "							------------------		------------------"
	print "Actual data				: 		",actual_data[0][1],"					",actual_data[1][1]
	print "Naive predicted data	: 		",test_data_naive[0][1],"					",test_data_naive[1][1]
	print "SVM predicted data		: 		",test_data_svm[0][1],"					",test_data_svm[1][1]

	global pos 
	pos = [actual_data[1][1],test_data_naive[1][1],test_data_svm[1][1]]
	global neg 
	neg = [actual_data[0][1],test_data_naive[0][1],test_data_svm[0][1]]

	accuracy_naive = 1.0 * result_naive.filter(lambda x : x[0] == x[1]).count() / test_data_count
	accuracy_svm = 1.0 * result_svm.filter(lambda x : x[0] == x[1]).count() / test_data_count
	print ""
	print "Naive Bayes classifier accuracy		: ",accuracy_naive * 100, "%"
	print "SVM classifier accuracy				: ",accuracy_svm * 100, "%"



################################################################################################

# This is to compare the two classifiers
model = train_sentiment_models("compare", sc)
result = test_models(model)
compare_results(result)

# Reads the feature list for BloomFilter
trainingdata = sc.textFile(feature_list);
size = size
hash_count = hash_count
bit_array = [None]*size
for i in range(size):
	bit_array[i] = 0

for word in trainingdata.collect():
	if(word!=''):
		word = word.replace(" ",'')
		word = word.strip().encode('utf-8')
		add(word.lower())

# This is to predict new tweets
data = sc.textFile("/home/amit/Documents/BigDataProject/tweets_test");
model_domain = train_domain_model("predict",sc)
model_sentiment = train_sentiment_models("predict", sc)

ops_type = 1

result = predict_results(model_domain, data, "domain", "both")

print "Sentiment classification of sports tweets by NaiveBayes classifier : "
print ""
result_naive_sports = predict_results(model_sentiment, result[0], "sentiment", "naive")

print "Sentiment classification of politics tweets by NaiveBayes classifier : "
print ""
result_naive_politics = predict_results(model_sentiment, result[1], "sentiment", "naive")

print "Sentiment classification of sports tweets by SVM classifier : "
print ""
result_SVM_sports = predict_results(model_sentiment, result[2], "sentiment", "svm")

print "Sentiment classification of politics tweets by SVM classifier : "
print ""
result_SVM_politics = predict_results(model_sentiment, result[3], "sentiment", "svm")


#Plots and displays a bar graph comapring SVM,Naive Bayes performance against actual data
def barPlot(pos,neg):
    N=3
    #positive table[actual,naive,svm]
    objects=('Actual','Naive','SVM')
    ind=np.arange(3)
    width=0.20
    opacity=0.7
    plt.bar(ind, pos,width,alpha=opacity,color='c',label='+ve %',yerr=0.01)
    plt.bar(ind + width, neg,width,alpha=opacity,color='g',label='-ve %',yerr=0)
    #plt.grid(True)
    plt.ylabel('Accuracy')
    plt.title('Comparison of Classifiers')
    plt.xticks(ind+width,objects)
    #ax.set_xticks(ind + width)
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), fancybox=True, shadow=True)
    #plt.legend()
    plt.show()
barPlot(pos,neg)
