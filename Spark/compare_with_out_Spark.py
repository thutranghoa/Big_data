# -*- coding: utf-8 -*-
"""
@author: TrinhThanh
"""

import datetime

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import DecisionTreeClassifier
import pandas as pd
import re
import warnings
import os
from sklearn.tree import DecisionTreeClassifier # Import Decision Tree Classifier
from sklearn.model_selection import train_test_split # Import train_test_split function
from sklearn import metrics #Import scikit-learn metrics module for accuracy calculation
import datetime
import numpy as np
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel



import pandas as pd
from sklearn.tree import DecisionTreeClassifier # Import Decision Tree Classifier
from sklearn.model_selection import train_test_split # Import train_test_split function
from sklearn.svm import SVC     ### SVM for classification
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.linear_model import LogisticRegression

from sklearn import metrics #Import scikit-learn metrics module for accuracy calculation
from sklearn.ensemble import RandomForestClassifier 
from sklearn.ensemble import BaggingClassifier
from sklearn.ensemble import StackingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.naive_bayes import MultinomialNB, BernoulliNB
from sklearn.svm import SVC
import math
from datetime import datetime


def run_without():
    print ("_____________RUN WITHOUT SPARK___________")

    df = pd.read_csv('data1M_10c.csv',header= None)    
    r,c = df.shape

    
    df.rename(columns = {0:'class'}, inplace = True)
    
    Y = df[['class']]  
    X = df.iloc[:,df.columns !='class']
   # df.columns.values[0] = 'class'   # thay ten bang class
  

  # split data
    X_Train, X_Test, Y_Train, Y_Test = train_test_split(X, Y,  stratify=Y, train_size=0.7, random_state=100000)
    
# decision tree        
    start = datetime.now()

    model = DecisionTreeClassifier()        
    model.fit(X_Train, Y_Train)            
    predictions = model.predict(X_Test)
    end = datetime.now() -start
    print ('DECISION TREE-------------- \n')
    print ("Run time : ", end)
    print("Accuracy:",metrics.accuracy_score(Y_Test, predictions))
    

# SVM
    start = datetime.now()

    svclassifier = SVC(kernel='rbf')
    svclassifier.fit(X_Train, Y_Train.values.ravel())
    y_pred = svclassifier.predict(X_Test)
    end = datetime.now() -start

    print ('\nSVM----------------- \n')
    print ("Run time : ", end)

    print("Accuracy:",metrics.accuracy_score(Y_Test, y_pred))
    

# naive classifier
    start = datetime.now()
    model_navie = GaussianNB()
  #  model_navie = MultinomialNB()        
    model_navie.fit(X_Train, Y_Train.values.ravel()) 
    prediction = model_navie.predict(X_Test) 
    end = datetime.now() -start

    print ('\nNaive Classifier-------------------\n')
    print ("Run time : ", end)
    print(" Accurancy: ", metrics.accuracy_score(Y_Test, prediction))
   

# bagging
   # base_cls = SVC()
   # base_cls = KNeighborsClassifier(n_neighbors=5)
   # base_clas = GaussianNB()  
    start = datetime.now()
    
    base_cls = DecisionTreeClassifier()
    
    #model = BaggingClassifier(base_estimator = base_cls, n_estimators = 100)
    model_bagging = BaggingClassifier(base_estimator = base_cls, n_estimators = 1)
    
    model_bagging.fit(X_Train, Y_Train.values.ravel())
    predictions = model_bagging.predict(X_Test)
    
    end = datetime.now() -start
            
    acc = metrics.accuracy_score(Y_Test, predictions)    

    print ('\nBagging------------------ \n')
    print ("Run time : ", end)
    print ("Accurancy: ",acc)     
    
    
#random forest
    start = datetime.now()

    rf_model = RandomForestClassifier(n_estimators=1, max_features= int(math.sqrt(c))+1)
    rf_model.fit(X_Train,Y_Train.values.ravel())
    pred_y = rf_model.predict(X_Test)
       
    end = datetime.now() -start
            
    accRF = metrics.accuracy_score(Y_Test, pred_y)

    print ('\nRandom Forest------------------ \n')
    print ("Run time : ", end)
    print ("Accuracy : ",accRF) 


def run_with_spark():
    
    print ("_____________RUN WITH SPARK___________")

    conf = SparkConf()
    conf.setMaster('local')
    conf.setAppName('spark-basic')
    sc = SparkContext(conf=conf)
    


   
    df= sc.textFile("data500K_10c.csv").map(lambda line: line.split(","))
                                            #label , features
    dataset  = df.map(lambda x: LabeledPoint(x[0], x[1:]))
    
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3])
    

    print (dataset.collect()[0])

    start = datetime.now()
    model = DecisionTree.trainClassifier(trainingData,  numClasses=20, categoricalFeaturesInfo={}, impurity='gini', maxDepth=8, maxBins=32)
    
# Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))    
   # print(predictions.collect())
    
    end = datetime.now() -start

    print ("Decision Tree -----------------\n")
    print ('Runtime : ', end)
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    
#    print(labelsAndPredictions.collect()[1])
    
    acc =0
    acc = (labelsAndPredictions.filter( lambda lp: lp[0] == lp[1]).count()) / float(testData.count())
    print('Test Accurancy = ' + str(acc))
    
    
    # Train a naive Bayes model.
    start = datetime.now()

    model = NaiveBayes.train(trainingData, 1.0)
    end = datetime.now() -start
    print ("\nNaive Bayes -----------------\n")
    print ('Runtime : ', end)
# Make prediction and test accuracy.
    predictionAndLabel = testData.map(lambda p: (model.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(lambda pl: pl[0] == pl[1]).count() / testData.count()
    print('Test Accuracy {}'.format(accuracy))


# Build the model
    start = datetime.now()

    model = SVMWithSGD.train(trainingData, iterations=10)
    end = datetime.now() -start

    print ("\nSVM -----------------\n")
    print ('Runtime : ', end)
    # Evaluating the model on training data
    labelsAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))
    acc = labelsAndPreds.filter(lambda lp: lp[0] == lp[1]).count() / float(testData.count())
    print("Test Accurancy = " + str(acc))    


# Build the model
    start = datetime.now()

    model = LogisticRegressionWithLBFGS.train(trainingData)
    end = datetime.now() -start
    print ("\nLogisticRegression -----------------\n")
    print ('Runtime : ', end)
    # Evaluating the model on training data
    labelsAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))
    acc = labelsAndPreds.filter(lambda lp: lp[0] == lp[1]).count() / float(testData.count())
    print("Test Accurancy = " + str(acc))
    
def main():
    run_without()
    
    # run_with_spark()
if __name__== '__main__':
    main()
    