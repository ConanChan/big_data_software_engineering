#-*- coding:utf-8 -*-

from pyspark import SparkContext
from operator import add
import os

year = 2013

feature_file = './intermediate_data/'+str(year)+'_stduent_class_avgCookedStats'
grades_file = './intermediate_data/'+str(year)+'_student_class_grades'

def feature_file_mapper(line):
    items = line.split('\t')
    student_ID = items[0]
    class_ID = items[1]
    features = items[2:]
    return ((student_ID, class_ID), features)

def grades_file_mapper(line):
    rank_in_exam, student_ID, class_ID, grades = line.split('\t')
    return ((student_ID, class_ID), [rank_in_exam, grades])

if __name__ == '__main__':
    sc = SparkContext(appName="_feature_labels_combined")
    sc.setLogLevel("WARN")

    features = sc.textFile(feature_file).map(feature_file_mapper)

    grades = sc.textFile(grades_file).map(grades_file_mapper)

    k_student_class_v_0_HWstats_1_EXstats = features.join(grades).sortBy(lambda x: int(x[1][1][0]), ascending=False)

    collected = k_student_class_v_0_HWstats_1_EXstats.collect()
    
    f = open('./intermediate_data/'+str(year)+'_feature_labels_combined', 'w')

    for idx, l in enumerate(collected):
        s = []

        s.append('\t'.join(l[0]))
       
        s.append('\t'.join(l[1][0]))

        s.append('\t'.join(l[1][1]))
        s = '\t'.join(s)

        f.write(s+'\n')

        print "feature_amount:", len(l[1][0]) 
    