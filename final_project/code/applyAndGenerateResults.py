#-*- coding:utf-8 -*-

from pyspark import SparkContext
from operator import add
import os

year = 2014
feature_file = './intermediate_data/2014_stduent_class_avgCookedStats'
parameter_str = '16.032989 26.104059 -7.077603 4.129807 17.625877 9.693184 -0.755491 7.677877 -0.046742 1.875395 6.858896 12.214638 18.271104 23.868472 -9.209646 18.028822 -8.729579 -5.896739 0.263204 28.459901 0.243807 9.481197 2.796526 12.729573 8.858088 '

parameters = [float(i) for i in parameter_str.strip().split(' ')]
class_folder = "./data_from_WXM/"+str(year)+"/classes"

def feature_file_mapper(line):
    items = line.split('\t')
    stduent_ID = items[0]
    class_ID = items[1]
    features= [float(i) for i in items[2:]]

    predicted_grades = sum(map(lambda a,b:a*b, features, parameters))

    return (class_ID, (stduent_ID, predicted_grades))


def class_record_maker(rdd):
    class_ID = rdd[0]
    values = list(rdd[1])

    all_students = values[1]
    list_of_stduent_grades = list(values[0])

    now_stduent_IDs = [i[0] for i in list_of_stduent_grades]
    #run a check
    for sid in all_students:
        if sid not in now_stduent_IDs:
            print class_ID, sid, "no grades got. we will set it 0"
            list_of_stduent_grades.append([sid, 0])            

    sorted_list_of_stduent_grades = sorted(list_of_stduent_grades, key=lambda x:x[1], reverse=True)
    f = open('./intermediate_data/upload/prediction/'+class_ID, 'w')
    stduent_IDs = [i[0] for i in sorted_list_of_stduent_grades if i[0] != '2494']
    f.write('\n'.join(stduent_IDs))
    f.close()

def purposed_file_mapper_for_class(record):
    class_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    student_IDs = lines[3:]

    return (class_ID, student_IDs)


if __name__ == '__main__':
    sc = SparkContext("local[*]", appName="get_student_assignment_pair_of_homework")
    sc.setLogLevel("WARN")
    k_class_v_students = sc.wholeTextFiles(class_folder).map(purposed_file_mapper_for_class)

    k_class_v_student_predicted_grades = sc.textFile(feature_file).map(feature_file_mapper).groupByKey().join(k_class_v_students).foreach(class_record_maker)
