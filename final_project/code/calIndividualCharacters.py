#-*- coding:utf-8 -*-

from pyspark import SparkContext
from operator import add
import os

year = 2014

student_folder = "./data_from_WXM/"+str(year)+"/students"
class_folder = "./data_from_WXM/"+str(year)+"/classes"
exam_folder = "./data_from_WXM/"+str(year)+"/exams"
submission_folder = "./data_from_WXM/"+str(year)+"/submissions"
homework_folder = "./data_from_WXM/"+str(year)+"/homework"
assignment_folder = "./data_from_WXM/"+str(year)+"/assignments"
exercise_folder = "./data_from_WXM/"+str(year)+"/exercises"

def file_mapper_for_class(record):
    class_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    class_name = lines[0]
    homework_ID = lines[1]
    exam_IDs = lines[2].strip().split(' ')
    student_IDs = lines[3:]
    return (class_ID, (homework_ID, exam_IDs, student_IDs))
    
def file_mapper_for_exam(record):
    exam_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    exam_name = lines[0]
    assignment_IDs = lines[1:]
    return (exam_ID, (exam_name, assignment_IDs))

def file_mapper_for_submission(record):
    submission_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    student_ID = lines[0]
    assignment_ID = lines[1]
    submit_time = lines[2]
    # ignore line 3
    grades = int(lines[4])
    # ignore feedback

    return ((student_ID, assignment_ID) , (submit_time, grades))

def file_mapper_for_homework(record):
    homework_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')
    assignment_IDs = lines
    return (homework_ID, (assignment_IDs, len(assignment_IDs)))

def m1(record):
    class_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    homework_ID = lines[1]

    student_IDs = lines[3:]

    return (homework_ID, (student_IDs, class_ID))


def purposed_assignment_file_mapper(record):
    assignment_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    exercise_ID = lines[0]

    return (exercise_ID, assignment_ID)

def purposed_exercise_file_mapper(record):
    exercise_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    exercise_type = lines[1]

    return (exercise_ID, exercise_type)

def students_cross_join_assignments(record):
    student_IDs = record[1][0][0]
    class_ID = record[1][0][1]
    assignment_IDs = record[1][1][0]
    assignment_amount = record[1][1][1]
    result = []
    for s in student_IDs:
        for a in assignment_IDs:
            result.append(((s, a), (class_ID, assignment_amount)))
    return result


def cooked_submission_line_mapper_for_homework(line):
    items = line.split('\t')


    student_ID = items[0]
    assignment_ID = items[1]
    features = [float(i) for i in items[2:]]

    return ((student_ID, assignment_ID), features)

if __name__ == '__main__':
    sc = SparkContext("local[*]", appName="get_student_assignment_pair_of_homework")
    sc.setLogLevel("WARN")
    '''
    k_exerciseID_v_assignment = sc.wholeTextFiles(assignment_folder).map(purposed_assignment_file_mapper)

    k_exerciseID_v_exerciseType = sc.wholeTextFiles(exercise_folder).map(purposed_exercise_file_mapper)

    k_assignment_v_exerciseType = k_exerciseID_v_assignment.join(k_exerciseID_v_exerciseType).map(lambda x: (x[1][0], x[1][1]))
    '''

    k_homework_v_students_class_pair = sc.wholeTextFiles(class_folder).map(m1)

    k_homework_v_assignments_assignAmount_pair = sc.wholeTextFiles(homework_folder).map(file_mapper_for_homework)
    
    k_homework_v_0_students_class_1_assignments_assignAmount_pair = k_homework_v_students_class_pair.join(k_homework_v_assignments_assignAmount_pair)

    k_student_assignment_v_class_assignAmount = k_homework_v_0_students_class_1_assignments_assignAmount_pair.flatMap(students_cross_join_assignments)

    cookedStats = sc.textFile("./intermediate_data/"+str(year)+"_submission_group_and_analyzed_and_ranked/*").map(cooked_submission_line_mapper_for_homework)

    k_student_assignment_v_0_class_assignAmount_1_cookedStats = k_student_assignment_v_class_assignAmount.join(cookedStats)

    k_stduent_class_v_cookedStats_assignAmount = k_student_assignment_v_0_class_assignAmount_1_cookedStats.map(lambda x: ((x[0][0], x[1][0][0]), (x[1][1], x[1][0][1])))
    #print '-'*20
    #print k_stduent_class_v_cookedStats_assignAmount.first()

    k_stduent_class_v_sumupedCookedStats_assignAmount = k_stduent_class_v_cookedStats_assignAmount.reduceByKey(lambda a,b: (map(add, a[0], b[0]), a[1]))
    #print '-'*20
    #print k_stduent_class_v_sumupedCookedStats_assignAmount.first()

    k_stduent_class_v_avgCookedStats = k_stduent_class_v_sumupedCookedStats_assignAmount.map(lambda x: (x[0], map(lambda y: float(y)/x[1][1], x[1][0])))
    #print '-'*20
    #print k_stduent_class_v_avgCookedStats.first()
    
    collected_k_stduent_class_v_avgCookedStats = k_stduent_class_v_avgCookedStats.collect()

    f = open('./intermediate_data/'+str(year)+'_stduent_class_avgCookedStats', 'w')

    for l in collected_k_stduent_class_v_avgCookedStats:
        str_stats = map(lambda x:str(x), l[1])
        f.write('\t'.join(l[0])+'\t'+'\t'.join(str_stats)+'\n')
        
