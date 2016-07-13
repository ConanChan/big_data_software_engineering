#-*- coding:utf-8 -*-

from pyspark import SparkContext
from datetime import datetime
import os
import shutil

year = 2014
shrunk_submission_folder = "./data_from_WXM/"+str(year)+"/shrunk_submission_v0"
assignment_folder = "./data_from_WXM/"+str(year)+"/assignments"
exercise_folder = "./data_from_WXM/"+str(year)+"/exercises"


def shrunk_submission_line_mapper(record):
    items = record.split('\t')
    submission_ID = items[0]
    student_ID = items[1]
    assignment_ID = items[2]
    submit_time = items[3]
    grades = items[4]

    return ((student_ID, assignment_ID), (submit_time, grades))

def features_extractor(record):
    ID_tuple = record[0]
    student_ID = ID_tuple[0]
    assignment_ID = ID_tuple[1]

    def list_mapper_func(elem):
        submit_time_str = elem[0]
        grades_str = elem[1]
        
        submit_time = datetime.strptime(submit_time_str, '%Y-%m-%d %H:%M:%S+00:00')
        try:
            grades = int(grades_str)
        except ValueError:
            grades = 0

        return (submit_time, grades)

        #2014-03-07 15:43:19+00:00
        #%Y-%m-%d %H:%M:%S+00:00

    features = []

    #sort by submit time
    submissions_info = sorted(map(list_mapper_func, record[1]), key=lambda x:x[0])

    best_grades_submission = max(submissions_info, key=lambda x:x[1])
    best_grades = best_grades_submission[1]

    for idx, si in enumerate(submissions_info):
        submit_t = si[0]
        submit_g = si[1]
        
        if submit_g >= 90 or submit_g == best_grades:
            submit_count = idx+1
            best_grades_submit_time = submit_t      
            break
    
    initial_submit_time = submissions_info[0][0]

    # estimate whether the student spent enough time in testing the codes
    #  before the first submission
    initial_submit_grades = submissions_info[0][1]


    #to estimate if this answer if 'original enough'-grow gradually not suddenly
    tmp_sum = 0
    for idx in range(submit_count):
        tmp_sum += submissions_info[idx][1]
    avg_grades_during_count_times = float(tmp_sum)/submit_count

    try:
        second_submit_grades = submissions_info[1][1]
    except IndexError:
        second_submit_grades = initial_submit_grades

    #finish with the first commit, it's likely that this is fake
    if initial_submit_grades == 100:
        return []
    #no compilation check before the first commit, which is very bad
    elif initial_submit_grades == 0:
        return []

    features = [best_grades, submit_count, initial_submit_time, \
                best_grades_submit_time, initial_submit_grades, avg_grades_during_count_times, \
                second_submit_grades]
    
    return [(assignment_ID, [student_ID]+features)]

def add_rank(func, list_of_student_features, d):
    amount = len(list_of_student_features)
    sorted_list_of_student_features = sorted(list_of_student_features, key=func, reverse=True)

    for idx, sp in enumerate(sorted_list_of_student_features):
        student_ID = sp[0]
        rank = 1-float(idx)/amount
        d[student_ID].append(rank)


def assignment_ranker(record):
    assignment_ID = record[0]
    list_of_student_features = record[1][0]
    assignment_info = record[1][1][0]
    exercise_type = record[1][1][1]

    if exercise_type != "PROBLEM_TYPE_PROGRAMMING":
        return []

    start_time, hard_due, soft_due = assignment_info


    student_IDs = [i[0] for i in list_of_student_features]
    d = dict((sid, []) for sid in student_IDs)

    funcs = [\
             #rank by finishing time, the earlier the higher
             lambda x: (x[1], hard_due-x[4], -x[2]),\
             #rank by initial_submit_grades, the higher the higher
             lambda x: x[5],\
             #rank by avg_grades_during_count_times, the higher the higher
             lambda x: x[6],\
             #rank by count, the less the higher
             lambda x: -x[2],\
             #rank by initial_submit_time, the earlier the higher
             lambda x: soft_due-x[3],\
             #rank by duration of debug time, the less the higher
             lambda x: -((x[4]-x[3]).total_seconds()),\
             #rank by second_submit_grades, the higher the higher
             lambda x: x[7],\
             ]
    for fun in funcs:
        add_rank(fun, list_of_student_features, d)
        result = []
        for sid in d:
            ranks = d[sid]
            result.append(([sid, assignment_ID], ranks))

    return result


def m4(record):
    assignment_ID = os.path.split(record[0])[1]

    lines = record[1].strip().split('\n')

    # ignore description
    
    start_time_str = lines[1]
    hard_due_str = lines[2]
    soft_due_str = lines[3]

    start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S+00:00')
    hard_due = datetime.strptime(hard_due_str, '%Y-%m-%d %H:%M:%S+00:00')
    soft_due = datetime.strptime(soft_due_str, '%Y-%m-%d %H:%M:%S+00:00')

    return (assignment_ID, (start_time, hard_due, soft_due))

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



if __name__ == '__main__':
    try:
        shutil.rmtree("./intermediate_data/"+str(year)+"_submission_group_and_analyzed_and_ranked")
    except Exception:
        pass

    sc = SparkContext("local[*]" ,"group_and_analyze_and_rank_submission")
    sc.setLogLevel("WARN")

    k_exerciseID_v_assignment = sc.wholeTextFiles(assignment_folder).map(purposed_assignment_file_mapper)

    k_exerciseID_v_exerciseType = sc.wholeTextFiles(exercise_folder).map(purposed_exercise_file_mapper)

    k_assignment_v_exerciseType = k_exerciseID_v_assignment.join(k_exerciseID_v_exerciseType).map(lambda x: (x[1][0], x[1][1]))
    
    assignment_asssignmentInfo_pair = sc.wholeTextFiles(assignment_folder).map(m4)

    k_assignment_v_0_asssignmentInfo_1_exerciseType = assignment_asssignmentInfo_pair.join(k_assignment_v_exerciseType)

    ranked = sc.textFile(shrunk_submission_folder+"/*/*").map(shrunk_submission_line_mapper).groupByKey().flatMap(features_extractor).groupByKey().join(k_assignment_v_0_asssignmentInfo_1_exerciseType).flatMap(assignment_ranker)
    print ranked.first()
    ranked.map(lambda x: '\t'.join( map(lambda y:str(y), x[0]+x[1]) )).saveAsTextFile("./intermediate_data/"+str(year)+"_submission_group_and_analyzed_and_ranked")
    
