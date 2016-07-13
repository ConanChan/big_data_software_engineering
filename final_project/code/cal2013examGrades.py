#-*- coding:utf-8 -*-

from pyspark import SparkContext
from operator import add
import os

year = 2013

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

def file_mapper_for_assignment(record):
	assignment_ID = os.path.split(record[0])[1]

	lines = record[1].strip().split('\n')

	
	exercise_ID = line[0]
	start_time = line[1]
	hard_due = line[2]
	soft_due = line[3]

	return (assignment_ID, (exercise_ID, start_time, hard_due, soft_due))

def m1(record):
	class_ID = os.path.split(record[0])[1]


	lines = record[1].strip().split('\n')

	exam_IDs = lines[2].strip().split(' ')
	student_IDs = lines[3:]
	
	result = []

	for e in exam_IDs:
		result.append((e, (student_IDs, class_ID)))
	return result

def m2(record):
	exam_ID = os.path.split(record[0])[1]
	lines = record[1].strip().split('\n')

	exam_name = lines[0]
	
	if u'simualtion' in exam_name or u'模拟' in exam_name:
		assignment_IDs = []
	else:
		assignment_IDs = lines[1:]


	return (exam_ID, assignment_IDs)

def m3(record):
	result = []
	student_IDs = record[1][0][0]
	class_ID = record[1][0][1]
	assignment_IDs = record[1][1]

	for s in student_IDs:
		for a in assignment_IDs:
			result.append(((s, a), class_ID))

	return result

def cooked_submission_line_mapper(line):
	student_ID, assignment_ID, rank, max_grades, submit_count, passion, debug_talent = line.split('\t')
	rank = float(rank)
	max_grades = int(max_grades)
	submit_count = int(submit_count)
	passion = float(passion)
	debug_talent = float(passion)

	return ((student_ID, assignment_ID), max_grades)


def add_up_grades(a, b):
	class_ID = a[0]
	return (class_ID, a[1]+b[1])

def set_key_to_aid(record):
	student_ID, assignment_ID = record[0]
	class_ID = record[1][0]
	max_grades = record[1][1]

	return (assignment_ID, (student_ID, class_ID, max_grades))

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

def weight_grades_mapper(record):
	assignment_ID = record[0]
	student_ID, class_ID, grades = record[1][0]
	exercise_type = record[1][1]

	if exercise_type == "PROBLEM_TYPE_PROGRAMMING":
		weight = 0.1
	elif exercise_type == "PROBLEM_TYPE_SINGLE_CHOICE":
		weight = 0.01
	else:
		weight = 10000

	return ((student_ID, class_ID), grades*weight)



if __name__ == '__main__':
    sc = SparkContext("local[*]", appName="cal_exam_grades")
    sc.setLogLevel("WARN")

    #classes = sc.wholeTextFiles(class_folder).map(file_mapper_for_class)
    #exams = sc.wholeTextFiles(exam_folder).map(file_mapper_for_exam)
    
    exam_students_class_pair = sc.wholeTextFiles(class_folder).flatMap(m1)    

    exam_assignments_pair = sc.wholeTextFiles(exam_folder).map(m2)

    exam_students_class_assignments_pair = exam_students_class_pair.join(exam_assignments_pair)

    student_assignment_class = exam_students_class_assignments_pair.flatMap(m3)

    cooked_grades = sc.textFile("./intermediate_data/"+str(year)+"_submission_group_and_analyzed_and_ranked/*").map(cooked_submission_line_mapper)

    k_assignment_v_student_class_grades = student_assignment_class.join(cooked_grades).map(set_key_to_aid)


    k_exerciseID_v_assignment = sc.wholeTextFiles(assignment_folder).map(purposed_assignment_file_mapper)

    k_exerciseID_v_exerciseType = sc.wholeTextFiles(exercise_folder).map(purposed_exercise_file_mapper)

    k_assignment_v_exerciseType = k_exerciseID_v_assignment.join(k_exerciseID_v_exerciseType).map(lambda x: (x[1][0], x[1][1]))


    k_assignment_v_0_student_class_grades_1_exerciseType = k_assignment_v_student_class_grades.join(k_assignment_v_exerciseType)
    														
    k_student_class_v_weightedGrades = k_assignment_v_0_student_class_grades_1_exerciseType.map(weight_grades_mapper)

    student_class_sumuped_grades = k_student_class_v_weightedGrades.reduceByKey(add).sortBy(lambda x:x[1], ascending=False).map(lambda x:'\t'.join([x[0][0], x[0][1], str(x[1])]))\
    				
    collected_student_class_sumuped_grades = student_class_sumuped_grades.collect()

    f = open('./intermediate_data/'+str(year)+'_student_class_grades', 'w')

    for idx, l in enumerate(collected_student_class_sumuped_grades):
    	f.write(str(idx)+'\t'+l+'\n')

