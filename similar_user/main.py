import sys
import os
from random import randrange

def url_to_index_mapper1(line):
    pos1 = line.find("\"GET")
    pos2 = line.rfind("HTTP")
    if pos2 == -1:
        return []

    while pos1 != -1:
        URL = line[pos1+5:pos2-1]
        break

    if pos1 == -1:
        pos1 = line.find("\"POST")
        URL = line[pos1+6:pos2-1]

    data = line.strip().split(" ")
    user_index = data[0]
    if not URL in ref_dict.value:
        return []
    else:
        return [(user_index, ref_dict.value[URL])]

def hash_min_func(a, b, sign):
    h = []
    for x in sign:
        h.append(((a * x) + b) % 89997)
    return min(h)

def band_gen(le, num):
    for x in xrange(0, len(le), num):
        yield frozenset(le[x:x + num])


def hash_min_sign_mapper2(x):
    h =[]
    for u, v in zip(hash_a_ref.value, hash_a_ref.value):
        h.append(hash_min_func(a, b, sign))
    hash_min_row = h(x[1])

    # get the band(l,n)
    band_res = band_gen(hash_min_row, 20)
    # generate results
    res = []
    count = 0
    for i, b in enumerate(band_res):
        k = (i, hash(b))
        res.append((k, x[0]))
        count+=1
    return res


def user_generate(x):
    lis = list(x[1])
    length = len(l)
    res = []
    for i in range(0, length):
        for j in range(i+1, length):
            if lis[i] < lis[j] or lis[i] == lis[j]:
                res.append((lis[i], lis[j]))
            elif lis[i] > lis[j]:
                res.append((lis[j], lis[i]))

    return res

def mapper3(item):
    user_index1 = item[0]
    user_index2 = item[1][0]
    user_ids = x[1][1]

    return (user_index2, (user_index1, user_ids))
# final mapper
def mapper4(x):
    user_index2 = x[0]
    user_ids2= x[1][1]

    (user_index1, user_ids1) = x[1][0]
    
    pair = len(user_ids1.intersection(user_ids2))
    pair1 = len(user_ids1)
    pair2 = len(user_ids2)

    if pair == pair1 and pair == pair2:
        jac = -1
        return (jac, (user_index1, user_index2))

    else:
        jac = float(pair) / float(pair1+pair2-pair)
        return (jac, (user_index1, user_index2))


def main_func(sc, rdd, url_dict):
    # use MinHash method
    if __name__ == '__main__':
        global ref_dict
        global hash_a_ref
        global hash_b_ref
        # generate a broadcast variable
        ref_dict = sc.broadcast(url_dict)
        hash_a = []
        hash_b = []
        for x in xrange(0, 100):
            hash_a.append(randrange(sys.maxint))
            hash_b.append(randrange(sys.maxint))

        hash_a_ref = sc.broadcast(hash_a)
        hash_b_ref = sc.broadcast(hash_b)

        rdd2 = rdd.flatMap(url_to_index_mapper1).distinct()
        rdd3 = rdd2.groupByKey().filter(lambda x:len(x[1]) > 10)
        rdd4 = rdd3.flatMap(hash_min_sign_mapper2).groupByKey()
        rdd5 = rdd4.filter(lambda x: len(x[1]) > 1)
        rdd6 = rdd5.flatMap(user_generate).distinct()

        # key: j-s similarity ; value: user pair
        rdd7 = rdd3.mapValues(lambda x: set(x))
        rdd8 = rdd6.join(rdd7).map(mapper3)
        rdd9 = rdd8.join(rdd7).map(mapper4)
        # print the results
        for x in rdd9.top(1000):
            print "%.5f" % (x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][1])
