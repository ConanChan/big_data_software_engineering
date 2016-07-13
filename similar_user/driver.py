import sys
from pyspark import SparkContext
 
if __name__ == "__main__":
    sc = SparkContext(appName="Co-occurrence")
    
    url_dict = {}      
    f = open("/projects/smart_programmer/smart_programmer/media/[2015] BDSE by Dr. Wang/object_mappings.sort", "r")
    for line in f:
        line = line.strip()
        idx= line.find(" ")
        url_id = int(line[:idx])
        url = line[idx+1:].rstrip()
        url_dict[url]=url_id
    f.close()
    rdd = sc.textFile("hdfs://eden/user/driver/smart_programmer/worldcup/w")
    main_func(sc, rdd, url_dict)
