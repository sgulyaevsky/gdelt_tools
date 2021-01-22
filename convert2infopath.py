import argparse
import glob
import os
from operator import itemgetter


from pyspark import SparkContext, SparkConf

from gdelt_tools.parse import file_to_lines, timedate_to_timestamp


def line_to_source_id(line):
    values = line.split('\t')
    source_id = values[4]
    return source_id


def extract_source_id(filename):
    return [line_to_source_id(line) for line in file_to_lines(filename)]


def line_to_mention_info(line, name_to_id):
    values = line.split('\t')
    event_id = values[0]
    mention_timedate = values[2]
    source_id = values[4]
    return int(event_id), (timedate_to_timestamp(mention_timedate), name_to_id[source_id])


def extract_mention_info(name_to_id):
    def f(filename):
        return [line_to_mention_info(line, name_to_id) for line in file_to_lines(filename)]
    return f


def merge_cascade_info(values):
        event_id, mentions = values
        cascade_list = list(mentions)
        if len(cascade_list) < 2:
            return ''
        cascade_list.sort(key=itemgetter(0))
        cascade_data = ','.join([str(id)+','+str(timestamp) for timestamp, id in cascade_list])
        return str(event_id) + ';' + cascade_data


def convert(data_dir, output_dir, cascades_temp=None):
    # list all input files
    mentions_files = glob.glob(data_dir + '/*.mentions.CSV.zip')

    conf = SparkConf().setAppName('gdelt_to_infopath').setMaster('local[2]')
    sc = SparkContext(conf=conf)

    # read mentions file list
    mentions_files_data = sc.parallelize(mentions_files)

    # prepare source name to unique id map
    unique_source_names = mentions_files_data.flatMap(extract_source_id).distinct().collect()
    name_to_id = dict([(name, i) for i, name in enumerate(unique_source_names)])

    # build cascades
    mentions_info = mentions_files_data.flatMap(extract_mention_info(name_to_id))
    cascades = mentions_info.groupByKey().map(merge_cascade_info).filter(lambda x: len(x) > 1)
    cascades_temp = os.path.join(output_dir, 'cascades_temp.txt')
    cascades.saveAsTextFile(cascades_temp)

    sc.stop()

    # format cascades as in https://github.com/snap-stanford/snap/blob/master/examples/infopath/ReadMe.txt
    output_filename = os.path.join(output_dir, 'cascades.txt')
    cascades_files = glob.glob(cascades_temp + '/part-000*')

    with open(output_filename, 'w') as outfile:
        for name, id in name_to_id.items():
            outfile.write('{0},{1}\n'.format(id, name))

        outfile.write('\n')

        for filename in cascades_files:
            with open(filename) as infile:
                for line in infile:
                    outfile.write(line)

    print('Output cascades file:', os.path.abspath(output_filename))


argparser = argparse.ArgumentParser()
argparser.add_argument("--data_dir", type=str, required=True)
argparser.add_argument("--work_dir", type=str, required=True)
args = argparser.parse_args()

convert(args.data_dir, args.work_dir)