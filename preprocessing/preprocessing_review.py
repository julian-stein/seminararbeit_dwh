import json


def read_file(filename):
    # open text file in read mode
    text_file = open(filename, "r", encoding='utf-8')
    # read whole file to a string
    data = text_file.read()
    # close file
    text_file.close()
    return data


def process_file_line_by_line(src_filename, target_filename, target_file_ext, records_per_file):
    with open(src_filename, 'r', encoding='utf-8') as src_file:
        counter = 0
        target_file_index = 0
        target_file = open(target_filename + "_" + str(target_file_index) + target_file_ext, 'a', encoding='utf-8')
        target_file.write('[')
        for line in src_file:
            counter += 1
            if (counter % records_per_file) == 1:
                sep = ""
            else:
                sep = ", "
            target_file.write(sep + escape_escaped_double_quotes(escape_double_backslash(line.rstrip("\n"))))
            if (counter % records_per_file) == 0:
                target_file.write("]")
                target_file.close()
                target_file_index += 1
                target_file = open(target_filename + "_" + str(target_file_index) + target_file_ext, 'a',
                                   encoding='utf-8')
                target_file.write('[')
        target_file.write(']')
        target_file.close()
        print(counter)


def escape_double_backslash(text):
    """
    Replace '\\' with '\\\\' so after sql copy the original intenden single \ in text is preserverd.
    :param text:
    :return:
    """
    return text.replace('\\', '\\\\')


def escape_escaped_double_quotes(text):
    return text.replace('\\\"', '\\\\\"')

def process_file_line_by_line_no_array(src_filename, target_filename, target_file_ext, records_per_file):
    with open(src_filename, 'r', encoding='utf-8') as src_file:
        counter = 0
        target_file_index = 0
        target_file = open(target_filename + "_" + str(target_file_index) + target_file_ext, 'a', encoding='utf-8')
        for line in src_file:
            counter += 1
            if (counter % records_per_file) == 1:
                sep = ""
            else:
                sep = ",\n"
            target_file.write(sep + line.rstrip("\n") )
            if (counter % records_per_file) == 0:
                target_file.close()
                exit()
                target_file_index += 1
                target_file = open(target_filename + "_" + str(target_file_index) + target_file_ext, 'a',
                                   encoding='utf-8')
        target_file.close()
        print(counter)


if __name__ == '__main__':
    no_records = 500000
    src_file_name = "yelp_data/review/yelp_academic_dataset_review.json"
    target_file_name = "yelp_data/review/review_json_files/yelp_academic_dataset_review_array"
    process_file_line_by_line(src_file_name, target_file_name, ".json", no_records)
    #process_file_line_by_line_no_array(src_file_name, target_file_name, ".json", no_records)
