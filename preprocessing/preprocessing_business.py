import json


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
            target_file.write(sep + escape_escaped_double_quotes(escape_double_backslash(convert_business_string_attributes_to_array(line.rstrip("\n")))))
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


def convert_business_string_attributes_to_array(user_string):
    object = json.loads(user_string)
    return json.dumps(convert_attributes_to_array(object, ["categories"]))

def convert_attributes_to_array(object, attributes):
    for attribute in attributes:
        attribute_string = object[attribute]
        if attribute_string is not None and attribute_string.strip() != "" and attribute_string.strip() != "None":
            attribute_array = attribute_string.split(",")
            for i in range(0, len(attribute_array)):
                attribute_array[i] = attribute_array[i].strip()
            object[attribute] = attribute_array
        elif attribute_string is not None and (attribute_string.strip() == "" or attribute_string.strip() == "None"):
            object[attribute] = []
    return object


def escape_double_backslash(text):
    """
    Replace '\\' with '\\\\' so after sql copy the originally intended single \ in text is preserverd.
    :param text:
    :return:
    """
    return text.replace('\\', '\\\\')


def escape_escaped_double_quotes(text):
    return text.replace('\\\"', '\\\\\"')


if __name__ == '__main__':
    src_file_name = "../yelp_data/business/yelp_academic_dataset_business.json"
    target_file_name = "../yelp_data/business/yelp_academic_dataset_business_array"
    process_file_line_by_line(src_file_name, target_file_name, ".json", 500000)