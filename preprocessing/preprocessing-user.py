import json


def read_file(filename):
    # open text file in read mode
    text_file = open(filename, "r", encoding='utf-8')
    # read whole file to a string
    data = text_file.read()
    # close file
    text_file.close()
    return data


def process_file_line_by_line(src_filename, target_filename, target_file_ext):
    with open(src_filename, 'r', encoding='utf-8') as src_file:
        counter = 0
        target_file_index = 0
        target_file = open(target_filename + "_" + str(target_file_index) + target_file_ext, 'a', encoding='utf-8')
        target_file.write('[')
        for line in src_file:
            counter += 1
            if (counter % 250000) == 1:
                sep = ""
            else:
                sep = ", "
            target_file.write(sep + convert_user_string_attributes_to_array(line.rstrip("\n")))
            if (counter % 250000) == 0:
                target_file.write("]")
                target_file.close()
                target_file_index += 1
                target_file = open(target_filename + "_" + str(target_file_index) + target_file_ext, 'a',
                                   encoding='utf-8')
                target_file.write('[')
        target_file.write(']')
        target_file.close()
        print(counter)


def convert_user_string_attributes_to_array(user_string):
    object = json.loads(user_string)
    return json.dumps(convert_attributes_to_array(object, ["friends", "elite"]))


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




if __name__ == '__main__':
    src_file_name = "yelp_data/user/yelp_academic_dataset_user.json"
    target_file_name = "yelp_data/user/yelp_academic_dataset_user_array"
    process_file_line_by_line(src_file_name, target_file_name, ".json")
