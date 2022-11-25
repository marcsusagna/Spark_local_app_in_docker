def handle_check_behavior(check_boolean, except_on_fail, check_name):
    """
    :param check_boolean: Boolean indicating if the check has passed. True means check passed and data quality
        is as expected
    :param except_on_fail: Boolean
    :param check_name:
    :return:
    """
    passing_message = "{} passed".format(check_name)
    warning_message = "{} did not pass, please review input dataset".format(check_name)
    if check_boolean:
        print(passing_message)
    else:
        if except_on_fail:
            raise Exception(warning_message)
        else:
            print(warning_message)