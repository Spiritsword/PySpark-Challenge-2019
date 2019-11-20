import re
from datetime import datetime as dt


def count_elements_in_dataset(dataset):
    """
    Given a dataset loaded on Spark, return the
    number of elements.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: number of elements in the RDD
    """
    return dataset.count()


def get_first_element(dataset):
    """
    Given a dataset loaded on Spark, return the
    first element
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: the first element of the RDD
    """
    return dataset.first()


def get_all_attributes(dataset):
    """
    Each element is a dictionary of attributes and their values for a post.
    Can you find the set of all attributes used throughout the RDD?
    The function dictionary.keys() gives you the list of attributes of a dictionary.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: all unique attributes collected in a list
    """
    element_attributes_list = dataset.map(lambda x: set(list(x.keys())))
    all_attributes = element_attributes_list.reduce(lambda set1, set2: set1.union(set2))
    return list(all_attributes)


def get_elements_w_same_attributes(dataset):
    """
    We see that there are more attributes than just the one used in the first element.
    This function should return all elements that have the same attributes
    as the first element.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD containing only elements with same attributes as the
    first element
    """
    def set_equal(a, b):
        return (a <= b) and (a >= b)

    def attribute_set(dataset):
        return set(list(dataset.keys()))

    def filter_by_attribute_set(dataset, att_set):
        return dataset.filter(lambda x: set_equal(attribute_set(x), att_set))

    return filter_by_attribute_set(dataset, attribute_set(dataset.first()))


def extract_time(timestamp):
    return dt.utcfromtimestamp(timestamp)


def extract_timestamps(dataset):
    return dataset.map(lambda x: x.get('created_at_i', None))


def get_min_max_timestamps(dataset):
    """
    Find the minimum and maximum timestamp in the dataset
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: min and max timestamp in a tuple object
    :rtype: tuple
    """
    timestamps = extract_timestamps(dataset)
    min_timestamp = timestamps.reduce(lambda x, y: min(x, y))
    max_timestamp = timestamps.reduce(lambda x, y: max(x, y))
    return (min_timestamp, max_timestamp)


def get_bucket(rec, min_timestamp, max_timestamp):
    interval = (max_timestamp - min_timestamp + 1) / 200.0
    return int((rec['created_at_i'] - min_timestamp)/interval)


def get_number_of_posts_per_bucket(dataset):
    """
    NB I TWEAKED THE SPEC FOR THIS TO MATCH JUPYTER NOTEBOOK SPEC MORE CLOSELY
    (USING TIMESTAMPS RATHER THAN DATETIME FORMAT)
    AND DERIVING THE MAX AND MIN TIMESTAMPS FROM DATASET WITHIN THE FUNCTION.
    I HOPE THAT'S OK!

New spec:

    Using the `get_bucket` function defined in the notebook (redefine it in this file), this function should return a
    new RDD that contains the number of elements that fall within each bucket of the dataset.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with number of elements per bucket (max and min times and buckets calculated within function)

Old spec (IGNORE):

    Using the `get_bucket` function defined in the notebook (redefine it in this file), this function should return a
    new RDD that contains the number of elements that fall within each bucket.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :param min_time: Minimum time to consider for buckets (datetime format)
    :param max_time: Maximum time to consider for buckets (datetime format)
    :return: an RDD with number of elements per bucket
    """

    result = get_min_max_timestamps(dataset)
    min_timestamp = result[0]
    max_timestamp = result[1]
    return dataset.map(lambda x: (get_bucket(x, min_timestamp, max_timestamp), 1)).reduceByKey(lambda a, b: a + b)


def get_hour(rec):
    time = dt.utcfromtimestamp(rec['created_at_i'])
    return time.hour


def get_number_of_posts_per_hour(dataset):
    """
    Using the `get_hour` function defined in the notebook (redefine it in this file), this function should return a
    new RDD that contains the number of elements per hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with number of elements per hour
    """
    return dataset.map(lambda x: (get_hour(x), 1)).reduceByKey(lambda a, b: a + b)


def get_score_per_hour(dataset):
    """
    The number of points scored by a post is under the attribute `points`.
    Use it to compute the average score received by submissions for each hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with average score per hour
    """
    def get_score_per_hour_accumulator(dataset):
        points_map = dataset.map(lambda x: (get_hour(x), (x['points'], 1)))
        return points_map.reduceByKey(lambda a, b: ((a[0] + b[0]), (a[1] + b[1])))

    return get_score_per_hour_accumulator(dataset).map(lambda x: (x[0], (x[1][0]/x[1][1])))


def get_words(line):
    return re.compile('\w+').findall(line)


def get_proportion_of_scores(dataset):
    """
    It may be more useful to look at sucessful posts that get over 200 points.
    Find the proportion of posts that get above 200 points per hour.
    This will be the number of posts with points > 200 divided by the total number of posts at this hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of scores over 200 per hour
    """

    def get_successes_per_hour_accumulator(dataset):
        successes_per_hour_map = dataset.map(lambda x: (get_hour(x), (int(x['points'] > 200), 1)))
        return successes_per_hour_map.reduceByKey(lambda a, b: ((a[0] + b[0]), (a[1] + b[1])))

    return get_successes_per_hour_accumulator(dataset).map(lambda x: (x[0], (x[1][0]/x[1][1])))


def get_successes_per_title_length_accumulator(dataset):
    filtered_dataset = dataset.map(lambda x: (len(get_words(x.get('title', ""))), (int(x['points'] > 200), 1)))
    return filtered_dataset.reduceByKey(lambda a, b: ((a[0] + b[0]), (a[1] + b[1])))


def get_proportion_of_success(dataset):
    """
    Using the `get_words` function defined in the notebook to count the
    number of words in the title of each post, look at the proportion
    of successful posts for each title length.

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of successful post per title length
    """
    return get_successes_per_title_length_accumulator(dataset).map(lambda x: (x[0], (x[1][0]/x[1][1])))


def get_title_length_distribution(dataset):
    """
    Count for each title length the number of submissions with that length.

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the number of submissions per title length
    """
    return get_successes_per_title_length_accumulator(dataset).map(lambda x: (x[0], x[1][1]))
