
import operator


MAX_AMOUNT_OF_WORDS = 100
PENALIZATION_DISTANCE = 300


class WordProfile:
    """
    It is the profile of a set of given words. It stores the frequency and order of the most frequent
    MAX_AMOUNT_OF_WORDS words
    """

    def __init__(self, word_freqs):
        """
        :param word_freqs: dictionary of word frequencies
        """
        self.profile = {}
        sorted_words_by_freq = sorted(word_freqs.items(), key=operator.itemgetter(1), cmp=lambda x, y: cmp(y, x))
        self.create_profile_dict(sorted_words_by_freq)

    @classmethod
    def from_words(cls, words):
        """
        Auxiliar constructor from list of words. It calculates the freq dict.
        :param words: list of words
        :return: instance of WordProfile
        """
        freqs = cls.get_freq_dict(words)
        return cls(freqs)

    def create_profile_dict(self, sorted_freqs):
        """
        :param sorted_freqs: sorted list of (ngram, freq) in descending order
        :return: dictionary of {ngram: (freq, order)}
        """
        iterations = MAX_AMOUNT_OF_WORDS if len(sorted_freqs) >= MAX_AMOUNT_OF_WORDS else len(sorted_freqs)
        for i in xrange(iterations):
            word, freq = sorted_freqs[i]
            self.profile[word] = freq, i
        return self.profile

    @staticmethod
    def get_freq_dict(words):
        freqs = {}
        for w in words:
            if w in freqs:
                freqs[w] += 1
            else:
                freqs[w] = 1
        return freqs

    def get_size_of_intersection_and_complement(self, other):
        intersection_dimension = len(self.profile.viewkeys() & other.profile.viewkeys())
        not_intersected_dimension = len(self.profile) + len(other.profile) - 2 * intersection_dimension
        return intersection_dimension, not_intersected_dimension

    def get_profile_distance(self, other):
        intersection = self.profile.viewkeys() & other.profile.viewkeys()
        result = 0
        for word in intersection:
            position_in_self = self.profile[word][1]
            position_in_other = other.profile[word][1]
            result += abs(position_in_other - position_in_self)

        dimension_of_not_intersected = len(self.profile) + len(other.profile) - 2 * len(intersection)

        return result + dimension_of_not_intersected * PENALIZATION_DISTANCE


def get_users_similarity(users_data):
    """
    Calculates how similar is every user with each other according to the words they use.
    :param users_data: list of UserLogs
    :return: matrix (dict of dicts) with the a tuple as value that has
    (similarity, size_of_intersection, size_of_complement_of_intersection)
    """
    profiles = {}
    for user_log in users_data:
        word_count = user_log.get_word_count(words_amount=MAX_AMOUNT_OF_WORDS, clean=True)
        if len(word_count) >= MAX_AMOUNT_OF_WORDS:
            profiles[user_log.name] = WordProfile(word_count)

    users = profiles.keys()

    result = {user: {} for user in users}
    for i in xrange(len(users)):
        for j in xrange(i + 1, len(users)):
            user_a = users[i]
            user_b = users[j]
            distance = profiles[user_a].get_profile_distance(profiles[user_b])
            intersection, complement = profiles[user_a].get_size_of_intersection_and_complement(profiles[user_b])
            result[user_a][user_b] = distance, intersection, complement
            result[user_b][user_a] = distance, intersection, complement

    return result



