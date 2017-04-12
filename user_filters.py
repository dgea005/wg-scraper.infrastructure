# define what listings to email
from pandas import Timestamp

def get_preferences():
    preferences = dict()
    preferences['min_free_from'] = Timestamp('2017-04-25')
    preferences['max_free_from'] = Timestamp('2017-05-03')
    preferences['max_cost'] = 1000
    preferences['min_cost'] = 300
    preferences['min_length'] = 30
    return preferences
