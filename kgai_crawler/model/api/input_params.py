"""
Houses the input params for the app
"""

from flask_restx import fields


class Inputparams(object):
    """constructor

    Arguments:
        object {[object]} -- [object of Flask Api]
    """

    def __init__(self, api):
        self.api = api

    def get_crawler_input(self):
        return self.api.model('News', {
            'topic': fields.String(required=True, description='The topic to search in google news')
        })
