#!/usr/bin/env python2.7
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
import requests

class Solr(object):
    """
    Solr client for querying docs
    """

    def __init__(self, solr_url):
        self.query_url = solr_url.rstrip('/') + '/' + 'select'


    def query_iterator(self, query='*:*', start=0, rows=20, limit=None, **kwargs):
        """
        Queries solr server and returns Solr response as dictionary
        returns None on failure, iterator of results on success
        """
        payload = {
            'q': query,
            'rows': rows,
            'wt': 'python'
        }

        if kwargs:
            for key in kwargs:
                payload[key] = kwargs.get(key)

        total = start + 1
        count = 0
        while start < total:
            if limit and count >= limit: #terminate
                break

            payload['start'] = start
            print 'start={0}   total={1}'.format(start, total)

            resp = requests.get(self.query_url, params=payload)
            if not resp:
                print "No response from Solr Server!"
                break

            if resp.status_code == 200:
                resp = eval(resp.text)
                total = resp['response']['numFound']
                for doc in resp['response']['docs']:
                    start += 1
                    count += 1
                    yield doc
            else:
                print resp
                print "Something went wrong when querying solr"
                print "Solr query params ", payload
                break











