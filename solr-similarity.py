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
from solr import Solr
from flask import Flask
import pandas as pd
from vector import Vector
from sklearn.cluster import KMeans
import requests, json, os

solrURL = "http://localhost:8983/solr/electronicsimagecatdev"
solrInstance = Solr(solrURL)
app = Flask(__name__)


def computeJaccard():

    lukeURL = "http://{0}/admin/luke?numTerms=0&wt=json".format(solrURL.split("://")[-1].rstrip('/'))
    luke = requests.get(lukeURL)

    #if not luke: return "<h1>Check if Solr server is up</h1>"
    if luke.status_code == 200:
        luke = luke.json()
        union_feature_names = set(luke["fields"].keys())
        total_num_features = len(union_feature_names)

        docs = solrInstance.query_iterator(query="*:*", start=0)

        for doc in docs:
            overlap = set(doc.keys()) & set(union_feature_names)
            doc["jaccard_abs"] = float(len(overlap)) / total_num_features
            yield doc
    #perform atomic updates, sort=metadataScore in kwargs payload

def clusterScores():

    threshold = 0.01

    # sort json

    # generator iterate only once

    cluster_dict = {}
    i = 0
    for doc in computeJaccard():
        i += 1

# hit the REST endpoint to choose your distance metric
@app.route('/jaccardKey')
def jaccard():



    json_response = {}

    '''
    node = { "metadata": json.dumps(doc),
                     "name": doc['id'].split('/')[-1],
                     "path": doc[],
                     "score": os.environ["S"]
    }
    '''


@app.route('/kmeans/<int:kval>')
def sk_kmeans(kval):

    list_of_points = []
    docs = solrInstance.query_iterator(query="*:*", start=0)

    for doc in docs:
        list_of_points.append(Vector(doc['id'], doc))

    df = pd.DataFrame(list_of_points)
    df = df.fillna(0)

    kmeans = KMeans(n_clusters=kval,
                init='k-means++',
                max_iter=300,  # k-means convergence
                n_init=10,  # find global minima
                n_jobs=-2,  # parallelize
                )

    labels = kmeans.fit_predict(df)

    return labels




if __name__ == "__main__":
    app.run(debug=True)



    #keep tab on docID when iterating through Solr response