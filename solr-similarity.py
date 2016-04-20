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
import requests, json, os, operator

solrURL = "http://localhost:8983/solr/electronicsimagecatdev"
# DHS
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

        docs = solrInstance.query_iterator(query="*:*", start=0, limit=100)

        bufferDocs = []
        for doc in docs:
            overlap = set(doc.keys()) & set(union_feature_names)
            # not performing atomic update to Solr index, just computing scores & clustering
            doc["jaccard_abs"] = float(len(overlap)) / total_num_features

            bufferDocs.append(doc) #yield doc

        bufferDocs.sort(key=operator.itemgetter('jaccard_abs'), reverse=True)
        return bufferDocs

    # perform atomic updates,
    # query Solr with sort="metadataScore" in kwargs payload


# hit the REST endpoint to choose your distance metric
@app.route('/jaccardKey')
def jaccard():

    threshold = 0.01
    docs = computeJaccard()
    prior = docs[0]["jaccard_abs"]

    json_data = {"name": "clusters"}

    cluster0 = { "name": "cluster0",
                 "children": [docs[0]]
    }

    clusters = [cluster0]
    clusterCount = 0

    for i in range(1, len(docs)):

        node = { "metadata": json.dumps(docs[i]),
                 "name": docs[i]['id'].split('/')[-1],
                 "path": os.environ["IMAGE_MOUNT"] + docs[i]['id'].split('/')[-1],
                 "score": docs[i]["jaccard_abs"]
        }

        diff = prior - docs[i]["jaccard_abs"]

        if diff < threshold:
            clusters[clusterCount]["children"].append(node)
        else:
            clusterCount += 1
            newCluster = { "name": "cluster"+str(clusterCount),
                           "children": [docs[i]]
            }
            clusters.append(newCluster)

        prior = docs[i]["jaccard_abs"]

    json_data["children"] = clusters

    return json.dumps(json_data)



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

    return str(labels)




if __name__ == "__main__":
    app.run(debug=True)



    #keep tab on docID when iterating through Solr response