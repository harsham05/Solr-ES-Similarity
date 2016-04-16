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

from elasticsearch import Elasticsearch
from tika import parser
from mysolr import Solr
import argparse, os, requests

def filterFiles(inputDir, acceptTypes):
    filename_list = []

    for root, dirnames, files in os.walk(inputDir):
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        for filename in files:
            if not filename.startswith('.'):
                filename_list.append(os.path.join(root, filename))

    filename_list = (filename for filename in filename_list if "metadata" in parser.from_file(filename))
    if acceptTypes:
        filename_list = (filename for filename in filename_list if str(parser.from_file(filename)['metadata']['Content-Type'].encode('utf-8')).split('/')[-1] in acceptTypes)
    else:
        print "Accepting all MIME Types....."

    return filename_list


def stringify(attribute_value):
    if isinstance(attribute_value, list):
        return str((", ".join(attribute_value)).encode('utf-8').strip())
    else:
        return str(attribute_value.encode('utf-8').strip())


def lazySolr(inputDir, accept):

    for doc in filterFiles(inputDir, accept):
        parsed = parser.from_file(doc)

        document = { "id": "file:" + os.path.abspath(inputDir) + "/" + str(parsed["metadata"].pop(u"resourceName", None).encode("utf-8")),
                     "content": parsed["content"]
        }

        for key in parsed["metadata"]:
            document[key] = stringify(parsed["metadata"][key])

        yield document


def solrIngest(URL, inputDir, accept):

    session = requests.Session()      #session.auth = (os.environ["SOLR_SIM_USER"], os.environ["SOLR_SIM_PASS"])
    solr = Solr(URL, make_request=session, version=4)

    documents = list(lazySolr(inputDir, accept))

    #print documents

    x = solr.update(documents, commit=True)

    if x.raw_content['responseHeader']['status'] != 0:
        print "Solr Commit Failed !!!! Error Status code: ", x.raw_content['responseHeader']['status']
    else:
        print "Awesome!! Solr Commit was a Success"



def ingestES(inputDir, accept):

    #intersect_features = set()
    # has to be done thru querying ES efficiently filter queries?

    es = Elasticsearch()
    for doc in filterFiles(inputDir, accept):

        parsed = parser.from_file(doc)
        parsed["metadata"].pop(u"resourceName", None)

        resp = es.index(index="example1", doc_type="polar", id="file:"+doc, body=parsed)

        if resp["created"]:
            continue
        else:
            print "Lost docID: ", doc


if __name__ == "__main__":

    argParser = argparse.ArgumentParser('Ingest Documents into Solr 4.10.4 or ES 2.3.1')
    argParser.add_argument('--URL', required=True, help='Solr or Elastic Document Store URL ## http://localhost:8983/solr/core1')
    argParser.add_argument('--inputDir', required=True, help='path to directory containing files')
    argParser.add_argument('--accept', nargs='+', type=str, help='Ingest only certain IANA MIME types')
    args = argParser.parse_args()

    if args.inputDir:

        URL = args.URL.rstrip('/') + '/'
        if "solr" in URL:
            solrIngest(URL, args.inputDir, args.accept)
        else:
            print "Defaulting to Elasticsearch"
            ingestES(URL, args.inputDir, args.accept)