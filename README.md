
# Flask based REST application for Solr Elasticsearch similarity using Jaccard, Edit-distance, Cosine & K-means metrics

Persists data onto a Document Store, which is later Clustered & Visualized in D3

1.Mount your raw images & Start Solr Instance

```
python -m SimpleHTTPServer
```

2.Source the below into **solr-es.env**

```
export IMAGE_MOUNT=http://localhost:8000/electronics-images/
```

3.Start the application
```
python solr-similarity.py
```

4.Open the following URL

###jaccard
http://localhost:5000/static/dynamic-cluster.html

### In progress k-means
http://localhost:5000/kmeans


