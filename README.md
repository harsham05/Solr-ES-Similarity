


1. Mount your raw images

```
python -m SimpleHTTPServer
```

2. source the below into **solr-es.env**

```
export SOLR_PREFIX=/data/memex/electronics-images/

export IMAGE_MOUNT=http://localhost:8000/

```

3.
```
python solr-similarity.py
```

#k-means
http://localhost:5000/kmeans