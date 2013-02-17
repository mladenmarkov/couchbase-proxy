couchbase-proxy
===============

A Java proxy to expose basic Couchbase API via a simple REST API.
The only need for this project is a problem in Coucbase node.js client library which fails if you feed it with too many documents to save.
Depending on the machine, it can handle a maximum of 50-100 documents at a time. Feed it more and it just halts with 100% CPU.
See JIRA bug <a href="http://www.couchbase.com/issues/browse/JSCBC-14">JSCBC-14</a>.

Feel free to fork and extend if necessary.

REST API
========
Currently only the following API methods are supported
- gets - Get one or more documents by keys. Returns the value (document) and CAS for each key.
- set - Save one document, no checks are done.
- cas - Save one document, first checking the CAS value.

The following REST API is exposed

- <code>GET /bucket/key</code> - Get a single document with key <code>key</code> from bucket <code>bucket</code>.
- <code>GET /bucket?ids=key1,key2,key3</code> - Get multiple documents with keys <code>key1</code>, <code>key2</code>, <code>key3</code> from bucket <code>bucket</code>.
- <code>POST /bucket/key</code> - Save a single document with key <code>key</code> in bucket <code>bucket</code>. The document is passed as the POST body. If the document exists, it is overwritten.
- <code>POST /bucket/key?cas=1234567890</code> - Save a single document with key <code>key</code> in bucket <code>bucket</code>. The passed <code>cas</code> parameter contains a CAS value to be used when saving so concurrent modifications are detected.

The GET methods return array of documents in the following JSON format. If a single document is requested, a single-element array is returned. When an <code>err</code> element is present, the document was not retrieved and the <code>doc</code>, <code>cas</code> and <code>key</code> elements are not present. And vice-versa.
<pre>
{
  "doc": document-as-string,
  "cas": cas-value-as-long,
  "key": document-key-as-string,
  "err": {
    "code": error-code-as-int,
    "message": error-message-as-string
  }
}
</pre>

In case or errors all methods return the error in the following JSON format
<pre>
{
  "err": {
    "code": error-code-as-int,
    "message": error-message-as-string
  }
}
</pre>

The following errors indicate issues when communicating with this proxy
- 2 - Invalid resource. The endpoint is incorrect and not supported.
- 3 - No bucket provided or bucket is not supported. Missing the bucket element of the REST endpoint or it was provided but not configured to be proxied.
- 4 - No document key(s) provided - neither as part of the REST endpoint path nor via the <code>keys</code> parameter.
- 5 - No keys found in the <code>keys</code> parameter. The <code>keys</code> parameter is a comma-separated value.
- 6 - Unexpected internal error.
- 7 - Invalid CAS value.
- 8 - Unsupported content type. Only <code>application/json</code> content type is supported and must be explicitly set when send POST requests.
- 9 - Unsupoorted method type. Only <code>GET</code> and <code>POST</code> are supported.

The following errors indicate issues when communicating with Couchbase server
- 10 - Unable to save the document.
- 12 - CAS value mismatch. The document has been modified.
- 13 - No such document.
- 14 - Interrupted while executing command.

Build
=====
It's built with Maven. Just execute <code>mvn verify</code> and you will have the JAR in the <code>target</code> folder. Start it as simply as <code>java -jar target/couchbase-proxy-1.0.jar</code>.
<pre>
$ mvn verify
$ java -jar target/couchbase-proxy-1.0.jar
</pre>

Configuration
=============
The configuration is a simple JSON file in the current directory, named <code>couchbase-proxy.json</code>. An alternative filename can be passed as (the one and only) command line argument.

<pre>
{
  "server": {
    "port": 8080
  },
  "couchbase": {
    "host": "localhost",
    "port": 8091,
    "buckets": [
      "facebook-pages",
      "facebook-users",
      "account-requests"
    ]
  }
}
</pre>

The <code>port</code> element specifies a port the REST server will listen on. The <code>couchbase</code> element describes the connection to Couchbase server and the buckets to be supported for proxying. 
Only calls for the buckets described here will be accepted.
