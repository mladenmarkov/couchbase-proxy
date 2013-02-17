package com.sophisticated.db.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.vbucket.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple proxy to relay requests to and from Couchbase 2.0 Servers using Java SDK.
 * Currently supports only <code>gets</code> and <code>cas</code> methods.
 * <p>
 * This proxy provides the following REST endpoints
 * <ul>
 *   <li><code>GET /bucket/key</code> - get a single document with key <code>key</code>.</li>
 *   <li><code>GET /bucket?keys=key1,key2,key3</code> - get multiple documents with keys listed in the <code>keys</code> parameter.</li>
 *   <li><code>POST /bucket/key</code> - set a single document with key <code>key</code> and value passed as POST body.
 *   If the document already exists, it is silently overwritten. No CAS checks are done.</li>
 *   <li><code>POST /bucket/key?cas=1234567890</code> - set a single document with key <code>key</code> and value passed as POST body.
 *   The passed <code>cas</code> parameter contains a CAS value to be used when saving so concurrent modifications are prevented.</li>
 * </ul>
 *
 * The <code>GET</code> methods return array of documents in the following JSON format. If a single document is requested, a single-element
 * array is returned. When <code>err</code> element is present, the document was not retrieved and the <code>doc</code>, <code>cas</code>
 * and <code>key</code> elements are not present. And vice-versa.
 * <pre>
 *   {
 *     "doc": [json]the-document,
 *     "cas": [long]cas-value,
 *     "key": [string]document-key,
 *     "err": {
 *       "code": [int]error-code,
 *       "message": [string]error-message
 *     }
 *   }
 * </pre>
 *
 * In case or errors all methods return the error in the following JSON format
 * <pre>
 *   {
 *     "err": {
 *       "code": [int]error-code,
 *       "message": [string]error-message
 *     }
 *   }
 * </pre>
 *
 * The following errors indicate issues when communicating with this proxy
 * <ul>
 *   <li>1 - </li>
 *   <li>2 - Invalid resource. The endpoint is incorrect and not supported.</li>
 *   <li>3 - No bucket provided or bucket is not supported. Missing the bucket element of the REST endpoint or it was provided but not configured to be proxied.</li>
 *   <li>4 - No document key(s) provided - neither as part of the REST endpoint path nor via the <code>keys</code> parameter.</li>
 *   <li>5 - No keys found in the <code>keys</code> parameter. The <code>keys</code> parameter is a comma-separated value.</li>
 *   <li>6 - Unexpected internal error.</li>
 *   <li>7 - Invalid CAS value.</li>
 *   <li>8 - Unsupported content type. Only <code>application/json</code> content type is supported and must be explicitly set when send POST requests.</li>
 *   <li>9 - Unsupoorted method type. Only <code>GET</code> and <code>POST</code> are supported.</li>
 * </ul>
 * The following errors indicate issues when communicating with Couchbase server
 * <ul>
 *   <li>10 - Unable to save the document.</li>
 *   <li>12 - CAS value mismatch. The document has been modified.</li>
 *   <li>13 - No such document.</li>
 * </ul>
 *
 * @author Mladen Markov (mladen.markov@gmail.com)
 * @version 1.0, 2013/02/14 2:39 PM
 */
public class CouchbaseProxy {
  private static final Logger LOG = Logger.getLogger(CouchbaseProxy.class);

  public static void main(String[] args) {
    PropertyConfigurator.configure(CouchbaseProxy.class.getClassLoader().getResource("log4j.properties"));

    String configFilename = "./couchbase-proxy.json";
    JSONObject config;

    String couchbaseHost = "localhost";
    int couchbasePort = 8091;
    int serverPort = 8080;
    String[] couchbaseBuckets = new String[] {"default"};

    if (args.length == 1) {
      configFilename = args[0];
    }

    File file = new File(configFilename);
    if (!file.exists()) {
      LOG.fatal("Unable to find file [" + configFilename + "]");
      System.out.println("Unable to find file " + configFilename);
      System.exit(1);
    }

    try {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      StringBuilder configJson = new StringBuilder(1000);
      String line;
      while ((line = reader.readLine()) != null) {
        configJson.append(line).append("\n");
      }

      config = new JSONObject(configJson.toString());

      if (config.has("couchbase")) {
        JSONObject couchbase = config.getJSONObject("couchbase");
        if (couchbase.has("host")) {
          couchbaseHost = couchbase.getString("host");
        }
        if (couchbase.has("port")) {
          couchbasePort = couchbase.getInt("port");
        }
        if (couchbase.has("buckets")) {
          List<String> bucketsList = new ArrayList<String>();

          JSONArray bucketsArray = couchbase.getJSONArray("buckets");
          for (int i = 0; i < bucketsArray.length(); i++) {
            bucketsList.add(bucketsArray.getString(i));
          }

          couchbaseBuckets = new String[bucketsList.size()];
          bucketsList.toArray(couchbaseBuckets);
        }
      }
    } catch (IOException e) {
      LOG.fatal("Unable to read configuration file [" + configFilename + "]", e);
      System.err.println("Unable to read configuration file " + configFilename);
      System.err.println(e.getMessage());
      System.exit(1);
    } catch (JSONException e) {
      LOG.fatal("Unable to parse configuration file [" + configFilename + "]", e);
      System.err.println("Unable to parse configuration file " + configFilename);
      System.err.println(e.getMessage());
      System.exit(1);
    }

    new CouchbaseProxy(serverPort, couchbaseHost, couchbasePort, couchbaseBuckets);
  }

  public CouchbaseProxy(int serverPort, String couchbaseHost, int couchbasePort, String[] buckets) {
    // we support only single-node servers
    List<URI> uris = new ArrayList<URI>();
    uris.add(URI.create("http://" + couchbaseHost + ":" + couchbasePort + "/pools"));

    // create a Couchbase client for each of the bucket we are configure to proxy
    Map<String, CouchbaseClient> clients = new HashMap<String, CouchbaseClient>();
    try {
      for (String bucket: buckets) {
        CouchbaseClient client = new CouchbaseClient(uris, bucket, "");
        clients.put(bucket, client);

        LOG.debug("Connected to Couchbase bucket [" + bucket + "]");
      }
    } catch (IOException e) {
      LOG.fatal("Unable to connect to Couchbase", e);
      System.err.println("Unable to connect to Couchbase: " + e.getMessage());
      System.exit(1);
    } catch (ConfigurationException e) {
      LOG.fatal("Invalid configuration to connect to Couchbase", e);
      System.err.println("Invalid configuration to connect to Couchbase: " + e.getMessage());
      System.exit(1);
    }

    // create a simple REST server using embedded Jetty
    try {
      Server server = new Server(serverPort);
      // configure our request handler with the clients for each bucket
      server.setHandler(new CouchbaseHandler(clients));

      server.start();
      server.join();

      LOG.info("Couchbase REST Proxy started on port [" + serverPort + "]. Proxying for buckets " + buckets);
    } catch (Exception e) {
      LOG.fatal("Unable to create REST server", e);
      System.err.println("Unable to create REST server: " + e.getMessage());
      System.exit(1);
    }
  }
}
