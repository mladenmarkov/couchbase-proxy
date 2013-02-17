package com.sophisticated.db.couchbase;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Mladen Markov (mladen.markov@gmail.com)
 * @version 1.0, 2013/02/14 3:45 PM
 */
public class CouchbaseHandler extends AbstractHandler {
  private static final Logger LOG = Logger.getLogger(CouchbaseHandler.class);

  private JSONObject errDocNotFound;
  private Map<String, CouchbaseClient> clients;

  public CouchbaseHandler(Map<String, CouchbaseClient> clients) throws IOException {
    this.clients = clients;

    // prepare a reply for the most used error to avoid creating it all the time
    errDocNotFound = new JSONObject();
    try {
      errDocNotFound.put("code", 13);
      errDocNotFound.put("message", "No such document");
    } catch (JSONException e) {
      throw new IOException("Unable to create JSON document", e);
    }
  }

  public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
    // the following requests are supported
    // GET http://localhost:8080/bucket/key
    // GET http://localhost:8080/bucket?keys=key1,key2,key3
    // POST http://localhost:8080/bucket/key
    // POST http://localhost:8080/bucket/key?cas=1234567890

    String bucket;
    long cas = 0;
    String key = null;
    String[] keys = null;

    LOG.debug("Processing [" + request.getMethod() + "] request, path [" + request.getPathInfo() + "], query [" + request.getQueryString() + "], content length [" + request.getContentLength() + "]");
    String path = request.getPathInfo().substring(1); // the path always starts with "/" - remove it
    String[] elements = path.split("/");
    if (elements.length > 2) {
      // some invalid resource, like /some/unsupported/path/

      httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      request.setHandled(true);
      httpServletResponse.getWriter().print(makeError(2, "Invalid resource"));
      LOG.debug("Invalid resource requested");
      return;
    }

    if (elements.length > 0) {
      // there's a bucket, like /bucket/...
      bucket = elements[0];
    } else {
      httpServletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
      request.setHandled(true);
      httpServletResponse.getWriter().print(makeError(3, "No bucket provided"));
      LOG.debug("No bucket provided");
      return;
    }

    if (elements.length == 2) {
      key = elements[1];
    } else if (elements.length == 1) {
      // only a bucket in the path, keys should be in the params, like /bucket?keys=key1,key2,key3

      String keysParam = request.getParameter("keys");
      if (keysParam == null || keysParam.length() == 0) {
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        request.setHandled(true);
        httpServletResponse.getWriter().print(makeError(4, "Either document key as a resource or keys as 'keys' parameters must be provided"));
        LOG.debug("No document key(s)");
        return;
      }

      keys = keysParam.split(",");
      if (keys.length == 0) {
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        request.setHandled(true);
        httpServletResponse.getWriter().println(makeError(5, "No documents keys. Either document key as a resource or keys as 'keys' parameters must be provided"));
        LOG.debug("Empty keys parameter");
        return;
      }
    } else {
      // should not happen, but just to be sure
      httpServletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      request.setHandled(true);
      httpServletResponse.getWriter().print(makeError(6, "Unexpected error #100"));
      LOG.error("Unexpected internal error #100");
      return;
    }

    // find the client we have for that bucket
    CouchbaseClient client = clients.get(bucket);
    if (client == null) {
      httpServletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
      request.setHandled(true);
      httpServletResponse.getWriter().print(makeError(3, "Unknown bucket [" + bucket + "]"));
      LOG.debug("Bucket [" + bucket + "] is not configured for proxying");
      return;
    }

    if (request.getParameter("cas") != null) {
      try {
        cas = Long.parseLong(request.getParameter("cas"));
      } catch (NumberFormatException e) {
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        request.setHandled(true);
        httpServletResponse.getWriter().print(makeError(7, "Invalid CAS value [" + request.getParameter("cas") + "]"));
        LOG.debug("Invalid CAS value [" + request.getParameter("cas"));
        return;
      }
    }

    if (request.getMethod().equalsIgnoreCase("get")) {
      if (keys == null) {
        if (key != null) {
          keys = new String[] {key};
        } else {
          // should not happen, but just to be sure
          httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          request.setHandled(true);
          httpServletResponse.getWriter().print(makeError(4, "Either document key as a resource or keys as 'keys' parameters must be provided"));
          LOG.error("Unexpected internal state error #101");
          return;
        }
      }

      String result = get(client, keys);

      httpServletResponse.setContentType("application/json;charset=UTF-8");
      httpServletResponse.setStatus(HttpServletResponse.SC_OK);
      request.setHandled(true);
      httpServletResponse.getWriter().print(result);

      if (LOG.isDebugEnabled()) {
        String keysString = "";
        for (String k : keys) {
          if (keysString.length() > 0) {
            keysString += ",";
          }
          keysString += k;
        }
        LOG.debug("Returned documents keys [" + keysString + "]");
      }
    } else if (request.getMethod().equalsIgnoreCase("post")) {
      if (key == null) {
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        request.setHandled(true);
        httpServletResponse.getWriter().print(makeError(4, "No document key"));
        LOG.debug("No document key");
        return;
      }

      if (!request.getContentType().toLowerCase().contains("application/json")) {
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        request.setHandled(true);
        httpServletResponse.getWriter().print(makeError(8, "Unsupported content type [" + request.getContentType() + "]"));
        LOG.debug("Unsupported content type [" + request.getContentType() + "]");
        return;
      }

      String body = readBody(httpServletRequest);
      String result = post(client, key, cas, body);

      httpServletResponse.setContentType("application/json");
      httpServletResponse.setStatus(HttpServletResponse.SC_OK);
      request.setHandled(true);
      httpServletResponse.getWriter().print(result);

      LOG.debug("Saved document key [" + key + "]");
    } else {
      httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      request.setHandled(true);
      httpServletResponse.getWriter().print(makeError(9, "Unsupported method type [" + request.getMethod() + "]"));
      LOG.debug("Unsupported method [" + request.getMethod() + "]");
    }
  }

  private String readBody(HttpServletRequest request) throws IOException {
    StringBuilder body = new StringBuilder(request.getContentLength() > 0 ? request.getContentLength() : 5000);
    BufferedReader reader = request.getReader();

    String line;
    while ((line = reader.readLine()) != null) {
      body.append(line).append("\n");
    }

    return body.toString();
  }

  private String get(CouchbaseClient client, String[] keys) {
    List<JSONObject> array = new ArrayList<JSONObject>();

    for (String key: keys) {
      JSONObject element = new JSONObject();
      try {
        CASValue cas = client.gets(key);
        if (cas != null) {
          element.put("key", key);
          element.put("cas", cas.getCas());
          element.put("doc", cas.getValue().toString()); // send the document as a String to avoid marshalling/demarshalling JSON
        } else {
          element.put("err", errDocNotFound);
        }
      } catch (JSONException e) {
        try {
          element.put("err", "Unable to parse JSON: " + e.getMessage());
        } catch (JSONException e1) {
          LOG.error("Unexpected error creating JSON response", e);
        }
      }

      array.add(element);
    }

    JSONArray result = new JSONArray(array);
    return result.toString();
  }

  private String post(CouchbaseClient client, String key, long cas, String doc) {
    JSONObject result = new JSONObject();

    try {
      if (cas == 0) {
        OperationFuture<Boolean> op = client.set(key, 0, doc);
        if (!op.get()) {
          LOG.error("Unable to save document. Coucbase SDK returned [false]");
          return makeError(10, "Unable to save document");
        }
      } else {
        CASResponse response = client.cas(key, cas, doc);
        if (response.equals(CASResponse.OK)) {
        } else if (response.equals(CASResponse.NOT_FOUND)) {
        } else if (response.equals(CASResponse.EXISTS)) {
          return makeError(12, "New CAS value");
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while executing command via Couchbase SDK", e);
      return makeError(1, "Interrupted while executing command");
    } catch (ExecutionException e) {
      LOG.error("Unable to execute command via Couchbase SDK", e);
      return makeError(1, "Unable to execute command");
    }

    return result.toString();
  }

  private String makeError(int code, String message) {
    return "{\"err\":{\"code\": " + code + ", \"message\": \"" + message + "\"}}";
  }
}
