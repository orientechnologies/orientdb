package com.orientechnologies.agent.proxy;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.OL;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Enrico Risa on 16/11/15.
 */
public class HttpProxy {

  ExecutorService threadpool = Executors.newFixedThreadPool(2);

  public void proxyRequest(ODistributedServerManager manager, String dest, OHttpRequest request, OHttpResponse response)
      throws IOException {

    proxyRequest(manager, dest, request, response, null);

  }

  protected ODocument findMember(ODistributedServerManager manager, String dest) {
    ODocument clusterConfiguration = manager.getClusterConfiguration();
    Collection<ODocument> members = clusterConfiguration.field("members");
    for (ODocument member : members) {
      String name = member.field("name");
      if (name.equalsIgnoreCase(dest)) {
        return member;
      }
    }
    return null;
  }

  protected Collection<ODocument> getMembers(ODistributedServerManager manager) {
    ODocument clusterConfiguration = manager.getClusterConfiguration();
    Collection<ODocument> members = clusterConfiguration.field("members");
    return members;
  }

  public void broadcastRequest(ODistributedServerManager manager, OHttpRequest request, OHttpResponse iResponse)
      throws IOException {

    Collection<ODocument> members = getMembers(manager);

    Collection<HttpBroadcastCallable> callables = new ArrayList<HttpBroadcastCallable>();

    for (ODocument member : members) {
      callables.add(new HttpBroadcastCallable(this, request, manager, member));
    }

    try {
      List<Future<HttpProxyResponse>> futures = threadpool.invokeAll(callables);

      ODocument result = new ODocument();

      Collection<ODocument> responses = new ArrayList<ODocument>();
      ODocument errors = new ODocument();
      result.field("results", responses);
      result.field("errors", errors);

      for (Future<HttpProxyResponse> future : futures) {
        HttpProxyResponse proxyResponse = future.get();
        if (proxyResponse.getCode() == 200) {
          responses.add(toDoc(proxyResponse.getStream()));
        } else {
          errors.field(proxyResponse.getName(), toDoc(proxyResponse.getStream()));
        }

      }
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, result.toJSON("prettyPrint"), null);
    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_JSON, e, null);
    }
  }

  public void proxyRequest(ODistributedServerManager manager, String dest, OHttpRequest request, OHttpResponse response,
      HttpProxyListener listener) throws IOException {

    try {

      ODocument member = findMember(manager, dest);

      if (member == null) {
        throw new IllegalArgumentException("Cannot find target node: " + dest);
      }

      if (listener == null) {
        listener = new HttpProxyListener() {
          @Override
          public void onProxySuccess(OHttpRequest request, OHttpResponse response, InputStream is) throws IOException {

            try {
              ODocument fromJSON = toDoc(is);
              response.writeRecord(fromJSON, null, "");
            } catch (OSerializationException e) {
              response.send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, OHttpUtils.STATUS_OK_NOCONTENT_DESCRIPTION,
                  OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
            }
          }

          @Override
          public void onProxyError(OHttpRequest request, OHttpResponse response, InputStream is, int code, Exception e)
              throws IOException {
            response.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e,
                null);
          }

        };
      }
      fetchFromServer(manager, member, request, request.getParameters(), response, listener);
    } catch (Exception e) {
      response.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
  }

  private ODocument toDoc(InputStream is) throws IOException {
    InputStreamReader isr = new InputStreamReader(is);
    int numCharsRead;
    char[] charArray = new char[1024];
    StringBuffer sb = new StringBuffer();
    while ((numCharsRead = isr.read(charArray)) > 0) {
      sb.append(charArray, 0, numCharsRead);
    }
    String res = sb.toString();
    ODocument result = new ODocument();
    return result.fromJSON(res);
  }

  public static void fetchFromServer(ODistributedServerManager manager, ODocument member, OHttpRequest request,
      Map<String, String> parameters, OHttpResponse response, HttpProxyListener proxyListener) throws Exception {

    Map<String, Object> map = manager.getConfigurationMap();

    String pwd = (String) map.get(OEnterpriseAgent.EE + member.field("name"));
    Collection<Map> listeners = member.field("listeners");

    String decrypt = "root:" + OL.decrypt(pwd);

    String authStringEnc = "Basic " + OBase64Utils.encodeBytes(decrypt.getBytes());

    for (Map listener : listeners) {
      String protocol = (String) listener.get("protocol");
      if (protocol.equalsIgnoreCase("ONetworkProtocolHttpDb")) {
        String listen = (String) listener.get("listen");
        String url = "http://" + listen + request.getUrl();

        if (parameters.size() > 0) {
          url += appendParamenters(parameters);
        }
        final URL remoteUrl = new java.net.URL(url);
        HttpURLConnection urlConnection = openConnectionForServer(member, authStringEnc, pwd, remoteUrl, request.httpMethod,
            request.content);

        try {
          InputStream is = urlConnection.getInputStream();
          proxyListener.onProxySuccess(request, response, is);
        } catch (Exception e) {
          int responseCode = urlConnection.getResponseCode();
          proxyListener.onProxyError(request, response, urlConnection.getErrorStream(), responseCode, e);
        }
      }
    }

  }

  private static String appendParamenters(Map<String, String> parameters) throws UnsupportedEncodingException {

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("?");
    int i = 0;
    int size = parameters.size();
    for (String s : parameters.keySet()) {
      stringBuilder.append(URLEncoder.encode(s, "UTF-8") + "=" + URLEncoder.encode(parameters.get(s), "UTF-8"));
      if (i < size - 1) {
        stringBuilder.append("&");
      }
      i++;
    }
    return stringBuilder.toString();
  }

  private static HttpURLConnection openConnectionForServer(final ODocument member, String authentication, String token,
      URL iRemoteUrl, String httpMethod, final String content) throws IOException, ProtocolException {

    HttpURLConnection urlConnection = (HttpURLConnection) iRemoteUrl.openConnection();

    urlConnection.setRequestProperty("Authorization", authentication);

    urlConnection.setRequestProperty("X-REQUEST-AGENT", token);

    urlConnection.setRequestMethod(httpMethod);

    if (content != null) {
      urlConnection.setDoOutput(true);
      OutputStream os = urlConnection.getOutputStream();
      os.write(content.getBytes("UTF-8"));
      os.close();
    } else {
      urlConnection.connect();
    }
    return urlConnection;
  }

}
