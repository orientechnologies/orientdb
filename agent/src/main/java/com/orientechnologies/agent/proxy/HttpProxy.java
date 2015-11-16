package com.orientechnologies.agent.proxy;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Map;

/**
 * Created by Enrico Risa on 16/11/15.
 */
public class HttpProxy {

  public static void proxyRequest(ODistributedServerManager manager, String dest, OHttpRequest request, OHttpResponse response)
      throws IOException {

    proxyRequest(manager, dest, request, response, null);

  }

  public static void proxyRequest(ODistributedServerManager manager, String dest, OHttpRequest request, OHttpResponse response,
      HttpProxyListener listener) throws IOException {

    try {

      ODocument clusterConfiguration = manager.getClusterConfiguration();
      Collection<ODocument> members = clusterConfiguration.field("members");
      for (ODocument member : members) {
        String name = member.field("name");
        if (name.equalsIgnoreCase(dest)) {
          if (listener == null) {
            listener = new HttpProxyListener() {
              @Override
              public void onProxySuccess(OHttpRequest request, OHttpResponse response, InputStream is) throws IOException {

                InputStreamReader isr = new InputStreamReader(is);
                int numCharsRead;
                char[] charArray = new char[1024];
                StringBuffer sb = new StringBuffer();
                while ((numCharsRead = isr.read(charArray)) > 0) {
                  sb.append(charArray, 0, numCharsRead);
                }
                String res = sb.toString();
                ODocument result = new ODocument();
                ODocument fromJSON = result.fromJSON(res);
                response.writeRecord(fromJSON, null, "");
              }

            };
          }
          fetchFromServer(member, request, response, listener);

        }
      }
    } catch (Exception e) {
      response.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
  }

  public static void fetchFromServer(ODocument member, OHttpRequest request, OHttpResponse response, HttpProxyListener proxyListener)
      throws Exception {

    Collection<Map> listeners = member.field("listeners");

    for (Map listener : listeners) {
      String protocol = (String) listener.get("protocol");
      if (protocol.equalsIgnoreCase("ONetworkProtocolHttpDb")) {
        String listen = (String) listener.get("listen");
        String url = "http://" + listen + request.getUrl();

        if (request.getParameters().size() > 0) {
          url += appendParamenters(request);
        }
        final URL remoteUrl = new java.net.URL(url);
        HttpURLConnection urlConnection = openConnectionForServer(member, request.getHeader("Authorization"), remoteUrl,
            request.httpMethod);
        InputStream is = urlConnection.getInputStream();
        proxyListener.onProxySuccess(request, response, is);
      }
    }

  }

  private static String appendParamenters(OHttpRequest method) throws UnsupportedEncodingException {

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("?");
    Map<String, String> parameters = method.getParameters();
    int i = 0;
    int size = parameters.size();
    for (String s : parameters.keySet()) {
      stringBuilder.append(URLEncoder.encode(s, "UTF-8") + "=" + URLEncoder.encode(parameters.get(s), "UTF-8"));
      if (i < size) {
        stringBuilder.append("&");
      }
      i++;
    }
    return stringBuilder.toString();
  }

  private static HttpURLConnection openConnectionForServer(final ODocument member, String authentication, URL iRemoteUrl,
      final String iMethod) throws IOException, ProtocolException {

    HttpURLConnection urlConnection = (HttpURLConnection) iRemoteUrl.openConnection();

    urlConnection.setRequestProperty("Authorization", authentication);

    urlConnection.setRequestMethod(iMethod);

    urlConnection.connect();
    return urlConnection;
  }

  public interface HttpProxyListener {

    public void onProxySuccess(OHttpRequest request, OHttpResponse response, InputStream is) throws IOException;
  }
}
