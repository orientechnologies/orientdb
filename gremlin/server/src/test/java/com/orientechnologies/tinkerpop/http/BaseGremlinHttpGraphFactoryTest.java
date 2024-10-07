package com.orientechnologies.tinkerpop.http;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.tinkerpop.AbstractRemoteGraphFactoryTest;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/** Created by Enrico Risa on 07/02/17. */
public abstract class BaseGremlinHttpGraphFactoryTest extends AbstractRemoteGraphFactoryTest {

  private String protocol = "http";
  private String host = "localhost";
  private int port = 2480;
  private String userName = "admin";
  private String userPassword = "admin";

  protected HttpResponse postWithBody(final String url, Optional<ODocument> body)
      throws IOException {
    return post(url, body.map(d -> d.toJSON()));
  }

  protected HttpResponse post(final String url, Optional<String> body) throws IOException {
    HttpPost httpPost = new HttpPost(getBaseURL() + "/" + url);

    if (body.isPresent()) {
      httpPost.setEntity(new StringEntity(body.get()));
    }

    return exec(httpPost);
  }

  protected HttpResponse exec(HttpRequestBase request) throws IOException {
    final HttpHost targetHost = new HttpHost(getHost(), getPort(), getProtocol());

    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(
        new AuthScope(targetHost),
        new UsernamePasswordCredentials(getUserName(), getUserPassword()));

    // Create AuthCache instance
    AuthCache authCache = new BasicAuthCache();
    // Generate BASIC scheme object and add it to the local auth cache
    BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);

    // Add AuthCache to the execution context
    HttpClientContext context = HttpClientContext.create();
    context.setCredentialsProvider(credsProvider);
    context.setAuthCache(authCache);

    request.addHeader("Connection", "Keep-Alive");

    final CloseableHttpClient httpClient = HttpClients.createDefault();

    return httpClient.execute(targetHost, request, context);
  }

  protected String getBaseURL() {
    return getProtocol() + "://" + getHost() + ":" + getPort();
  }

  protected String getProtocol() {
    return protocol;
  }

  protected String getHost() {
    return host;
  }

  protected int getPort() {
    return port;
  }

  public String getUserName() {
    return userName;
  }

  protected String getUserPassword() {
    return userPassword;
  }

  protected String getDatabaseName() {
    return name.getMethodName();
  }

  protected ODocument asDocument(InputStream stream) throws IOException {
    return new ODocument().fromJSON(stream);
  }

  protected ODocument asDocument(String body) throws IOException {
    return new ODocument().fromJSON(body);
  }

  protected String asString(InputStream stream) throws IOException {
    return OIOUtils.readStreamAsString(stream);
  }
}
