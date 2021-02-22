package com.orientechnologies.orient.server.distributed.http;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class EEBaseDistributedHttpTest {

  @Rule public TestName name = new TestName();

  private OServer server0;
  private OServer server1;
  private OServer server2;

  private String realm = "OrientDB-";
  private String userName = "root";
  private String userPassword = "root";
  private String databaseName;
  private Boolean keepAlive = null;

  private HttpRequestBase request;
  private AbstractHttpEntity payload;
  private HttpResponse response;
  private int retry = 1;
  private OrientDB remote;

  @Before
  public void init() throws Exception {

    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");

    remote = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database `"
            + name.getMethodName()
            + "` plocal users(admin identified by 'admin' role admin, reader identified by 'reader' role reader, writer identified by 'writer' role writer)");
  }

  @After
  public void after() throws InterruptedException {

    remote.drop(name.getMethodName());
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
  }

  public enum CONTENT {
    TEXT,
    JSON
  }

  public EEBaseDistributedHttpTest payload(final String s, final CONTENT iContent) {
    payload =
        new StringEntity(
            s,
            ContentType.create(
                iContent == CONTENT.JSON ? "application/json" : "plain/text", Consts.UTF_8));
    return this;
  }

  protected boolean isInDevelopmentMode() {
    final String env = System.getProperty("orientdb.test.env");
    return env == null || env.equals("dev");
  }

  protected EEBaseDistributedHttpTest exec() {
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

    if (keepAlive != null) request.addHeader("Connection", keepAlive ? "Keep-Alive" : "Close");

    if (payload != null && request instanceof HttpEntityEnclosingRequestBase)
      ((HttpEntityEnclosingRequestBase) request).setEntity(payload);

    final CloseableHttpClient httpClient = HttpClients.createDefault();

    // DefaultHttpMethodRetryHandler retryhandler = new DefaultHttpMethodRetryHandler(retry, false);
    // context.setAttribute(HttpMethodParams.RETRY_HANDLER, retryhandler);

    try {
      response = httpClient.execute(targetHost, request, context);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return this;
  }

  protected EEBaseDistributedHttpTest get(final String url, Header[] headers) {
    request = new HttpGet(getBaseURL() + "/" + url);
    request.setHeaders(headers);

    response = null;
    return this;
  }

  protected EEBaseDistributedHttpTest get(final String url) {
    request = new HttpGet(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected EEBaseDistributedHttpTest post(final String url) {
    request = new HttpPost(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected EEBaseDistributedHttpTest put(final String url) {
    request = new HttpPut(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected EEBaseDistributedHttpTest delete(final String url) {
    request = new HttpDelete(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected EEBaseDistributedHttpTest patch(final String url) {
    request = new HttpPatch(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected HttpResponse getResponse() {
    if (response == null) {
      exec();
    }
    return response;
  }

  protected EEBaseDistributedHttpTest setKeepAlive(final boolean iValue) {
    keepAlive = iValue;
    return this;
  }

  protected String getBaseURL() {
    return getProtocol() + "://" + getHost() + ":" + getPort();
  }

  public String getUserName() {
    return userName;
  }

  protected EEBaseDistributedHttpTest setUserName(final String userName) {
    this.userName = userName;
    return this;
  }

  public EEBaseDistributedHttpTest setRetry(final int iRetry) {
    retry = iRetry;
    return this;
  }

  protected String getUserPassword() {
    return userPassword;
  }

  protected EEBaseDistributedHttpTest setUserPassword(final String userPassword) {
    this.userPassword = userPassword;
    return this;
  }

  protected String getProtocol() {
    return "http";
  }

  protected String getHost() {
    return "localhost";
  }

  protected int getPort() {
    return 2480;
  }

  protected String getDatabaseName() {
    return databaseName;
  }

  protected String getRealm() {
    return realm;
  }

  protected EEBaseDistributedHttpTest setRealm(String realm) {
    this.realm = realm;
    return this;
  }
}
