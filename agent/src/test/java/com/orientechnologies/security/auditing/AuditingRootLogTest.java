package com.orientechnologies.security.auditing;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.OSystemUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.http.EEBaseServerHttpTest.CONTENT;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AuditingRootLogTest {

  @Rule public TestName name = new TestName();

  protected OServer server;

  private String realm = "OrientDB-";
  private String userName = "root";
  private String userPassword = "root";
  private Boolean keepAlive = null;

  private HttpRequestBase request;
  private AbstractHttpEntity payload;
  private HttpResponse response;
  protected OrientDB remote;

  @Before
  public void init() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-simple-config.xml");

    remote = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());

    if (shouldCreateDatabase()) {
      remote.execute(
          "create database `"
              + name.getMethodName()
              + "` plocal users(admin identified by 'admin' role admin, reader identified by"
              + " 'reader' role reader, writer identified by 'writer' role writer)");
    }
  }

  @After
  public void after() {

    remote.drop(name.getMethodName());
    remote.close();

    server.shutdown();
  }

  public enum CONTENT {
    TEXT,
    JSON
  }

  public AuditingRootLogTest payload(final String s, final CONTENT iContent) {
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

  protected AuditingRootLogTest exec() {
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

  protected AuditingRootLogTest get(final String url, Header[] headers) {
    request = new HttpGet(getBaseURL() + "/" + url);
    request.setHeaders(headers);

    response = null;
    return this;
  }

  protected AuditingRootLogTest get(final String url) {
    request = new HttpGet(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected AuditingRootLogTest post(final String url) {
    request = new HttpPost(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected AuditingRootLogTest put(final String url) {
    request = new HttpPut(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected AuditingRootLogTest delete(final String url) {
    request = new HttpDelete(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected AuditingRootLogTest patch(final String url) {
    request = new HttpPatch(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  public HttpResponse getResponse() {
    if (response == null) {
      exec();
    }
    return response;
  }

  protected AuditingRootLogTest setKeepAlive(final boolean iValue) {
    keepAlive = iValue;
    return this;
  }

  protected String getBaseURL() {
    return getProtocol() + "://" + getHost() + ":" + getPort();
  }

  public String getUserName() {
    return userName;
  }

  protected AuditingRootLogTest setUserName(final String userName) {
    this.userName = userName;
    return this;
  }

  protected String getUserPassword() {
    return userPassword;
  }

  protected AuditingRootLogTest setUserPassword(final String userPassword) {
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
    return name.getMethodName();
  }

  protected String getRealm() {
    return realm;
  }

  protected AuditingRootLogTest setRealm(String realm) {
    this.realm = realm;
    return this;
  }

  protected boolean shouldCreateDatabase() {
    return true;
  }

  @Test
  public void changePasswordWitRootTest() throws Exception {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    ODatabaseSession session = remote.open(name.getMethodName(), "root", "root");

    session
        .command("update OUser set password = ? where name = ?", new Object[] {"foo", "reader"})
        .close();

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 12")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Thread.sleep(100);
    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        "The password for user 'reader' has been changed", result.getProperty("note"));
  }

  @Test
  public void loginReader() throws Exception {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    ODatabaseSession session = remote.open(name.getMethodName(), "readerServer", "readerServer");

    long count = session.query("select from OUser").stream().count();

    Assert.assertEquals(0, count);
  }

  @Test(expected = OSecurityAccessException.class)
  public void loginGuest() throws Exception {
    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    ODatabaseSession session = remote.open(name.getMethodName(), "guest", "guest");
  }

  @Test
  public void reloadSecurityTest() throws Exception {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    server
        .getSecurity()
        .reload(
            new OSystemUser("root", null, "Server"), new ODocument().fromJSON(security, "noMap"));

    Thread.sleep(1000);

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 11")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        "The security configuration file has been reloaded", result.getProperty("note"));
  }

  @Test
  public void postSecurity() throws IOException, InterruptedException {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));

    try {
      server
          .getSystemDatabase()
          .executeWithDB(
              (db) -> {
                db.command("delete from OAuditingLog");
                return null;
              });
    } catch (OCommandExecutionException e) {

    }

    ODocument config = new ODocument().fromJSON(security, "noMap");

    ODocument cfg = new ODocument().field("config", config);

    HttpResponse response =
        post("/security/reload").payload(cfg.toJSON(), CONTENT.JSON).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Thread.sleep(1000);

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 11")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        "The security configuration file has been reloaded", result.getProperty("note"));
  }

  @Test
  public void postSecurityAndAuditingConfig() throws IOException {

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));

    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    String auditing =
        OIOUtils.readStreamAsString(
            Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("auditing-config.json"));

    ODocument config = new ODocument().fromJSON(auditing, "noMap");

    HttpResponse response =
        post("/auditing/" + name.getMethodName() + "/config")
            .payload(config.toJSON(), CONTENT.JSON)
            .getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 7")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        String.format(
            "The auditing configuration for the database '%s' has been changed",
            name.getMethodName()),
        result.getProperty("note"));
  }
}
