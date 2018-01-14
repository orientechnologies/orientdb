package com.orientechnologies.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.cloud.CloudEndpoint;
import com.orientechnologies.agent.cloud.CloudException;
import com.orientechnologies.agent.cloud.CloudPushEndpoint;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Enrico Risa on 08/01/2018.
 */
public class OEnterpriseCloudManager extends Thread {
  private final OEnterpriseAgent  agent;
  protected     CloudEndpoint     cloudEndpoint;
  private       CloudPushEndpoint cloudPushEndpoint;
  private       String            token;
  private       String            projectId;
  private       String            authToken;
  private       String            refreshToken;

  private        Object monitor   = new Object();
  private static String tokenPath = "/uaa/oauth/token";

  private AtomicBoolean refreshing = new AtomicBoolean(false);

  ObjectMapper objectMapper = new ObjectMapper();

  private String cloudBaseUrl;

  public OEnterpriseCloudManager(OEnterpriseAgent agent) {
    this.agent = agent;
    init();
    cloudEndpoint = new CloudEndpoint(this);
    cloudPushEndpoint = new CloudPushEndpoint(this);

  }

  public void shutdown() {
    cloudEndpoint.shutdown();
    cloudPushEndpoint.shutdown();
  }

  public <T> T runWithToken(ConsumerWithToken<T> function) throws IOException, ClassNotFoundException, CloudException {

    do {
      try {
        if (!refreshing.get()) {
          return function.accept(authToken);
        } else {
          waitForRefresh();
        }
      } catch (CloudException e) {
        if (e.getStatus() == 401) {
          if (refreshing.compareAndSet(false, true)) {
            OLogManager.instance().info(this, "Token is Expired. Negotiation in progress.");
            tryRefreshToken();
            OLogManager.instance().info(this, "Token negotiation finished");
          } else {
            OLogManager.instance().info(this, "Waiting for token generation.");
            waitForRefresh();
          }

        } else {
          throw e;
        }
      }
    } while (true);
  }

  private void tryRefreshToken() throws IOException, CloudException {
    try {
      refreshToken();
    } catch (CloudException ex) {
      negotiationToken();
    } finally {
      refreshing.set(false);
      synchronized (monitor) {
        monitor.notifyAll();
      }
    }
  }

  private void waitForRefresh() {
    synchronized (monitor) {
      try {
        monitor.wait();
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
    }
  }

  private void refreshToken() throws IOException, CloudException {

    try {
      URI uri = new URIBuilder(new URI(cloudBaseUrl + tokenPath)).addParameter("projectId", projectId)
          .addParameter("grant_type", "refresh_token").addParameter("refresh_token", refreshToken).build();

      String auth = "agent" + ":";
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("ISO-8859-1")));
      String authHeader = "Basic " + new String(encodedAuth);
      String body = post(uri.getPath() + "?" + uri.getQuery(), authHeader);

      JsonNode tree = objectMapper.readTree(body);

      JsonNode accessToken = tree.get("access_token");
      JsonNode refreshToken = tree.get("refresh_token");

      authToken = accessToken.asText();
      this.refreshToken = refreshToken.asText();

    } catch (URISyntaxException e) {

      e.printStackTrace();
    }
  }

  private void negotiationToken() throws IOException, CloudException {

    try {

      URI uri = new URIBuilder(new URI(cloudBaseUrl + tokenPath)).addParameter("projectId", projectId).addParameter("token", token)
          .addParameter("grant_type", "customAgent").build();

      String auth = "agent" + ":";
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("ISO-8859-1")));
      String authHeader = "Basic " + new String(encodedAuth);
      String body = post(uri.getPath() + "?" + uri.getQuery(), authHeader);

      JsonNode tree = objectMapper.readTree(body);

      JsonNode accessToken = tree.get("access_token");
      JsonNode refreshToken = tree.get("refresh_token");

      authToken = accessToken.asText();
      this.refreshToken = refreshToken.asText();

    } catch (URISyntaxException e) {

    }
  }

  private void throwError(String path, int statusCode, String err) throws CloudException, IOException {

    JsonNode tree = objectMapper.readTree(err);

    String msg = "";
    String errMsg = "";
    if (tree != null) {
      JsonNode message = tree.get("message");
      JsonNode error = tree.get("error");
      msg = message != null ? message.asText() : msg;
      errMsg = error != null ? error.asText() : errMsg;
    }

    throw new CloudException(path, statusCode, msg, errMsg);
  }

  public String get(String path) throws IOException, CloudException {
    return get(path, null);
  }

  public String get(String path, String authentication) throws IOException, CloudException {
    CloseableHttpClient client = HttpClients.createDefault();

    try {
      HttpGet request = new HttpGet(cloudBaseUrl + path);

      if (authentication != null) {
        request.addHeader("Authorization", authentication);
      }

      HttpResponse response = client.execute(request);

      InputStream content = response.getEntity().getContent();
      String test = OIOUtils.readStreamAsString(content);
      if (response.getStatusLine().getStatusCode() != 200) {
        throwError(path, response.getStatusLine().getStatusCode(), test);
      }
      return test;

    } finally {
      client.close();
    }
  }

  public String post(String path, String authentication) throws IOException, CloudException {
    return post(path, authentication, null);
  }

  public String post(String path, String authentication, String body) throws IOException, CloudException {
    CloseableHttpClient client = HttpClients.createDefault();

    try {
      HttpPost request = new HttpPost(cloudBaseUrl + path);

      if (authentication != null) {
        request.addHeader("Authorization", authentication);
      }

      if (body != null) {
        StringEntity entity = new StringEntity(body);
        request.setEntity(entity);
      }
      request.setHeader("Accept", "application/json");
      request.setHeader("Content-type", "application/json");
      HttpResponse response = client.execute(request);

      InputStream content = response.getEntity().getContent();
      String test = OIOUtils.readStreamAsString(content);
      if (response.getStatusLine().getStatusCode() != 200) {
        throwError(path, response.getStatusLine().getStatusCode(), test);
      }
      return test;

    } finally {
      client.close();
    }
  }

  private void init() {
    token = OGlobalConfiguration.CLOUD_PROJECT_TOKEN.getValue();
    cloudBaseUrl = OGlobalConfiguration.CLOUD_BASE_URL.getValue();
    projectId = OGlobalConfiguration.CLOUD_PROJECT_ID.getValue();

  }

  public OEnterpriseAgent getAgent() {
    return agent;
  }

  @Override
  public void run() {

    init();

    while (authToken == null) {
      try {
        negotiationToken();

      } catch(ConnectException exception){
        OLogManager.instance().warn(this, "OrientDB cloud is offline");
      } catch (Exception e) {
        OLogManager.instance().warn(this, "Error negotiating token", e);
      }

      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    cloudEndpoint.start();
    cloudPushEndpoint.start();

  }

  public interface ConsumerWithToken<T> {
    T accept(String token) throws IOException, CloudException, ClassNotFoundException;
  }
}
