package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class CloudEndpoint extends Thread {
  private final OEnterpriseAgent agent;
  private       boolean          terminate;
  private       long             requestInterval;
  private long MAX_REQUEST_INTERVAL = 2000;//milliseconds TODO make it parametric or tunable

  private String token;
  private String projectId;
  private String cloudBaseUrl;

  private static String requestPath  = "/commands/{projectId}";
  private static String responsePath = "/commands/{projectId}/response";

  public CloudEndpoint(OEnterpriseAgent oEnterpriseAgent) {
    agent = oEnterpriseAgent;
    init();
  }

  private void init() {
    token = OGlobalConfiguration.CLOUD_PROJECT_TOKEN.getValue();
    projectId = OGlobalConfiguration.CLOUD_PROJECT_ID.getValue();
    cloudBaseUrl = OGlobalConfiguration.CLOUD_BASE_URL.getValue();

  }

  @Override
  public void run() {
    while (!terminate) {
      try {
        handleRequest();
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (requestInterval > 0) {
        try {
          Thread.sleep(requestInterval);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  private void handleRequest() throws IOException {
    List<?> request = fetchRequest();
    if (request == null) {
      if (requestInterval < 50) {
        requestInterval = 50;
      } else {
        requestInterval = Math.min(MAX_REQUEST_INTERVAL, requestInterval * 2);
      }
      return;
    } else {
      requestInterval = 0;
    }
    try {
      Object response = processRequest(request);
      sendResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void sendResponse(Object response) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    String fetchRequestsUrl = cloudBaseUrl + responsePath.replaceAll("\\{projectId\\}", projectId);
    HttpPost httpPost = new HttpPost(fetchRequestsUrl);
    httpPost.addHeader("Authorization", token);

    String json = serialize(response);
    StringEntity entity = new StringEntity(json);
    httpPost.setEntity(entity);
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");
    client.execute(httpPost);
    client.close();
  }

  private <T> T deserialize(InputStream content) {
    return null; //TODO
  }

  private String serialize(Object response) {
    return null;//TODO
  }

  private Object processRequest(Object request) {
    // TODO
    return null;
  }

  private <T> T fetchRequest() throws IOException {
    if (cloudBaseUrl == null || projectId == null || token == null) {
      init();
      return null;
    }
    CloseableHttpClient client = HttpClients.createDefault();
    String fetchRequestsUrl = cloudBaseUrl + requestPath.replaceAll("\\{projectId\\}", projectId);
    HttpPost httpPost = new HttpPost(fetchRequestsUrl);

    httpPost.addHeader("Authorization", token);

    HttpResponse response = client.execute(httpPost);

    InputStream content = response.getEntity().getContent();
    StringBuilder builder = new StringBuilder();
    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
    String line;
    do {
      line = reader.readLine();
      if (line != null) {
        builder.append(line);
      }
    } while (line != null);
    content.close();
    client.close();

    return deserialize(content);
  }

  public void shutdown() {
    terminate = true;
  }
}
