package com.orientechnologies.agent.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class CloudEndpoint extends Thread {

  ObjectMapper objectMapper = new ObjectMapper();

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

  private void handleRequest() throws IOException, ClassNotFoundException {
    Command request = fetchRequest();
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
      CommandResponse response = processRequest(request);
      sendResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void sendResponse(CommandResponse response) throws IOException {
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

  private Command deserializeRequest(InputStream content) throws IOException, ClassNotFoundException {
    JsonNode tree = objectMapper.readTree(content);

    Command cmd = new Command();
    cmd.setId(tree.get("id").asText());
    cmd.setCmd(tree.get("cmd").asText());
    cmd.setPayloadClass(tree.get("payloadClass").asText());

    JsonNode payloadTree = tree.get("payload");
    Object payload = objectMapper.readValue(payloadTree.toString(), Class.forName(cmd.getPayloadClass()));
    cmd.setPayload(payload);

    cmd.setResponseChannel(tree.get("responseChannel").asText());

    return cmd;
  }

  private String serialize(CommandResponse response) throws JsonProcessingException {
    return objectMapper.writeValueAsString(response);
  }

  private CommandResponse processRequest(Command request) {
    // TODO


    return null;
  }

  private Command fetchRequest() throws IOException, ClassNotFoundException {
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

    return deserializeRequest(content);
  }

  public void shutdown() {
    terminate = true;
  }
}
