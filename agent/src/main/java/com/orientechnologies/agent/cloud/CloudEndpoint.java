package com.orientechnologies.agent.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.CloudCommandProcessor;
import com.orientechnologies.agent.cloud.processor.CloudCommandProcessorFactory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class CloudEndpoint extends Thread {

  ObjectMapper objectMapper = new ObjectMapper();

  private final OEnterpriseAgent agent;
  private       boolean          terminate;
  private       long             requestInterval;
  private long MAX_REQUEST_INTERVAL = 4000;//milliseconds TODO make it parametric or tunable

  private String token;
  private String projectId;
  private String cloudBaseUrl;

  private static String requestPath  = "/agent/commands/{projectId}";
  private static String responsePath = "/agent/commands/{projectId}/response";

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
      if (requestInterval < 500) {
        requestInterval = 500;
      } else {
        requestInterval = Math.min(MAX_REQUEST_INTERVAL, requestInterval * 2);
      }
      return;
    } else {
      requestInterval /= 2;
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
    httpPost.addHeader("Authorization", "Bearer " + token);

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

    if (tree == null) {
      return null;
    }
    Command cmd = new Command();
    JsonNode id = tree.get("id");
    JsonNode command = tree.get("cmd");
    JsonNode responseChannel = tree.get("responseChannel");

    if (id == null || command == null || responseChannel == null) {
      System.out.println("ERROR, invalid packet " + id + command + responseChannel);
      return null;
    }

    JsonNode payloadClass = tree.get("payloadClass");
    cmd.setId(id.asText());
    cmd.setCmd(command.asText());

    cmd.setPayloadClass(payloadClass.asText());

    if (payloadClass != null) {
      JsonNode payloadTree = tree.get("payload");
      if (payloadTree != null && !payloadTree.isNull()) {
        Object payload = objectMapper.readValue(payloadTree.toString(), Class.forName(cmd.getPayloadClass()));
        cmd.setPayload(payload);
      }
    }
    cmd.setResponseChannel(responseChannel.asText());

    return cmd;
  }

  private String serialize(CommandResponse response) throws JsonProcessingException {
    return objectMapper.writeValueAsString(response);
  }

  private CommandResponse processRequest(Command request) {
    CloudCommandProcessor processor = CloudCommandProcessorFactory.INSTANCE.getProcessorFor(request.getCmd());
    if (processor == null) {
      return commandNotSupported(request);
    }
    try {
      return processor.execute(request, this.agent);
    } catch (Exception e) {
      return exceptionDuringExecution(request, e);
    }
  }

  private CommandResponse exceptionDuringExecution(Command cmd, Exception e) {
    CommandResponse result = new CommandResponse();
    result.setSuccess(false);
    result.setErrorCode(CommandResponse.ErrorCode.EXCEPTION_DURING_EXECUTION);
    result.setErrorMessage(
        "Exception during execution of command " + cmd.getCmd() + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
    result.setResponseChannel(cmd.getResponseChannel());
    result.setProjectId(OGlobalConfiguration.CLOUD_PROJECT_ID.getValueAsString());
    result.setId(cmd.getId());
    return result;
  }

  private CommandResponse commandNotSupported(Command cmd) {
    CommandResponse result = new CommandResponse();
    result.setSuccess(false);
    result.setErrorCode(CommandResponse.ErrorCode.COMMAND_NOT_SUPPORTED);
    result.setErrorMessage("Command not supported: " + cmd.getCmd());
    result.setResponseChannel(cmd.getResponseChannel());
    result.setProjectId(OGlobalConfiguration.CLOUD_PROJECT_ID.getValueAsString());
    result.setId(cmd.getId());
    return result;
  }

  private Command fetchRequest() throws IOException, ClassNotFoundException {
    if (cloudBaseUrl == null || projectId == null || token == null) {
      init();
      return null;
    }
    CloseableHttpClient client = HttpClients.createDefault();
    String fetchRequestsUrl = cloudBaseUrl + requestPath.replaceAll("\\{projectId\\}", projectId);
    HttpGet request = new HttpGet(fetchRequestsUrl);

    request.addHeader("Authorization", "Bearer " + token);

    HttpResponse response = client.execute(request);
    if (response.getStatusLine().getStatusCode() != 200) {
      System.out.println("Request Error: " + response.getStatusLine().getStatusCode());
      return null;
    }

    InputStream content = response.getEntity().getContent();
//    StringBuilder builder = new StringBuilder();
//    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
//    String line;
//    do {
//      line = reader.readLine();
//      if (line != null) {
//        builder.append(line);
//      }
//    } while (line != null);
//    content.close();
//    client.close();

    try {
      return deserializeRequest(content);
    } finally {
      client.close();
    }
  }

  public void shutdown() {
    terminate = true;
  }
}
