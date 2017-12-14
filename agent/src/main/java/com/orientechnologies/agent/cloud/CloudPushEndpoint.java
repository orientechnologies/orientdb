package com.orientechnologies.agent.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.CloudCommandProcessor;
import com.orientechnologies.agent.cloud.processor.CloudCommandProcessorFactory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.CommandType;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class CloudPushEndpoint extends Thread {

  ObjectMapper objectMapper = new ObjectMapper();

  private final OEnterpriseAgent agent;
  private       boolean          terminate;

  private long REQUEST_INTERVAL = 5000;//milliseconds TODO make it parametric or tunable

  private String token;
  private String projectId;
  private String cloudBaseUrl;

  private static String monitoringPath = "/monitoring/collectStats/{projectId}";

  public CloudPushEndpoint(OEnterpriseAgent oEnterpriseAgent) {
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
        pushData();
      } catch (Exception e) {
        e.printStackTrace();
      }
      try {
        Thread.sleep(REQUEST_INTERVAL);
      } catch (InterruptedException e) {
      }
    }
  }

  private void pushData() {
    if (projectId != null && cloudBaseUrl != null) {
      try {
        Command request = new Command();
        request.setCmd(CommandType.LIST_SERVERS.command);
        request.setResponseChannel(monitoringPath);

        CommandResponse response = processRequest(request);
        sendResponse(response);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      init();//try to re-init for next round
    }
  }

  private void sendResponse(CommandResponse response) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    String fetchRequestsUrl = cloudBaseUrl + response.getResponseChannel().replaceAll("\\{projectId\\}", projectId);
    HttpPost httpPost = new HttpPost(fetchRequestsUrl);
    httpPost.addHeader("Authorization", "Bearer " + token);

    String json = serialize(response.getPayload());
    StringEntity entity = new StringEntity(json);
    httpPost.setEntity(entity);
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");
    CloseableHttpResponse r = client.execute(httpPost);

    client.close();
  }

  private String serialize(Object response) throws JsonProcessingException {
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

  public void shutdown() {
    terminate = true;
  }
}
