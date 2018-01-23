package com.orientechnologies.agent.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.OEnterpriseCloudManager;
import com.orientechnologies.agent.cloud.processor.CloudCommandProcessor;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;

import java.io.IOException;
import java.net.ConnectException;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class CloudEndpoint extends Thread {

  ObjectMapper objectMapper = new ObjectMapper();


  private final OEnterpriseCloudManager cloudManager;
  private       boolean                 terminate;
  private       long                    requestInterval;
  private long MAX_REQUEST_INTERVAL = 4000;//milliseconds TODO make it parametric or tunable

  private String token;
  private String projectId;
  private String cloudBaseUrl;

  private static String requestPath  = "/agent/commands/{projectId}";
  private static String responsePath = "/agent/commands/{projectId}/response";

  public CloudEndpoint(OEnterpriseCloudManager oEnterpriseAgent) {
    cloudManager = oEnterpriseAgent;
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
      } catch (ConnectException e) {
        OLogManager.instance().debug(this, "Connection Refused");
      } catch (CloudException e) {
        OLogManager.instance().debug(this, "Error on api request", e);
      } catch (Exception e) {
        OLogManager.instance().warn(this, "Error handling request", e);
      }
      if (requestInterval > 0) {
        try {
          Thread.sleep(requestInterval);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  private void handleRequest() throws IOException, ClassNotFoundException, CloudException {
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

  private void sendResponse(CommandResponse response) throws IOException, CloudException, ClassNotFoundException {

    cloudManager.runWithToken((token) -> {
      String json = serialize(response);
      cloudManager.post(responsePath.replaceAll("\\{projectId\\}", projectId), "Bearer " + token, json);
      return null;
    });

  }

  private Command deserializeRequest(String content) throws IOException, ClassNotFoundException {
    JsonNode tree = objectMapper.readTree(content);

    if (tree == null) {
      return null;
    }
    Command cmd = new Command();
    JsonNode id = tree.get("id");
    JsonNode command = tree.get("cmd");
    JsonNode responseChannel = tree.get("responseChannel");

    if (id == null || command == null || responseChannel == null) {
      OLogManager.instance().warn(this, "ERROR, invalid packet " + id + command + responseChannel);
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
    CloudCommandProcessor processor = cloudManager.getCommandFactory().getProcessorFor(request.getCmd());
    if (processor == null) {
      return commandNotSupported(request);
    }
    try {
      return processor.execute(request, this.cloudManager.getAgent());
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

  private Command fetchRequest() throws IOException, ClassNotFoundException, CloudException {
    if (cloudBaseUrl == null || projectId == null || token == null) {
      init();
      return null;
    }

    return cloudManager.runWithToken((token) -> {

      try {
        String payload = cloudManager.get(requestPath.replaceAll("\\{projectId\\}", projectId), "Bearer " + token);
        return deserializeRequest(payload);
      } catch (CloudException e) {
        if (e.getStatus() == 404) {
          return null;
        } else {
          throw e;
        }
      }
    });
  }

  public void shutdown() {
    terminate = true;
  }
}
