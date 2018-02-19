package com.orientechnologies.agent.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.OEnterpriseCloudManager;
import com.orientechnologies.agent.cloud.processor.CloudCommandProcessor;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.CommandType;

import java.io.IOException;
import java.net.ConnectException;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class CloudPushEndpoint extends Thread {

  ObjectMapper objectMapper = new ObjectMapper();

  private final OEnterpriseCloudManager cloudManager;
  private       boolean                 terminate;

  private long REQUEST_INTERVAL = 5000;//milliseconds TODO make it parametric or tunable

  private String projectId;
  private String cloudBaseUrl;

  private static String monitoringPath = "/api/v1/monitoring/collectStats/{projectId}";

  public CloudPushEndpoint(OEnterpriseCloudManager oEnterpriseAgent) {
    cloudManager = oEnterpriseAgent;
    init();
  }

  private void init() {

    projectId = OGlobalConfiguration.CLOUD_PROJECT_ID.getValue();
    cloudBaseUrl = OGlobalConfiguration.CLOUD_BASE_URL.getValue();

  }

  @Override
  public void run() {
    while (!terminate) {
      try {
        pushData();
      } catch (ConnectException e) {
        OLogManager.instance().debug(this, "Connection Refused");
      } catch (CloudException e) {
        OLogManager.instance().debug(this, "Error on api request", e);
      } catch (Exception e) {
        OLogManager.instance().warn(this, "Error handling request", e);
      }
      try {
        Thread.sleep(REQUEST_INTERVAL);
      } catch (InterruptedException e) {
      }
    }
  }

  private void pushData() throws Exception {
    if (projectId != null && cloudBaseUrl != null) {
      Command request = new Command();
      request.setCmd(CommandType.LIST_SERVERS.command);
      request.setResponseChannel(monitoringPath);

      CommandResponse response = processRequest(request);
      sendResponse(response);

    } else {
      init();//try to re-init for next round
    }
  }

  private void sendResponse(final CommandResponse response) throws IOException, ClassNotFoundException, CloudException {

    cloudManager.runWithToken((token) -> {
      String json = serialize(response.getPayload());
      cloudManager.post(response.getResponseChannel().replaceAll("\\{projectId\\}", projectId), "Bearer " + token, json);
      return null;
    });

  }

  private String serialize(Object response) throws JsonProcessingException {
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

  public void shutdown() {
    terminate = true;
  }
}
