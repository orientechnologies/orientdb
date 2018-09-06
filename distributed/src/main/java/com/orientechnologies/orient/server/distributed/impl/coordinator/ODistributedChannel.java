package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface ODistributedChannel {
  /**
   * Send submit request, that should go only to coordinator.
   *
   * @param request the request
   */
  void submit(OSubmitRequest request);

  /**
   * Send submit response, this is sent from the coordinator to the node that sent a submit request.
   *
   * @param response
   */
  void reply(OSubmitResponse response);

  /**
   * Send an operation to the node this is used by the coordinator to send operations of the distributed flow.
   *
   * @param id
   * @param nodeRequest
   */
  void sendRequest(OLogId id, ONodeRequest nodeRequest);

  /**
   * Send the response back to the coordinator, this is used by nodes to send a reply to a node request.
   *
   * @param id
   * @param nodeResponse
   */
  void sendResponse(OLogId id, ONodeResponse nodeResponse);
}
