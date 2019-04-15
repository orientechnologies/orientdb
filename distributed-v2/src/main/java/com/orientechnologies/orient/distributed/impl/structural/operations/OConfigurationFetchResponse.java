package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.distributed.impl.structural.OStructuralSharedConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.CONFIGURATION_FETCH_SUBMIT_REQUEST;

public class OConfigurationFetchResponse implements OStructuralSubmitResponse {

  private OStructuralSharedConfiguration sharedConfiguration;

  public OConfigurationFetchResponse(OStructuralSharedConfiguration sharedConfiguration) {
    this.sharedConfiguration = sharedConfiguration;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    sharedConfiguration.distributeSerialize(output);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    sharedConfiguration = new OStructuralSharedConfiguration();
    sharedConfiguration.distributeDeserialize(input);
  }

  public OStructuralSharedConfiguration getSharedConfiguration() {
    return sharedConfiguration;
  }

  @Override
  public int getResponseType() {
    return CONFIGURATION_FETCH_SUBMIT_REQUEST;
  }
}
