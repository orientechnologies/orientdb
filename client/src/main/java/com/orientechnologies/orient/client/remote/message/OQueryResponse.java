package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInfoExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 01/12/16. */
public class OQueryResponse implements OBinaryResponse {

  public static final byte RECORD_TYPE_BLOB = 0;
  public static final byte RECORD_TYPE_VERTEX = 1;
  public static final byte RECORD_TYPE_EDGE = 2;
  public static final byte RECORD_TYPE_ELEMENT = 3;
  public static final byte RECORD_TYPE_PROJECTION = 4;

  private String queryId;
  private boolean txChanges;
  private List<OResult> result;
  private Optional<OExecutionPlan> executionPlan;
  private boolean hasNextPage;
  private Map<String, Long> queryStats;
  private boolean reloadMetadata;

  public OQueryResponse(
      String queryId,
      boolean txChanges,
      List<OResult> result,
      Optional<OExecutionPlan> executionPlan,
      boolean hasNextPage,
      Map<String, Long> queryStats,
      boolean reloadMetadata) {
    this.queryId = queryId;
    this.txChanges = txChanges;
    this.result = result;
    this.executionPlan = executionPlan;
    this.hasNextPage = hasNextPage;
    this.queryStats = queryStats;
    this.reloadMetadata = reloadMetadata;
  }

  public OQueryResponse() {}

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeString(queryId);
    channel.writeBoolean(txChanges);
    writeExecutionPlan(executionPlan, channel, serializer);
    // THIS IS A PREFETCHED COLLECTION NOT YET HERE
    channel.writeInt(0);
    channel.writeInt(result.size());
    for (OResult res : result) {
      OMessageHelper.writeResult(res, channel, serializer);
    }
    channel.writeBoolean(hasNextPage);
    writeQueryStats(queryStats, channel);
    channel.writeBoolean(reloadMetadata);
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
    queryId = network.readString();
    txChanges = network.readBoolean();
    executionPlan = readExecutionPlan(network);
    // THIS IS A PREFETCHED COLLECTION NOT YET HERE
    int prefetched = network.readInt();
    int size = network.readInt();
    this.result = new ArrayList<>(size);
    while (size-- > 0) {
      result.add(OMessageHelper.readResult(network));
    }
    this.hasNextPage = network.readBoolean();
    this.queryStats = readQueryStats(network);
    reloadMetadata = network.readBoolean();
  }

  private void writeQueryStats(Map<String, Long> queryStats, OChannelDataOutput channel)
      throws IOException {
    if (queryStats == null) {
      channel.writeInt(0);
      return;
    }
    channel.writeInt(queryStats.size());
    for (Map.Entry<String, Long> entry : queryStats.entrySet()) {
      channel.writeString(entry.getKey());
      channel.writeLong(entry.getValue());
    }
  }

  private Map<String, Long> readQueryStats(OChannelDataInput channel) throws IOException {
    Map<String, Long> result = new HashMap<>();
    int size = channel.readInt();
    for (int i = 0; i < size; i++) {
      String key = channel.readString();
      Long val = channel.readLong();
      result.put(key, val);
    }
    return result;
  }

  private void writeExecutionPlan(
      Optional<OExecutionPlan> executionPlan,
      OChannelDataOutput channel,
      ORecordSerializer recordSerializer)
      throws IOException {
    if (executionPlan.isPresent()) {
      channel.writeBoolean(true);
      OMessageHelper.writeResult(executionPlan.get().toResult(), channel, recordSerializer);
    } else {
      channel.writeBoolean(false);
    }
  }

  private Optional<OExecutionPlan> readExecutionPlan(OChannelDataInput network) throws IOException {
    boolean present = network.readBoolean();
    if (!present) {
      return Optional.empty();
    }
    OResult read = OMessageHelper.readResult(network);
    return Optional.of(OInfoExecutionPlan.fromResult(read));
  }

  public String getQueryId() {
    return queryId;
  }

  public List<OResult> getResult() {
    return result;
  }

  public Optional<OExecutionPlan> getExecutionPlan() {
    return executionPlan;
  }

  public boolean isHasNextPage() {
    return hasNextPage;
  }

  public Map<String, Long> getQueryStats() {
    return queryStats;
  }

  public boolean isTxChanges() {
    return txChanges;
  }

  public boolean isReloadMetadata() {
    return reloadMetadata;
  }
}
