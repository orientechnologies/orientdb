package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.sql.executor.*;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 01/12/16.
 */
public class OQueryResponse implements OBinaryResponse {

  public static final byte RECORD_TYPE_BLOB       = 0;
  public static final byte RECORD_TYPE_VERTEX     = 1;
  public static final byte RECORD_TYPE_EDGE       = 2;
  public static final byte RECORD_TYPE_ELEMENT    = 3;
  public static final byte RECORD_TYPE_PROJECTION = 4;

  private OResultSet result;
  private boolean    txChanges;

  public OQueryResponse() {
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    if (!(result instanceof OLocalResultSetLifecycleDecorator)) {
      throw new IllegalStateException();
    }
    channel.writeString(((OLocalResultSetLifecycleDecorator) result).getQueryId());
    channel.writeBoolean(txChanges);

    writeExecutionPlan(result.getExecutionPlan(), channel, serializer);
    while (result.hasNext()) {
      OResult row = result.next();
      channel.writeBoolean(true);
      OMessageHelper.writeResult(row, channel, serializer);
    }
    channel.writeBoolean(false);
    channel.writeBoolean(((OLocalResultSetLifecycleDecorator) result).hasNextPage());
    writeQueryStats(result.getQueryStats(), channel);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    ORemoteResultSet rs = new ORemoteResultSet(
        (ODatabaseDocumentRemote) ODatabaseDocumentTxInternal.getInternal(ODatabaseRecordThreadLocal.INSTANCE.get()));
    doRead(network, rs);
    this.result = rs;
  }

  protected void doRead(OChannelDataInput network, ORemoteResultSet rs) throws IOException {
    rs.setQueryId(network.readString());
    txChanges = network.readBoolean();
    rs.setExecutionPlan(readExecutionPlan(network));
    boolean hasNext = network.readBoolean();
    while (hasNext) {
      OResult item = OMessageHelper.readResult(network);
      rs.add(item);
      hasNext = network.readBoolean();
    }
    rs.setHasNextPage(network.readBoolean());
    rs.setQueryStats(readQueryStats(network));
  }

  private void writeQueryStats(Map<String, Long> queryStats, OChannelDataOutput channel) throws IOException {
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

  private void writeExecutionPlan(Optional<OExecutionPlan> executionPlan, OChannelDataOutput channel,
      ORecordSerializer recordSerializer) throws IOException {
    if (executionPlan.isPresent()) {
      channel.writeBoolean(true);
      OMessageHelper.writeResult(executionPlan.get().toResult(), channel, recordSerializer);
    } else {
      channel.writeBoolean(false);
    }
  }

  private OExecutionPlan readExecutionPlan(OChannelDataInput network) throws IOException {
    boolean present = network.readBoolean();
    if (!present) {
      return null;
    }
    OInfoExecutionPlan result = new OInfoExecutionPlan();
    OResult read = OMessageHelper.readResult(network);
    result.setCost(((Number) read.getProperty("cost")).intValue());
    result.setType(read.getProperty("type"));
    result.setJavaType(read.getProperty("javaType"));
    result.setPrettyPrint(read.getProperty("prettyPrint"));
    result.setStmText(read.getProperty("stmText"));
    List<OResult> subSteps = read.getProperty("steps");
    if (subSteps != null) {
      subSteps.forEach(x -> result.getSteps().add(toInfoStep(x)));
    }
    return result;
  }

  private OExecutionStep toInfoStep(OResult x) {
    OInfoExecutionStep result = new OInfoExecutionStep();
    result.setName(x.getProperty("name"));
    result.setType(x.getProperty("type"));
    result.setTargetNode(x.getProperty("targetNode"));
    result.setJavaType(x.getProperty("javaType"));
    result.setCost(x.getProperty("cost") == null ? -1 : x.getProperty("cost"));
    List<OResult> ssteps = x.getProperty("subSteps");
    if (ssteps != null) {
      ssteps.stream().forEach(sstep -> result.getSubSteps().add(toInfoStep(sstep)));
    }
    result.setDescription(x.getProperty("description"));
    return result;
  }

  public void setResult(OLocalResultSetLifecycleDecorator result) {
    this.result = result;
  }

  public OResultSet getResult() {
    return result;
  }

  public boolean isTxChanges() {
    return txChanges;
  }

  public void setTxChanges(boolean txChanges) {
    this.txChanges = txChanges;
  }
}
