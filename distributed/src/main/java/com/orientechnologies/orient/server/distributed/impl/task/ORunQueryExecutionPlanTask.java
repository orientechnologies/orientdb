package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.result.binary.OResultSerializerNetwork;
import com.orientechnologies.orient.core.sql.executor.*;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Created by luigidellaquila on 23/06/17.
 */
public class ORunQueryExecutionPlanTask extends OAbstractRemoteTask {

  public static final int FACTORYID = 29;

  private OExecutionPlan      plan;
  private Map<Object, Object> inputParams;

  public ORunQueryExecutionPlanTask(OExecutionPlan executionPlan, Map<Object, Object> inputParameters) {
    this.plan = executionPlan;
    this.inputParams = inputParameters;
  }

  public ORunQueryExecutionPlanTask() {
  }

  @Override
  public String getName() {
    return "RunQueryExecutionPlan";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {

    ODatabaseDocumentInternal prev = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      ODatabaseDocumentInternal db = database.copy();
      db.activateOnCurrentThread();
      OLocalResultSetLifecycleDecorator result = ((ODatabaseDocumentEmbedded) db).query(plan, inputParams);

      //TODO implement serialization

      throw new UnsupportedOperationException();
    } finally {
      if (prev == null) {
        ODatabaseRecordThreadLocal.INSTANCE.remove();
      } else {
        ODatabaseRecordThreadLocal.INSTANCE.set(prev);
      }
    }
  }

  public OResultSet getResult(ODistributedResponse resp) {
    Object payload = resp.getPayload();
    //TODO implement deserialization

    throw new UnsupportedOperationException();
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    OResultSerializerNetwork serializerNetwork = new OResultSerializerNetwork();
    BytesContainer container = new BytesContainer();

    serializerNetwork.serialize(serializePlan(plan), container);

    OResultInternal params = new OResultInternal();
    params.setProperty("params", inputParams);

    container.fitBytes();
    out.write(container.bytes.length);
    out.write(container.bytes);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    OResultSerializerNetwork serializerNetwork = new OResultSerializerNetwork();
    int length = in.readInt();
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = in.readByte();
    }
    BytesContainer container = new BytesContainer(bytes);
    OResult serializedExecutionPlan = serializerNetwork.deserialize(container);
    inputParams = serializerNetwork.deserialize(container).getProperty("params");
    this.plan = deserializePlan(serializedExecutionPlan);
  }

  private OExecutionPlan deserializePlan(OResult serializedExecutionPlan) {
    String className = serializedExecutionPlan.getProperty(OInternalExecutionPlan.JAVA_TYPE);

    OInternalExecutionPlan internalPlan = null;
    try {
      internalPlan = (OInternalExecutionPlan) Class.forName(className).newInstance();
      internalPlan.deserialize(serializedExecutionPlan);
    } catch (Exception e) {
      e.printStackTrace();//TODO
      throw new ODistributedException("Cannot create execution plan: " + className);
    }
    return internalPlan;
  }

  private OResult serializePlan(OExecutionPlan plan) {
    return ((OInternalExecutionPlan) plan).serialize();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
