package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.DistributedQueryContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.result.binary.OResultSerializerNetwork;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.sql.executor.ODistributedResultSet;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Created by luigidellaquila on 23/06/17. */
public class ORunQueryExecutionPlanTask extends OAbstractRemoteTask {

  public static final int FACTORYID = 40;

  private String nodeName;
  private OExecutionPlan plan;
  private Map<Object, Object> inputParams;

  public ORunQueryExecutionPlanTask(
      OExecutionPlan executionPlan, Map<Object, Object> inputParameters, String nodeName) {
    this.plan = executionPlan;
    this.inputParams = inputParameters;
    this.nodeName = nodeName;
  }

  public ORunQueryExecutionPlanTask() {}

  @Override
  public String getName() {
    return "RunQueryExecutionPlan";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {

    ODatabaseDocumentInternal prev = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      ODatabaseDocumentInternal db = database.copy();
      db.activateOnCurrentThread();
      OLocalResultSetLifecycleDecorator result =
          ((ODatabaseDocumentEmbedded) db).query(plan, inputParams);

      DistributedQueryContext context = new DistributedQueryContext();
      context.setDb(db);
      context.setResultSet(result);
      context.setQueryId(String.valueOf(UUID.randomUUID()));

      ((OSharedContextEmbedded) db.getSharedContext())
          .getActiveDistributedQueries()
          .put(context.getQueryId(), context);
      OResultInternal serialized = new OResultInternal();

      serialized.setProperty("queryId", context.getQueryId());
      List<OResult> firstPage = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        if (!result.hasNext()) {
          break;
        }
        firstPage.add(result.next());
      }
      serialized.setProperty("data", firstPage);

      return serialized;
    } finally {
      if (prev == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(prev);
      }
    }
  }

  public OResultSet getResult(ODistributedResponse resp, ODatabaseDocumentDistributed db) {
    OResult payload = (OResult) resp.getPayload();
    ODistributedResultSet result = new ODistributedResultSet();
    result.setQueryId(payload.getProperty("queryId"));
    result.setData(payload.getProperty("data"));
    result.setDatabase(db);
    result.setNodeName(nodeName);
    return result;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    OResultSerializerNetwork serializerNetwork = new OResultSerializerNetwork();
    BytesContainer container = new BytesContainer();

    serializerNetwork.serialize(serializePlan(plan), container);

    OResultInternal params = new OResultInternal();
    params.setProperty("params", convertParams(inputParams));
    serializerNetwork.serialize(params, container);

    OResultInternal metadata = new OResultInternal();
    params.setProperty("nodeName", nodeName);
    serializerNetwork.serialize(metadata, container);

    container.fitBytes();
    out.write(container.bytes.length);
    out.write(container.bytes);
  }

  private Map<String, Object> convertParams(Map<Object, Object> inputParams) {
    Map<String, Object> result = new HashMap<>();
    for (Map.Entry<Object, Object> entry : inputParams.entrySet()) {
      result.put(String.valueOf(entry.getKey()), entry.getValue());
    }
    return result;
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
    OResult metadata = serializerNetwork.deserialize(container);
    nodeName = metadata.getProperty("nodeName");
    this.plan = deserializePlan(serializedExecutionPlan);
  }

  private OExecutionPlan deserializePlan(OResult serializedExecutionPlan) {
    String className = serializedExecutionPlan.getProperty(OInternalExecutionPlan.JAVA_TYPE);

    OInternalExecutionPlan internalPlan = null;
    try {
      internalPlan = (OInternalExecutionPlan) Class.forName(className).newInstance();
      internalPlan.deserialize(serializedExecutionPlan);
    } catch (Exception e) {
      throw OException.wrapException(
          new ODistributedException("Cannot create execution plan: " + className), e);
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
