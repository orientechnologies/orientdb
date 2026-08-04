/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public final class OCommandRequest implements OBinaryRequest<OCommandResponse> {
  private static final OLogger logger = OLogManager.instance().logger(OCommandRequest.class);
  private static final String SCRIPT_COMMAND_CLASS = "s";
  private static final byte[] SCRIPT_COMMAND_CLASS_ASBYTES = SCRIPT_COMMAND_CLASS.getBytes();
  private static final String SQL_COMMAND_CLASS = "c";
  private static final byte[] SQL_COMMAND_CLASS_ASBYTES = SQL_COMMAND_CLASS.getBytes();
  private static final String QUERY_COMMAND_CLASS = "q";
  private static final byte[] QUERY_COMMAND_CLASS_ASBYTES = QUERY_COMMAND_CLASS.getBytes();
  private static final String LIVE_QUERY_COMMAND_CLASS =
      "com.orientechnologies.orient.core.sql.query.OLiveQuery";

  public class OQuery {

    private final String text;
    private final int limit;
    private final String fetchPlan;
    private final Map<Object, Object> parameters;
    private final ORID nextPageRID;
    private final Map<Object, Object> previousQueryParams;

    public OQuery(
        String text,
        int limit,
        String fetchPlan,
        Map<Object, Object> parameters,
        ORID nextPageRID,
        Map<Object, Object> previousQueryParams) {
      this.text = text;
      this.limit = limit;
      this.fetchPlan = fetchPlan;
      this.parameters = parameters;
      this.nextPageRID = nextPageRID;
      this.previousQueryParams = previousQueryParams;
    }

    public String getText() {
      return text;
    }

    public int getLimit() {
      return limit;
    }

    public String getFetchPlan() {
      return fetchPlan;
    }

    public Map<Object, Object> getParameters() {
      return parameters;
    }

    public ORID getNextPageRID() {
      return nextPageRID;
    }

    public Map<Object, Object> getPreviousQueryParams() {
      return previousQueryParams;
    }
  }

  public class OLQuery {

    private final String text;
    private final int limit;
    private final String fetchPlan;
    private final Map<Object, Object> parameters;

    public OLQuery(String text, int limit, String fetchPlan, Map<Object, Object> parameters) {
      this.text = text;
      this.limit = limit;
      this.fetchPlan = fetchPlan;
      this.parameters = parameters;
    }

    public String getText() {
      return text;
    }

    public int getLimit() {
      return limit;
    }

    public String getFetchPlan() {
      return fetchPlan;
    }

    public Map<Object, Object> getParameters() {
      return parameters;
    }
  }

  public class OCommand {
    private final String text;
    private final Map<Object, Object> parameters;

    public OCommand(String text, Map<Object, Object> parameters) {
      this.text = text;
      this.parameters = parameters;
    }

    public String getText() {
      return text;
    }

    public Map<Object, Object> getParameters() {
      return parameters;
    }
  }

  public class OScript {
    private final String language;
    private final DISTRIBUTED_EXECUTION_MODE executionMode;
    private final String text;
    private final Map<Object, Object> parameters;

    public OScript(
        String language,
        DISTRIBUTED_EXECUTION_MODE executionMode,
        String text,
        Map<Object, Object> parameters) {
      this.language = language;
      this.executionMode = executionMode;
      this.text = text;
      this.parameters = parameters;
    }

    public String getLanguage() {
      return language;
    }

    public DISTRIBUTED_EXECUTION_MODE getExecutionMode() {
      return executionMode;
    }

    public String getText() {
      return text;
    }

    public Map<Object, Object> getParameters() {
      return parameters;
    }
  }

  private ODatabaseDocumentInternal database;
  private boolean asynch;
  private boolean live;
  private OQuery queryD;
  private OCommand commandD;
  private OScript scriptD;
  private OLQuery lqueryD;

  public OCommandRequest() {}

  @Override
  public void write(OChannelDataOutput network) throws IOException {
    if (live) {
      network.writeByte((byte) 'l');
    } else {
      network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
    }
    network.writeBytes(toStream());
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {

    byte type = channel.readByte();
    if (type == (byte) 'l') live = true;
    if (type == (byte) 'a') asynch = true;
    fromStream(channel.readBytes(), serializer);
  }

  /**
   * Re-Create any object if the class has a public constructor that accepts a String as unique
   * parameter.
   */
  public void fromStream(final byte[] iStream, ORecordSerializer serializer) throws IOException {
    if (iStream == null || iStream.length == 0)
      // NULL VALUE
      return;

    final int classNameSize = OBinaryProtocol.bytes2int(iStream);

    if (classNameSize <= 0) {
      final String message =
          "Class signature not found in ANY element: " + Arrays.toString(iStream);
      logger.error("%s", null, message);

      throw new OSerializationException(message);
    }

    final String className = new String(iStream, 4, classNameSize, "UTF-8");
    byte[] commandData = OArrays.copyOfRange(iStream, 4 + classNameSize, iStream.length);
    final OMemoryStream buffer = new OMemoryStream(commandData);

    try {
      // CHECK FOR ALIASES
      if (className.equalsIgnoreCase(SCRIPT_COMMAND_CLASS)) {
        // QUERY
        this.queryD = queryFromStream(buffer, serializer);
      } else if (className.equalsIgnoreCase(SQL_COMMAND_CLASS)) {
        this.commandD = commandFromStream(buffer, serializer);
        // SQL COMMAND
      } else if (className.equalsIgnoreCase(SCRIPT_COMMAND_CLASS)) {
        this.scriptD = scriptFromStream(buffer, serializer);
        // SCRIPT COMMAND

      } else if (className.equalsIgnoreCase(LIVE_QUERY_COMMAND_CLASS)) {
        this.lqueryD = liveQueryFromStream(buffer, serializer);
        // SCRIPT COMMAND

      } else {
        throw new UnsupportedOperationException(
            " query with type " + className + " not supported anymore");
        // CREATE THE OBJECT BY INVOKING THE EMPTY CONSTRUCTOR
      }

    } catch (Exception e) {
      final String message = "Error on unmarshalling content. Class: " + className;
      logger.error(message, e);
      throw OException.wrapException(new OSerializationException(message), e);
    }
  }

  /** Serialize the class name size + class name + object content */
  public byte[] toStream() throws IOException {

    // SERIALIZE THE CLASS NAME
    byte[] className = null;
    OMemoryStream stream = new OMemoryStream();
    if (lqueryD != null) {
      try {
        className = LIVE_QUERY_COMMAND_CLASS.getBytes("UTF-8");
      } catch (Exception e) {
        final String message = "Error on unmarshalling content. Class: " + className;
        logger.error(message, e);
        throw OException.wrapException(new OSerializationException(message), e);
      }
      liveQueryToStream(stream, lqueryD);
    } else if (queryD != null) {
      className = QUERY_COMMAND_CLASS_ASBYTES;
      queryToStream(stream, queryD);
    } else if (commandD != null) {
      className = SQL_COMMAND_CLASS_ASBYTES;
      commandToStream(stream, commandD);
    } else if (scriptD != null) {
      className = SCRIPT_COMMAND_CLASS_ASBYTES;
      scriptToStream(stream, scriptD);
    }
    // SERIALIZE THE OBJECT CONTENT
    byte[] objectContent = stream.toByteArray();

    byte[] result = new byte[4 + className.length + objectContent.length];

    // COPY THE CLASS NAME SIZE + CLASS NAME + OBJECT CONTENT
    System.arraycopy(OBinaryProtocol.int2bytes(className.length), 0, result, 0, 4);
    System.arraycopy(className, 0, result, 4, className.length);
    System.arraycopy(objectContent, 0, result, 4 + className.length, objectContent.length);

    return result;
  }

  protected byte[] serializeQueryParameters(final Map<Object, Object> params) {
    if (params == null || params.size() == 0)
      // NO PARAMETER, JUST SEND 0
      return OCommonConst.EMPTY_BYTE_ARRAY;

    final ODocument param = new ODocument();
    param.field("params", convertToRIDsIfPossible(params));
    return param.toStream();
  }

  private void queryToStream(final OMemoryStream buffer, OQuery query) {

    buffer.setUtf8(query.text); // TEXT AS STRING
    buffer.set(query.limit); // LIMIT AS INTEGER
    buffer.setUtf8(query.fetchPlan != null ? query.fetchPlan : "");

    buffer.set(serializeQueryParameters(query.parameters));
    buffer.setUtf8(query.nextPageRID != null ? query.nextPageRID.toString() : "");

    final byte[] queryParams = serializeQueryParameters(query.previousQueryParams);
    buffer.set(queryParams);
  }

  protected OQuery queryFromStream(final OMemoryStream buffer, ORecordSerializer serializer) {
    String text = buffer.getAsString();
    int limit = buffer.getAsInteger();

    String fetchPlan = buffer.getAsString();
    ;

    final byte[] paramBuffer = buffer.getAsByteArray();
    Map<Object, Object> parameters = deserializeQueryParameters(paramBuffer, serializer);

    final String rid = buffer.getAsString();
    ORecordId nextPageRID;
    if ("".equals(rid)) nextPageRID = null;
    else nextPageRID = new ORecordId(rid);

    final byte[] serializedPrevParams = buffer.getAsByteArray();
    Map<Object, Object> previousQueryParams =
        deserializeQueryParameters(serializedPrevParams, serializer);
    return new OQuery(text, limit, fetchPlan, parameters, nextPageRID, previousQueryParams);
  }

  protected OLQuery liveQueryFromStream(final OMemoryStream buffer, ORecordSerializer serializer) {
    String text = buffer.getAsString();
    int limit = buffer.getAsInteger();

    String fetchPlan = buffer.getAsString();
    ;

    final byte[] paramBuffer = buffer.getAsByteArray();
    Map<Object, Object> parameters = deserializeQueryParameters(paramBuffer, serializer);

    return new OLQuery(text, limit, fetchPlan, parameters);
  }

  private void liveQueryToStream(final OMemoryStream buffer, OLQuery query) {

    buffer.setUtf8(query.text); // TEXT AS STRING
    buffer.set(query.limit); // LIMIT AS INTEGER
    buffer.setUtf8(query.fetchPlan != null ? query.fetchPlan : "");

    buffer.set(serializeQueryParameters(query.parameters));
  }

  protected Map<Object, Object> deserializeQueryParameters(
      final byte[] paramBuffer, ORecordSerializer serializer) {
    if (paramBuffer == null || paramBuffer.length == 0) return Collections.emptyMap();

    final ODocument param = new ODocument();

    serializer.fromStream(paramBuffer, param, null);
    param.setFieldType("params", OType.EMBEDDEDMAP);
    final Map<String, Object> params = param.rawField("params");

    final Map<Object, Object> result = new HashMap<Object, Object>();
    for (Entry<String, Object> p : params.entrySet()) {
      if (Character.isDigit(p.getKey().charAt(0)))
        result.put(Integer.parseInt(p.getKey()), p.getValue());
      else result.put(p.getKey(), p.getValue());
    }
    return result;
  }

  protected OScript scriptFromStream(final OMemoryStream buffer, ORecordSerializer serializer) {
    String language = buffer.getAsString();

    // FIX TO HANDLE USAGE OF EXECUTION MODE STARTING FROM v2.1.3
    final int currPosition = buffer.getPosition();
    final String value = buffer.getAsString();
    DISTRIBUTED_EXECUTION_MODE executionMode = null;
    try {
      executionMode = OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.valueOf(value);
    } catch (IllegalArgumentException ignore) {
      // OLD VERSION: RESET TO THE OLD POSITION
      buffer.setPosition(currPosition);
    }
    OCommand command = commandFromStream(buffer, serializer);
    return new OScript(language, executionMode, command.text, command.parameters);
  }

  protected OCommand commandFromStream(final OMemoryStream buffer, ORecordSerializer serializer) {
    String text = buffer.getAsString();

    Map<Object, Object> parameters = null;

    final boolean simpleParams = buffer.getAsBoolean();
    if (simpleParams) {
      final byte[] paramBuffer = buffer.getAsByteArray();
      final ODocument param = new ODocument();
      if (serializer != null) serializer.fromStream(paramBuffer, param, null);
      else param.fromStream(paramBuffer);

      Map<Object, Object> params = param.field("params");
      parameters = new HashMap<Object, Object>();
      if (params != null) {
        for (Entry<Object, Object> p : params.entrySet()) {
          final Object value;
          if (p.getValue() instanceof String)
            value = ORecordSerializerStringAbstract.getTypeValue((String) p.getValue());
          else value = p.getValue();

          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), value);
          else parameters.put(p.getKey(), value);
        }
      } else {
        params = param.field("parameters");
        for (Entry<Object, Object> p : params.entrySet()) {
          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), p.getValue());
          else parameters.put(p.getKey(), p.getValue());
        }
      }
    }

    final boolean compositeKeyParamsPresent = buffer.getAsBoolean();
    if (compositeKeyParamsPresent) {
      final byte[] paramBuffer = buffer.getAsByteArray();
      final ODocument param = new ODocument();
      if (serializer != null) serializer.fromStream(paramBuffer, param, null);
      else param.fromStream(paramBuffer);

      final Map<Object, Object> compositeKeyParams = param.field("compositeKeyParams");

      if (parameters == null) parameters = new HashMap<Object, Object>();

      for (final Entry<Object, Object> p : compositeKeyParams.entrySet()) {
        if (p.getValue() instanceof List) {
          final OCompositeKey compositeKey = new OCompositeKey((List<?>) p.getValue());
          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), compositeKey);
          else parameters.put(p.getKey(), compositeKey);

        } else {
          final Object value =
              OCompositeKeySerializer.INSTANCE.deserialize(
                  OStringSerializerHelper.getBinaryContent(p.getValue()), 0);

          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), value);
          else parameters.put(p.getKey(), value);
        }
      }
    }
    return new OCommand(text, parameters);
  }

  public void scriptToStream(final OMemoryStream buffer, OScript script)
      throws OSerializationException {
    buffer.setUtf8(script.language);
    buffer.setUtf8(script.executionMode.name());
    commandToStream(buffer, new OCommand(script.text, script.parameters));
  }

  protected void commandToStream(final OMemoryStream buffer, OCommand cmd) {
    buffer.setUtf8(cmd.text);

    if (cmd.parameters == null || cmd.parameters.size() == 0) {
      // simple params are absent
      buffer.set(false);
      // composite keys are absent
      buffer.set(false);
    } else {
      final Map<Object, Object> params = new HashMap<Object, Object>();
      final Map<Object, List<Object>> compositeKeyParams = new HashMap<Object, List<Object>>();

      for (final Entry<Object, Object> paramEntry : cmd.parameters.entrySet())
        if (paramEntry.getValue() instanceof OCompositeKey) {
          final OCompositeKey compositeKey = (OCompositeKey) paramEntry.getValue();
          compositeKeyParams.put(paramEntry.getKey(), compositeKey.getKeys());
        } else params.put(paramEntry.getKey(), paramEntry.getValue());

      buffer.set(!params.isEmpty());
      if (!params.isEmpty()) {
        final ODocument param = new ODocument();
        param.field("parameters", params);
        buffer.set(param.toStream());
      }

      buffer.set(!compositeKeyParams.isEmpty());
      if (!compositeKeyParams.isEmpty()) {
        final ODocument compositeKey = new ODocument();
        compositeKey.field("compositeKeyParams", compositeKeyParams);
        buffer.set(compositeKey.toStream());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Object> convertToRIDsIfPossible(final Map<Object, Object> params) {
    final Map<Object, Object> newParams = new HashMap<Object, Object>(params.size());

    for (Entry<Object, Object> entry : params.entrySet()) {
      final Object value = entry.getValue();

      if (value instanceof Set<?>
          && !((Set<?>) value).isEmpty()
          && ((Set<?>) value).iterator().next() instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final Set<ORID> newSet = new HashSet<ORID>();
        for (ORecord rec : (Set<ORecord>) value) {
          newSet.add(rec.getIdentity());
        }
        newParams.put(entry.getKey(), newSet);

      } else if (value instanceof List<?>
          && !((List<?>) value).isEmpty()
          && ((List<?>) value).get(0) instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final List<ORID> newList = new ArrayList<ORID>();
        for (ORecord rec : (List<ORecord>) value) {
          newList.add(rec.getIdentity());
        }
        newParams.put(entry.getKey(), newList);

      } else if (value instanceof Map<?, ?>
          && !((Map<?, ?>) value).isEmpty()
          && ((Map<?, ?>) value).values().iterator().next() instanceof ORecord) {
        // CONVERT RECORDS AS RIDS
        final Map<Object, ORID> newMap = new HashMap<Object, ORID>();
        for (Entry<?, ORecord> mapEntry : ((Map<?, ORecord>) value).entrySet()) {
          newMap.put(mapEntry.getKey(), mapEntry.getValue().getIdentity());
        }
        newParams.put(entry.getKey(), newMap);
      } else if (value instanceof OIdentifiable) {
        newParams.put(entry.getKey(), ((OIdentifiable) value).getIdentity());
      } else newParams.put(entry.getKey(), value);
    }

    return newParams;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_COMMAND;
  }

  public OCommand getQueryCommand() {
    return commandD;
  }

  public OScript getScript() {
    return scriptD;
  }

  public OLQuery getLiveQuery() {
    return lqueryD;
  }

  @Override
  public String getDescription() {
    return "Execute remote command";
  }

  public OQuery getQuery() {
    return queryD;
  }

  public boolean isAsynch() {
    return asynch;
  }

  public boolean isLive() {
    return live;
  }

  @Override
  public OCommandResponse createResponse() {
    return new OCommandResponse(database, live);
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCommand(this);
  }
}
