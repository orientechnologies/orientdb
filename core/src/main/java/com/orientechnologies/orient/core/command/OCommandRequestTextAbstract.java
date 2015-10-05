/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Text based Command Request abstract class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("serial")
public abstract class OCommandRequestTextAbstract extends OCommandRequestAbstract implements OCommandRequestText {
  protected String text;

  protected OCommandRequestTextAbstract() {
  }

  protected OCommandRequestTextAbstract(final String iText) {
    if (iText == null)
      throw new IllegalArgumentException("Text cannot be null");

    text = iText.trim();
  }

  /**
   * Delegates the execution to the configured command executor.
   */
  @SuppressWarnings("unchecked")
  public <RET> RET execute(final Object... iArgs) {
    setParameters(iArgs);
    return (RET) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().command(this);
  }

  public String getText() {
    return text;
  }

  public OCommandRequestText setText(final String iText) {
    this.text = iText;
    return this;
  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    final OMemoryStream buffer = new OMemoryStream(iStream);
    fromStream(buffer);
    return this;
  }

  public byte[] toStream() throws OSerializationException {
    final OMemoryStream buffer = new OMemoryStream();
    return toStream(buffer);
  }

  @Override
  public String toString() {
    return "?." + text;
  }

  protected byte[] toStream(final OMemoryStream buffer) {
    buffer.setUtf8(text);

    if (parameters == null || parameters.size() == 0) {
      // simple params are absent
      buffer.set(false);
      // composite keys are absent
      buffer.set(false);
    } else {
      final Map<Object, Object> params = new HashMap<Object, Object>();
      final Map<Object, List<Object>> compositeKeyParams = new HashMap<Object, List<Object>>();

      for (final Entry<Object, Object> paramEntry : parameters.entrySet())
        if (paramEntry.getValue() instanceof OCompositeKey) {
          final OCompositeKey compositeKey = (OCompositeKey) paramEntry.getValue();
          compositeKeyParams.put(paramEntry.getKey(), compositeKey.getKeys());
        } else
          params.put(paramEntry.getKey(), paramEntry.getValue());

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

    return buffer.toByteArray();
  }

  protected void fromStream(final OMemoryStream buffer) {
    text = buffer.getAsString();

    parameters = null;

    final boolean simpleParams = buffer.getAsBoolean();
    if (simpleParams) {
      final byte[] paramBuffer = buffer.getAsByteArray();
      final ODocument param = new ODocument();
      param.fromStream(paramBuffer);

      Map<Object, Object> params = param.field("params");
      parameters = new HashMap<Object, Object>();
      if (params != null) {
        for (Entry<Object, Object> p : params.entrySet()) {
          final Object value;
          if (p.getValue() instanceof String)
            value = ORecordSerializerStringAbstract.getTypeValue((String) p.getValue());
          else
            value = p.getValue();

          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), value);
          else
            parameters.put(p.getKey(), value);
        }
      } else {
        params = param.field("parameters");
        for (Entry<Object, Object> p : params.entrySet()) {
          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), p.getValue());
          else
            parameters.put(p.getKey(), p.getValue());
        }
      }
    }

    final boolean compositeKeyParamsPresent = buffer.getAsBoolean();
    if (compositeKeyParamsPresent) {
      final byte[] paramBuffer = buffer.getAsByteArray();
      final ODocument param = new ODocument();
      param.fromStream(paramBuffer);

      final Map<Object, Object> compositeKeyParams = param.field("compositeKeyParams");

      if (parameters == null)
        parameters = new HashMap<Object, Object>();

      for (final Entry<Object, Object> p : compositeKeyParams.entrySet()) {
        if (p.getValue() instanceof List) {
          final OCompositeKey compositeKey = new OCompositeKey((List<?>) p.getValue());
          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), compositeKey);
          else
            parameters.put(p.getKey(), compositeKey);

        } else {
          final Object value = OCompositeKeySerializer.INSTANCE.deserialize(OStringSerializerHelper.getBinaryContent(p.getValue()),
              0);

          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0)))
            parameters.put(Integer.parseInt((String) p.getKey()), value);
          else
            parameters.put(p.getKey(), value);
        }
      }
    }
  }
}
