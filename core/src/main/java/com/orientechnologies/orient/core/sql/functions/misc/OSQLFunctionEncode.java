/*
 * Copyright 2013 Geomatys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import java.util.Base64;

/**
 * Encode a string in various format (only base64 for now)
 *
 * @author Johann Sorel (Geomatys)
 */
public class OSQLFunctionEncode extends OSQLFunctionAbstract {

  public static final String NAME = "encode";
  public static final String FORMAT_BASE64 = "base64";

  /** Get the date at construction to have the same date for all the iteration. */
  public OSQLFunctionEncode() {
    super(NAME, 2, 2);
  }

  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {

    final Object candidate = iParams[0];
    final String format = iParams[1].toString();

    byte[] data = null;
    if (candidate instanceof byte[]) {
      data = (byte[]) candidate;
    } else if (candidate instanceof ORecordId) {
      final ORecord rec = ((ORecordId) candidate).getRecord();
      if (rec instanceof OBlob) {
        data = ((OBlob) rec).toStream();
      }
    } else if (candidate instanceof OSerializableStream) {
      data = ((OSerializableStream) candidate).toStream();
    }

    if (data == null) {
      return null;
    }

    if (FORMAT_BASE64.equalsIgnoreCase(format)) {
      return Base64.getEncoder().encodeToString(data);
    } else {
      throw new ODatabaseException("unknowned format :" + format);
    }
  }

  @Override
  public String getSyntax() {
    return "encode(<binaryfield>, <format>)";
  }
}
