/*
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
package com.orientechnologies.orient.object.enhancement;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

/**
 * {@link OObjectFieldOTypeHandlingStrategy} that stores each {@link OType#BINARY} object split in several {@link ORecordBytes}.
 * 
 * Binary data optimization: http://orientdb.com/docs/2.2/Binary-Data.html
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class OObjectSplitRecordBytesOTypeHandlingStrategy implements OObjectFieldOTypeHandlingStrategy {

  private static final int DEFAULT_CHUNK_SIZE = 64;
  private static final int BYTES_PER_KB       = 1024;

  private final int        chunkSize;

  /**
   * Constuctor. Chunk size = {@value #DEFAULT_CHUNK_SIZE}
   */
  public OObjectSplitRecordBytesOTypeHandlingStrategy() {
    this(DEFAULT_CHUNK_SIZE);
  }

  /**
   * Constructor
   * 
   * @param chunkSizeInKb
   */
  public OObjectSplitRecordBytesOTypeHandlingStrategy(int chunkSizeInKb) {
    this.chunkSize = chunkSizeInKb * BYTES_PER_KB;
  }

  @Override
  public ODocument store(ODocument iRecord, String fieldName, Object fieldValue) {

    byte[] bytes = fieldValue != null ? (byte[]) fieldValue : null;
    ODatabaseDocumentInternal database = iRecord.getDatabase();
    List<ORID> chunks;

    // Delete the current data
    if ((chunks = iRecord.field(fieldName)) != null) {
      // There's already some binary data stored
      for (ORID oRid : chunks) {
        database.delete(oRid);
      }
      iRecord.removeField(fieldName);
    }

    // Store new data
    if (bytes != null) {
      database.declareIntent(new OIntentMassiveInsert());
      chunks = new ArrayList<ORID>();
      int offset = 0;
      int nextChunkLength = this.chunkSize;
      while (offset < bytes.length) {

        if (offset + nextChunkLength > bytes.length) {
          // last chunk, and it's smaller than the predefined chunk size
          nextChunkLength = bytes.length - offset;
        }

        chunks.add(database.save(new ORecordBytes(Arrays.copyOfRange(bytes, offset, offset + nextChunkLength))).getIdentity());
        offset += nextChunkLength;
      }

      iRecord.field(fieldName, chunks);
      database.declareIntent(null);
    }

    return iRecord;
  }

  @Override
  public Object load(ODocument iRecord, String fieldName) {

    iRecord.setLazyLoad(false);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ORecordBytes chunk;

    for (OIdentifiable id : (List<OIdentifiable>) iRecord.field(fieldName)) {
      chunk = (ORecordBytes) id.getRecord();
      try {
        chunk.toOutputStream(outputStream);
      } catch (IOException e) {
        throw OException.wrapException(new OSerializationException("Error loading binary field " + fieldName), e);
      }
      chunk.unload();
    }

    return outputStream.toByteArray();
  }

  @Override
  public OType getOType() {
    return OType.BINARY;
  }
}
