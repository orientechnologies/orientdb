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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.*;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.EmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public final class OWALRecordsFactory {
  private static final int RECORD_ID_OFFSET = 0;
  private static final int RECORD_ID_SIZE = 2;

  private static final int OPERATION_ID_OFFSET = RECORD_ID_OFFSET + RECORD_ID_SIZE;
  private static final int OPERATION_ID_SIZE = 4;

  private static final int ORIGINAL_CONTENT_SIZE_OFFSET = OPERATION_ID_OFFSET + OPERATION_ID_SIZE;
  private static final int ORIGINAL_CONTENT_SIZE = 4;

  private static final int METADATA_SIZE = RECORD_ID_SIZE + OPERATION_ID_SIZE;
  private static final int COMPRESSED_METADATA_SIZE =
      RECORD_ID_SIZE + OPERATION_ID_SIZE + ORIGINAL_CONTENT_SIZE;

  private final Map<Integer, Class<?>> idToTypeMap = new HashMap<>();

  public static final OWALRecordsFactory INSTANCE = new OWALRecordsFactory();

  private static final LZ4Factory factory = LZ4Factory.fastestInstance();
  private static final int MIN_COMPRESSED_RECORD_SIZE =
      OGlobalConfiguration.WAL_MIN_COMPRESSED_RECORD_SIZE.getValueAsInteger();

  public static ByteBuffer toStream(final WriteableWALRecord walRecord) {
    final int contentSize = walRecord.serializedSize() + METADATA_SIZE;

    final ByteBuffer content = ByteBuffer.allocate(contentSize).order(ByteOrder.nativeOrder());

    final int recordId = walRecord.getId();
    content.putShort(RECORD_ID_OFFSET, (short) recordId);
    content.position(METADATA_SIZE);

    walRecord.toStream(content);

    if (MIN_COMPRESSED_RECORD_SIZE <= 0 || contentSize < MIN_COMPRESSED_RECORD_SIZE) {
      return content;
    }

    final LZ4Compressor compressor = factory.fastCompressor();
    final int maxCompressedLength = compressor.maxCompressedLength(contentSize - 1);

    final ByteBuffer compressedContent =
        ByteBuffer.allocate(maxCompressedLength + COMPRESSED_METADATA_SIZE)
            .order(ByteOrder.nativeOrder());

    content.position(OPERATION_ID_OFFSET + OPERATION_ID_SIZE);
    compressedContent.position(COMPRESSED_METADATA_SIZE);
    final int compressedLength =
        compressor.compress(
            content,
            METADATA_SIZE,
            contentSize - METADATA_SIZE,
            compressedContent,
            COMPRESSED_METADATA_SIZE,
            maxCompressedLength);

    if (compressedLength + COMPRESSED_METADATA_SIZE < contentSize) {
      compressedContent.limit(compressedLength + COMPRESSED_METADATA_SIZE);
      compressedContent.putShort(RECORD_ID_OFFSET, (short) (-(recordId + 1)));
      compressedContent.putInt(ORIGINAL_CONTENT_SIZE_OFFSET, contentSize);

      return compressedContent;
    } else {
      return content;
    }
  }

  public WriteableWALRecord fromStream(byte[] content) {
    int recordId = OShortSerializer.INSTANCE.deserializeNative(content, RECORD_ID_OFFSET);

    if (recordId < 0) {
      final int originalLen =
          OIntegerSerializer.INSTANCE.deserializeNative(content, ORIGINAL_CONTENT_SIZE_OFFSET);
      final byte[] restored = new byte[originalLen];

      final LZ4FastDecompressor decompressor = factory.fastDecompressor();
      decompressor.decompress(
          content,
          COMPRESSED_METADATA_SIZE,
          restored,
          METADATA_SIZE,
          restored.length - METADATA_SIZE);
      recordId = -recordId - 1;
      content = restored;
    }

    final WriteableWALRecord walRecord = walRecordById(recordId);

    walRecord.fromStream(content, METADATA_SIZE);

    if (walRecord.getId() != recordId) {
      throw new IllegalStateException(
          "Deserialized WAL record id does not match to the serialized record id "
              + walRecord.getId()
              + " - "
              + content[0]);
    }

    return walRecord;
  }

  private WriteableWALRecord walRecordById(final int recordId) {
    final WriteableWALRecord walRecord;
    switch (recordId) {
      case UPDATE_PAGE_RECORD:
        walRecord = new OUpdatePageRecord();
        break;
      case HIGH_LEVEL_TRANSACTION_CHANGE_RECORD:
        walRecord = new OHighLevelTransactionChangeRecord();
        break;
      case ATOMIC_UNIT_START_RECORD:
        walRecord = new OAtomicUnitStartRecord();
        break;
      case ATOMIC_UNIT_START_METADATA_RECORD:
        walRecord = new OAtomicUnitStartMetadataRecord();
        break;
      case ATOMIC_UNIT_END_RECORD:
        walRecord = new OAtomicUnitEndRecord();
        break;
      case ATOMIC_UNIT_END_RECORD_WITH_PAGE_LSNS:
        walRecord = new AtomicUnitEndRecordWithPageLSNs();
        break;
      case FILE_CREATED_WAL_RECORD:
        walRecord = new OFileCreatedWALRecord();
        break;
      case NON_TX_OPERATION_PERFORMED_WAL_RECORD:
        walRecord = new ONonTxOperationPerformedWALRecord();
        break;
      case FILE_DELETED_WAL_RECORD:
        walRecord = new OFileDeletedWALRecord();
        break;
      case EMPTY_WAL_RECORD:
        walRecord = new EmptyWALRecord();
        break;
      case TX_METADATA:
        walRecord = new MetaDataRecord();
        break;
      default:
        if (idToTypeMap.containsKey(recordId))
          try {
            walRecord =
                (WriteableWALRecord)
                    idToTypeMap.get(recordId).getDeclaredConstructor().newInstance();
          } catch (final InstantiationException
              | NoSuchMethodException
              | InvocationTargetException
              | IllegalAccessException e) {
            throw new IllegalStateException("Cannot deserialize passed in record", e);
          }
        else throw new IllegalStateException("Cannot deserialize passed in wal record.");
    }
    return walRecord;
  }

  public void registerNewRecord(final int id, final Class<? extends WriteableWALRecord> type) {
    idToTypeMap.put(id, type);
  }
}
