package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;

import java.io.IOException;
import java.util.Optional;

public interface WALSegment extends AutoCloseable {
    Optional<WriteableWALRecord> read(final OLogSequenceNumber lsn);
    Optional<WriteableWALRecord> next(final WriteableWALRecord record);
    Optional<WriteableWALRecord> next(final OLogSequenceNumber lsn);
    Optional<OLogSequenceNumber> begin();
    Optional<OLogSequenceNumber> end();
    Optional<Long> segmentIndex();

    void delete() throws IOException;
}
