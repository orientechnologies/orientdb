package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public final class JacksonSerializer implements ORecordSerializer {
    @Override
    public ORecord fromStream(byte[] iSource, ORecord iRecord, String[] iFields) {
        return null;
    }

    @Override
    public byte[] toStream(ORecord iSource) {
        return new byte[0];
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public int getMinSupportedVersion() {
        return 0;
    }

    @Override
    public String[] getFieldNames(ODocument reference, byte[] iSource) {
        return new String[0];
    }

    @Override
    public boolean getSupportBinaryEvaluate() {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }
}
