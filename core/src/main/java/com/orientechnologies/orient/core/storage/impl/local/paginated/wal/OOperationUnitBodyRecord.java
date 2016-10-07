package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 31/12/14
 */
public abstract class OOperationUnitBodyRecord extends OOperationUnitRecord {
	protected OOperationUnitBodyRecord() {
	}

	protected OOperationUnitBodyRecord(OOperationUnitId operationUnitId) {
		super(operationUnitId);
	}


	@Override
	public int toStream(byte[] content, int offset) {
		offset = super.toStream(content, offset);

		return offset;
	}

	@Override
	public int fromStream(byte[] content, int offset) {
		offset = super.fromStream(content, offset);

		return offset;
	}

	@Override
	public int serializedSize() {
		return super.serializedSize();
	}
}