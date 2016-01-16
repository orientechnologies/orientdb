package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 31/12/14
 */
public abstract class OOperationUnitBodyRecord extends OOperationUnitRecord {
	private OLogSequenceNumber startLsn;

	protected OOperationUnitBodyRecord() {
	}

	protected OOperationUnitBodyRecord(OOperationUnitId operationUnitId, OLogSequenceNumber startLsn) {
		super(operationUnitId);
		this.startLsn = startLsn;
	}

	public OLogSequenceNumber getStartLsn() {
		return startLsn;
	}

	@Override
	public int toStream(byte[] content, int offset) {
		offset = super.toStream(content, offset);

		OLongSerializer.INSTANCE.serializeNative(startLsn.getSegment(), content, offset);
		offset += OLongSerializer.LONG_SIZE;

		OLongSerializer.INSTANCE.serializeNative(startLsn.getPosition(), content, offset);
		offset += OLongSerializer.LONG_SIZE;

		return offset;
	}

	@Override
	public int fromStream(byte[] content, int offset) {
		offset = super.fromStream(content, offset);

		final long segment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
		offset += OLongSerializer.LONG_SIZE;

		final long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
		offset += OLongSerializer.LONG_SIZE;

		startLsn = new OLogSequenceNumber(segment, position);

		return offset;
	}

	@Override
	public int serializedSize() {
		return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
	}
}