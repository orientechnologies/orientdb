package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import java.util.List;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;

/**
 * Serializer that is used for serialization of {@link OCompositeKey} keys in index.
 *
 * @author Andrey Lomakin
 * @since 29.07.11
 */
public class OCompositeKeySerializer implements OBinarySerializer<OCompositeKey> {


	public static final OCompositeKeySerializer	INSTANCE	= new OCompositeKeySerializer();
	public static final byte ID = 14;

	public int getObjectSize(OCompositeKey compositeKey) {
		final List<Comparable> keys = compositeKey.getKeys();

		int size = 2 * OIntegerSerializer.INT_SIZE;

		final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;
		for (final Comparable key : keys) {
			final OType type = OType.getTypeByClass(key.getClass());

			size += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE + factory.getObjectSerializer(type).getObjectSize(key);
		}

		return size;
	}

	public void serialize(OCompositeKey compositeKey, byte[] stream, int startPosition) {
		final List<Comparable> keys = compositeKey.getKeys();
		final int keysSize = keys.size();

		final int oldStartPosition = startPosition;

		startPosition += OIntegerSerializer.INT_SIZE;

		OIntegerSerializer.INSTANCE.serialize(keysSize, stream, startPosition);

		startPosition += OIntegerSerializer.INT_SIZE;

		final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;

		for (final Comparable key : keys) {
			final OType type = OType.getTypeByClass(key.getClass());
			OBinarySerializer binarySerializer = factory.getObjectSerializer(type);

			stream[startPosition] = binarySerializer.getId();
			startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;


			binarySerializer.serialize(key, stream, startPosition);
			startPosition += binarySerializer.getObjectSize(key);
		}

		OIntegerSerializer.INSTANCE.serialize((startPosition - oldStartPosition), stream, oldStartPosition);
	}

	public OCompositeKey deserialize(byte[] stream, int startPosition) {
		final OCompositeKey compositeKey = new OCompositeKey();

		startPosition += OIntegerSerializer.INT_SIZE;

		final int keysSize = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
		startPosition += OIntegerSerializer.INSTANCE.getObjectSize(keysSize);

		final OBinarySerializerFactory factory = OBinarySerializerFactory.INSTANCE;
		for (int i = 0; i < keysSize; i++) {
			final byte serializerId = stream[startPosition];
			startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

			OBinarySerializer binarySerializer = factory.getObjectSerializer(serializerId);
			final Comparable key = (Comparable)binarySerializer.deserialize(stream, startPosition);
			compositeKey.addKey(key);

			startPosition += binarySerializer.getObjectSize(key);
		}

		return compositeKey;
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
	}

	public byte getId() {
		return ID;
	}
}
