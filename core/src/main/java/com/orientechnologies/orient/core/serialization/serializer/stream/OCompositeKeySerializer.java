package com.orientechnologies.orient.core.serialization.serializer.stream;

import java.io.IOException;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 29.07.11
 */
public class OCompositeKeySerializer implements OStreamSerializer {
	public static final String									NAME			= "cks";
	public static final OCompositeKeySerializer	INSTANCE	= new OCompositeKeySerializer();

	public byte[] toStream(final ODatabaseRecord iDatabase, final Object iObject) throws IOException {
		final OCompositeKey compositeKey = (OCompositeKey) iObject;

		final OMemoryStream outputStream = new OMemoryStream();
		outputStream.set(compositeKey.getKeys().size());
		for (final Comparable<?> comparable : compositeKey.getKeys()) {
			outputStream.set(OStreamSerializerLiteral.INSTANCE.toStream(iDatabase, comparable));
		}

		return outputStream.getInternalBuffer();
	}

	public Object fromStream(final ODatabaseRecord iDatabase, final byte[] iStream) throws IOException {
		final OCompositeKey compositeKey = new OCompositeKey();
		final OMemoryInputStream inputStream = new OMemoryInputStream(iStream);

		final int keysSize = inputStream.getAsInteger();
		for (int i = 0; i < keysSize; i++) {
			compositeKey.addKey((Comparable<?>) OStreamSerializerLiteral.INSTANCE.fromStream(iDatabase, inputStream.getAsByteArray()));
		}
		return compositeKey;
	}

	public String getName() {
		return NAME;
	}
}
