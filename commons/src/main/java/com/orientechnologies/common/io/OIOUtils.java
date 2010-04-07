package com.orientechnologies.common.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class OIOUtils {
	public static byte[] toStream(Externalizable iSource) throws IOException {
		final ByteArrayOutputStream stream = new ByteArrayOutputStream();
		final ObjectOutputStream oos = new ObjectOutputStream(stream);
		iSource.writeExternal(oos);
		oos.flush();
		stream.flush();
		return stream.toByteArray();
	}

	public static Externalizable fromStream(byte[] iSource, Externalizable iDestination) throws IOException {
		try {
			iDestination.readExternal(new ObjectInputStream(new ByteArrayInputStream(iSource)));
		} catch (ClassNotFoundException e) {
			throw new IOException("Can't unmarshall source", e);
		}
		return iDestination;
	}
}
