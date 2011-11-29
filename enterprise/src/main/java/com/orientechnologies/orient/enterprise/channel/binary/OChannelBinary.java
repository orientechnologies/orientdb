/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.OChannel;

public abstract class OChannelBinary extends OChannel {
	public DataInputStream	in;
	public DataOutputStream	out;
	private final int				maxChunkSize;
	private boolean					debug;
	private final byte[]		buffer;

	public OChannelBinary(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		super(iSocket, iConfig);

		maxChunkSize = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_MAX_CONTENT_LENGTH);
		debug = iConfig.getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_DEBUG);
		buffer = new byte[maxChunkSize];
	}

	public byte readByte() throws IOException {
		if (debug) {
			OLogManager.instance().debug(this, "Reading byte (1 byte)...");
			final byte value = in.readByte();
			OLogManager.instance().debug(this, "Read byte: " + (int) value);
			return value;
		}

		return in.readByte();
	}

	public int readInt() throws IOException {
		if (debug) {
			OLogManager.instance().debug(this, "Reading int (4 bytes)...");
			final int value = in.readInt();
			OLogManager.instance().debug(this, "Read int: " + value);
			return value;
		}

		return in.readInt();
	}

	public long readLong() throws IOException {
		if (debug) {
			OLogManager.instance().debug(this, "Reading long (8 bytes)...");
			final long value = in.readLong();
			OLogManager.instance().debug(this, "Read long: " + value);
			return value;
		}

		return in.readLong();
	}

	public short readShort() throws IOException {
		if (debug) {
			OLogManager.instance().debug(this, "Reading short (2 bytes)...");
			final short value = in.readShort();
			OLogManager.instance().debug(this, "Read short: " + value);
			return value;
		}

		return in.readShort();
	}

	public String readString() throws IOException {
		if (debug) {
			OLogManager.instance().debug(this, "Reading string (4+N bytes)...");
			byte[] buffer = readBytes();
			if (buffer == null)
				return null;
			String value = new String(buffer);
			OLogManager.instance().debug(this, "Read string: " + value);
			return value;
		}

		final byte[] buffer = readBytes();
		if (buffer == null)
			return null;

		return new String(buffer);
	}

	public byte[] readBytes() throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Reading chunk of bytes. Reading chunk length as int (4 bytes)...");

		final int len = in.readInt();

		if (debug)
			OLogManager.instance().debug(this, "Read chunk lenght: " + len);

		if (len < 0)
			return null;

		if (debug)
			OLogManager.instance().debug(this, "Reading " + len + " bytes...");

		final byte[] tmp = new byte[len];
		in.readFully(tmp);

		if (debug)
			OLogManager.instance().debug(this, "Read " + len + " bytes: " + new String(tmp));

		return tmp;
	}

	public List<String> readStringList() throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Reading string list. Reading string list items as int (4 bytes)...");

		final int items = in.readInt();

		if (debug)
			OLogManager.instance().debug(this, "Read string list items: " + items);

		if (items < 0)
			return null;

		List<String> result = new ArrayList<String>();
		for (int i = 0; i < items; ++i)
			result.add(readString());

		if (debug)
			OLogManager.instance().debug(this, "Read string list with %d items: ", items);

		return result;
	}

	public Set<String> readStringSet() throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Reading string set. Reading string set items as int (4 bytes)...");

		int items = in.readInt();

		if (debug)
			OLogManager.instance().debug(this, "Read string set items: " + items);

		if (items < 0)
			return null;

		Set<String> result = new HashSet<String>();
		for (int i = 0; i < items; ++i)
			result.add(readString());

		if (debug)
			OLogManager.instance().debug(this, "Read string set with %d items: ", items);

		return result;
	}

	public ORecordId readRID() throws IOException {
		return new ORecordId(readShort(), readLong());
	}

	public void writeByte(final byte iContent) throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Writing byte (1 byte): %d", iContent);

		out.write(iContent);
	}

	public void writeInt(final int iContent) throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Writing int (4 bytes): %d", iContent);

		out.writeInt(iContent);
	}

	public void writeLong(final long iContent) throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Writing long (8 bytes): %d", iContent);

		out.writeLong(iContent);
	}

	public void writeShort(final short iContent) throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Writing long (2 bytes): %d", iContent);

		out.writeShort(iContent);
	}

	public OChannelBinary writeString(final String iContent) throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Writing string (4+%d=%d bytes): %s", iContent != null ? iContent.length() : 0,
					iContent != null ? iContent.length() + 4 : 4, iContent);

		if (iContent == null)
			out.writeInt(-1);
		else
			writeBytes(iContent.getBytes());

		return this;
	}

	public OChannelBinary writeBytes(final byte[] iContent) throws IOException {
		return writeBytes(iContent, iContent != null ? iContent.length : 0);
	}

	public OChannelBinary writeBytes(final byte[] iContent, final int iLength) throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Writing bytes (4+%d=%d bytes): %s", iLength, iLength + 4, iContent);

		if (iContent == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(iLength);
			out.write(iContent, 0, iLength);
		}
		return this;
	}

	public OChannelBinary writeCollectionString(final Collection<String> iCollection) throws IOException {
		if (debug)
			OLogManager.instance().debug(this, "Writing strings (4+%d=%d items): %s", iCollection != null ? iCollection.size() : 0,
					iCollection != null ? iCollection.size() + 4 : 4, iCollection.toString());

		if (iCollection == null)
			writeInt(-1);
		else {
			writeInt(iCollection.size());

			for (String s : iCollection)
				writeString(s);
		}

		return this;
	}

	public void writeRID(final ORID iRID) throws IOException {
		writeShort((short) iRID.getClusterId());
		writeLong(iRID.getClusterPosition());
	}

	public void clearInput() throws IOException {
		if (in != null)
			while (in.available() > 0)
				in.read();
	}

	@Override
	public void flush() throws IOException {
		super.flush();
		out.flush();
	}

	@Override
	public void close() {
		try {
			acquireExclusiveLock();
		} catch (OTimeoutException e1) {
			return;
		}

		try {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
			}

			try {
				if (out != null)
					out.close();
			} catch (IOException e) {
			}

			super.close();

		} finally {
			releaseExclusiveLock();
		}
	}

	public int readStatus() throws IOException {
		// READ THE RESPONSE
		return handleStatus(readByte(), readInt());
	}

	protected int handleStatus(final byte iResult, final int iClientTxId) throws IOException {
		if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_OK || iResult == OChannelBinaryProtocol.PUSH_DATA) {
		} else if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {
			StringBuilder buffer = new StringBuilder();

			final List<OPair<String, String>> exceptions = new ArrayList<OPair<String, String>>();

			// EXCEPTION
			while (readByte() == 1) {
				final String excClassName = readString();
				final String excMessage = readString();
				exceptions.add(new OPair<String, String>(excClassName, excMessage));
			}

			Exception previous = null;
			for (int i = exceptions.size() - 1; i > -1; --i) {
				previous = createException(exceptions.get(i).getKey(), exceptions.get(i).getValue(), previous);
			}

			if (previous != null) {
				if (previous instanceof RuntimeException)
					throw (RuntimeException) previous;
				else
					throw new ODatabaseException("Generic error, see the underlying cause", previous);
			} else
				throw new ONetworkProtocolException("Network response error: " + buffer.toString());

		} else {
			// PROTOCOL ERROR
			// close();
			throw new ONetworkProtocolException("Error on reading response from the server");
		}
		return iClientTxId;
	}

	@SuppressWarnings("unchecked")
	private static RuntimeException createException(final String iClassName, final String iMessage, final Exception iPrevious) {
		RuntimeException rootException = null;
		Constructor<?> c = null;
		try {
			final Class<RuntimeException> excClass = (Class<RuntimeException>) Class.forName(iClassName);
			if (iPrevious != null) {
				try {
					c = excClass.getConstructor(String.class, Throwable.class);
				} catch (NoSuchMethodException e) {
					c = excClass.getConstructor(String.class, Exception.class);
				}
			}

			if (c == null)
				c = excClass.getConstructor(String.class);

		} catch (Exception e) {
			// UNABLE TO REPRODUCE THE SAME SERVER-SIZE EXCEPTION: THROW A STORAGE EXCEPTION
			rootException = new OStorageException(iMessage, iPrevious);
		}

		if (c != null)
			try {
				if (c.getParameterTypes().length > 1)
					rootException = (RuntimeException) c.newInstance(iMessage, iPrevious);
				else
					rootException = (RuntimeException) c.newInstance(iMessage);
			} catch (InstantiationException e) {
			} catch (IllegalAccessException e) {
			} catch (InvocationTargetException e) {
			}

		return rootException;
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public int getMaxChunkSize() {
		return maxChunkSize;
	}

}