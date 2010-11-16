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

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.exception.ONetworkProtocolException;

public class OChannelBinary extends OChannel {
	public DataInputStream	in;
	public DataOutputStream	out;

	public OChannelBinary(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		super(iSocket, iConfig);
	}

	public byte readByte() throws IOException {
		return in.readByte();
	}

	public int readInt() throws IOException {
		return in.readInt();
	}

	public long readLong() throws IOException {
		return in.readLong();
	}

	public byte[] readBytes() throws IOException {
		int len = in.readInt();

		if (len < 0)
			return null;

		byte[] tmp = new byte[len];
		in.readFully(tmp);
		return tmp;
	}

	public short readShort() throws IOException {
		return in.readShort();
	}

	public String readString() throws IOException {
		byte[] buffer = readBytes();
		if (buffer == null)
			return null;

		return new String(buffer);
	}

	public List<String> readStringList() throws IOException {
		int size = in.readInt();
		if (size < 0)
			return null;

		List<String> result = new ArrayList<String>();
		for (int i = 0; i < size; ++i)
			result.add(readString());

		return result;
	}

	public Set<String> readStringSet() throws IOException {
		int size = in.readInt();
		if (size < 0)
			return null;

		Set<String> result = new HashSet<String>();
		for (int i = 0; i < size; ++i)
			result.add(readString());

		return result;
	}

	public void writeByte(final byte iContent) throws IOException {
		out.write(iContent);
	}

	public void writeInt(final int iContent) throws IOException {
		out.writeInt(iContent);
	}

	public void writeLong(final long iContent) throws IOException {
		out.writeLong(iContent);
	}

	public void writeShort(final short iContent) throws IOException {
		out.writeShort(iContent);
	}

	public OChannelBinary writeString(final String iContent) throws IOException {
		if (iContent == null)
			out.writeInt(-1);
		else {
			byte[] temp = iContent.getBytes();
			out.writeInt(temp.length);
			out.write(temp);
		}
		return this;
	}

	public OChannelBinary writeBytes(final byte[] iContent) throws IOException {
		if (iContent == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(iContent.length);
			out.write(iContent);
		}
		return this;
	}

	public OChannelBinary writeCollectionString(final Collection<String> iCollection) throws IOException {
		if (iCollection == null)
			writeInt(-1);
		else {
			writeInt(iCollection.size());

			int i = 0;
			for (String s : iCollection) {
				writeString(s);
				i++;
			}
		}

		return this;
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
			in.close();
		} catch (IOException e) {
		}

		try {
			out.close();
		} catch (IOException e) {
		}

		super.close();
	}

	public int readStatus() throws IOException {
		flush();
		final byte result = readByte();

		// TODO: USE THIS TO ROUTE TO THE REQUESTER TX THREAD
		final int clientTxId = readInt();

		if (result == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {
			StringBuilder buffer = new StringBuilder();
			boolean moreDetails = false;
			String rootClassName = null;

			do {
				final String excClassName = readString();
				final String excMessage = readString();

				if (!moreDetails) {
					// FIRST ONE: TAKE AS ROOT CLASS/MSG
					rootClassName = excClassName;
				} else {
					// DETAIL: APPEND AS STRING SINCE EXCEPTIONS DON'T ALLOW TO BE REBUILT PROGRAMMATICALLY
					buffer.append("\n-> ");
					buffer.append(excClassName);
					buffer.append(": ");
				}
				buffer.append(excMessage);

				// READ IF MORE DETAILS ARE COMING
				moreDetails = readByte() == 1;

			} while (moreDetails);

			throw createException(rootClassName, buffer.toString());
		} else if (result != OChannelBinaryProtocol.RESPONSE_STATUS_OK) {
			// PROTOCOL ERROR
			clearInput();
			throw new ONetworkProtocolException("Error on reading response from the server");
		}

		return clientTxId;
	}

	@SuppressWarnings("unchecked")
	private static RuntimeException createException(final String iClassName, final String iMessage) {
		RuntimeException rootException = null;
		Constructor<?> c = null;
		try {
			final Class<RuntimeException> excClass = (Class<RuntimeException>) Class.forName(iClassName);
			c = excClass.getConstructor(String.class);
		} catch (Exception e) {
			// UNABLE TO REPRODUCE THE SAME SERVER-SIZE EXCEPTION: THROW A STORAGE EXCEPTION
			rootException = new OStorageException(iMessage, null);
		}

		if (c != null)
			try {
				rootException = (RuntimeException) c.newInstance(iMessage);
			} catch (InstantiationException e) {
			} catch (IllegalAccessException e) {
			} catch (InvocationTargetException e) {
			}

		return rootException;
	}

}