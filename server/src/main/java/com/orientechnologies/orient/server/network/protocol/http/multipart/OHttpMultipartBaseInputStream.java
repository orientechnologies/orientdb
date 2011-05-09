/*
 *
 * Copyright 2011 Luca Molino (luca.molino--AT--assetdata.it)
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
package com.orientechnologies.orient.server.network.protocol.http.multipart;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author luca.molino
 * 
 */
public class OHttpMultipartBaseInputStream extends InputStream {

	protected StringBuilder	buffer;

	protected int						contentLength	= 0;

	protected InputStream		wrappedInputStream;

	public OHttpMultipartBaseInputStream(InputStream in, int iSkipInput, int iContentLength) {
		wrappedInputStream = in;
		contentLength = iContentLength;
		buffer = new StringBuilder();
		buffer.append((char) iSkipInput);
	}
	
	public OHttpMultipartBaseInputStream(InputStream in, char iSkipInput, int iContentLength) {
		wrappedInputStream = in;
		contentLength = iContentLength;
		buffer = new StringBuilder();
		buffer.append(iSkipInput);
	}

	public InputStream getWrappedInputStream() {
		return wrappedInputStream;
	}

	public void setSkipInput(int iSkipInput) {
		this.buffer.append((char) iSkipInput);
		contentLength++;
	}

	public void setSkipInput(StringBuilder iSkipInput) {
		this.buffer.append(iSkipInput);
		contentLength += iSkipInput.length();
	}

	@Override
	public synchronized int available() throws IOException {
		return contentLength;
	}

	@Override
	public synchronized int read() throws IOException {
		contentLength--;
		if (buffer.length() > 0) {
			final int returnValue = buffer.charAt(0);
			buffer.deleteCharAt(0);
			return returnValue;
		}
		return wrappedInputStream.read();
	}

	@Override
	public void close() throws IOException {
		wrappedInputStream.close();
	}

	@Override
	public synchronized void mark(int readlimit) {
		wrappedInputStream.mark(readlimit);
	}

	@Override
	public boolean markSupported() {
		return wrappedInputStream.markSupported();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return wrappedInputStream.read(b, off, len);
	}

	@Override
	public int read(byte[] b) throws IOException {
		return wrappedInputStream.read(b);
	}

	@Override
	public synchronized void reset() throws IOException {
		wrappedInputStream.reset();
	}

	@Override
	public long skip(long n) throws IOException {
		return wrappedInputStream.skip(n);
	}

	@Override
	public String toString() {
		return wrappedInputStream.toString();
	}

	public void resetBuffer() {
		buffer.setLength(0);
	}

	public int wrappedAvailable() throws IOException {
		return wrappedInputStream.available();
	}

}
