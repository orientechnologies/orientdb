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

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;

/**
 * Implementation that supports multiple client requests.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OChannelBinaryAsynch extends OChannelBinary {
	private final ReentrantLock	lockRead							= new ReentrantLock();
	private final ReentrantLock	lockWrite							= new ReentrantLock();
	private boolean							channelRead						= false;
	private byte								currentStatus;
	private int									currentSessionId;

	private static final int		MAX_UNREAD_RESPONSES	= 5;
	private static final int		MAX_LENGTH_DEBUG			= 100;

	public OChannelBinaryAsynch(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		super(iSocket, iConfig);
	}

	public void beginRequest() {
		lockWrite.lock();
	}

	public void endRequest() throws IOException {
		flush();
		lockWrite.unlock();
	}

	public void beginResponse(final int iRequesterId) throws IOException {
		try {
			beginResponse(iRequesterId, 0);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// NEVER HAPPENS?
			e.printStackTrace();
		}
	}

	public void beginResponse(final int iRequesterId, final long iTimeout) throws IOException, InterruptedException {
		int unreadResponse = 0;
		// WAIT FOR THE RESPONSE
		do {
			if (iTimeout <= 0)
				lockRead.lock();
			else if (!lockRead.tryLock(iTimeout, TimeUnit.MILLISECONDS))
				throw new OTimeoutException("Cannot acquire read lock against channel: " + this);

			if (!channelRead) {
				channelRead = true;

				try {
					currentStatus = readByte();
					currentSessionId = readInt();

				} catch (IOException e) {
					// UNLOCK THE RESOURCE AND PROPAGATES THE EXCEPTION
					lockRead.unlock();
					channelRead = false;
					throw e;
				}
			}

			if (currentSessionId == iRequesterId)
				// IT'S FOR ME
				break;

			if (unreadResponse > MAX_UNREAD_RESPONSES) {
				final StringBuilder dirtyBuffer = new StringBuilder();
				int i = 0;
				while (in.available() > 0) {
					char c = (char) in.read();
					++i;

					if (dirtyBuffer.length() < MAX_LENGTH_DEBUG) {
						if (dirtyBuffer.length() > 0)
							dirtyBuffer.append('-');
						dirtyBuffer.append(c);
					}
				}

				OLogManager.instance().error(
						this,
						"Received unread response for session=" + currentSessionId
								+ ", probably corrupted data from the network connection. Cleared dirty data in the buffer (" + i + " bytes): ["
								+ dirtyBuffer + (i > dirtyBuffer.length() ? "..." : "") + "]", OIOException.class);
			}

			lockRead.unlock();

			// WAIT 1 SECOND AND RETRY
			synchronized (this) {
				try {
					final long start = System.currentTimeMillis();
					wait(1000);
					if (System.currentTimeMillis() - start >= 1000)
						unreadResponse++;

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		} while (true);

		handleStatus(currentStatus, currentSessionId);
	}

	public void endResponse() {
		channelRead = false;
		lockRead.unlock();

		// WAKE UP ALL THE WAITING THREADS
		synchronized (this) {
			notifyAll();
		}
	}

	public ReentrantLock getLockRead() {
		return lockRead;
	}

	public ReentrantLock getLockWrite() {
		return lockWrite;
	}

	@Override
	public void close() {
		synchronized (this) {
			notifyAll();
		}

		super.close();
	}

	@Override
	public void clearInput() throws IOException {
		lockRead.lock();
		try {
			super.clearInput();
		} finally {
			lockRead.unlock();
		}
	}
}