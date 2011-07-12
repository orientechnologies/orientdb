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
import java.util.concurrent.locks.ReentrantLock;

import com.orientechnologies.orient.core.config.OContextConfiguration;

/**
 * Implementation that supports multiple client requests.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OChannelBinaryAsynch extends OChannelBinary {
	private final ReentrantLock	lockRead		= new ReentrantLock();
	private final ReentrantLock	lockWrite		= new ReentrantLock();
	private boolean							channelRead	= false;
	private byte								currentStatus;
	private int									currentTxId;

	public OChannelBinaryAsynch(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		super(iSocket, iConfig);
	}

	public void beginRequest() {
		lockWrite.lock();
	}

	public void endRequest() throws IOException {
		out.flush();
		lockWrite.unlock();
	}

	public int beginResponse(final int iRequesterId) throws IOException {
		// WAIT FOR THE RESPONSE
		do {
			lockRead.lock();

			if (!channelRead) {
				channelRead = true;

				try {
					currentStatus = readByte();
					currentTxId = readInt();

				} catch (IOException e) {
					// UNLOCK THE RESOURCE AND PROPAGATES THE EXCEPTION
					lockRead.unlock();
					throw e;
				}
			}

			if (currentTxId == iRequesterId)
				// IT'S FOR ME
				break;

			lockRead.unlock();

			synchronized (this) {
				try {
					wait(1000);
				} catch (InterruptedException e) {
				}
			}
		} while (true);

		handleStatus(currentStatus, currentTxId);

		return iRequesterId;
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