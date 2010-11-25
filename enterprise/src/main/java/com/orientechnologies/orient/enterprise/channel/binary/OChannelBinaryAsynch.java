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

	public int readStatus(final OChannelBinaryAsynchRequester iRequester) throws IOException {
		// WAIT FOR THE RESPONSE
		do {
			lockRead.lock();

			if (!channelRead) {
				currentStatus = readByte();
				currentTxId = readInt();
			}

			if (currentTxId == iRequester.getRequesterId())
				// IT'S FOR ME
				break;

			lockRead.unlock();

			synchronized (this) {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} while (true);

		handleStatus(currentStatus, currentTxId);

		return iRequester.getRequesterId();
	}

	public ReentrantLock getLockRead() {
		return lockRead;
	}

	public ReentrantLock getLockWrite() {
		return lockWrite;
	}
}