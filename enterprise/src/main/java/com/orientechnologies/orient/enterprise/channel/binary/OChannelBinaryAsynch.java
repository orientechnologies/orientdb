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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.exception.ONetworkProtocolException;

/**
 * Implementation that supports multiple client requests.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OChannelBinaryAsynch extends OChannelBinary {
	private final OSoftThread													reader;
	private final List<OChannelBinaryAsynchRequester>	requestersQueue	= new ArrayList<OChannelBinaryAsynchRequester>();
	private final ReentrantLock												lockRead				= new ReentrantLock();
	private final ReentrantLock												lockWrite				= new ReentrantLock();
	private volatile boolean													responseParsed	= true;

	public OChannelBinaryAsynch(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		super(iSocket, iConfig);

		reader = new OSoftThread(Orient.getThreadGroup(), "IO-Binary-ChannelReaderMulti") {
			@Override
			protected void execute() throws Exception {
				if (!responseParsed || in == null) {
					// NOT READY
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
					}
					return;
				}

				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this, "Waiting for a response from server");

				// WAIT FOR A RESPONSE
				lockRead.lock();
				responseParsed = false;
				try {
					readStatus();
				} catch (IOException e) {
					close();
				} catch (Exception e) {
					e.printStackTrace();
					close();
				} finally {
					lockRead.unlock();
				}

			}
		};
		reader.start();
	}

	@Override
	public void close() {
		super.close();
		reader.sendShutdown();
	}

	public int readStatus(final OChannelBinaryAsynchRequester iRequester) throws IOException {
		// WAIT FOR THE RESPONSE
		final Object result;
		try {
			result = iRequester.getRequesterResponseQueue().take();

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Received response for txid %d: %s", iRequester.getRequesterId(), result);

		} catch (InterruptedException e) {
			close();
			throw new ONetworkProtocolException("Error on reading of the response from the Server", e);

		}

		lockRead.lock();
		responseParsed = true;

		if (result instanceof RuntimeException)
			throw (RuntimeException) result;

		return iRequester.getRequesterId();
	}

	public void addRequester(final OChannelBinaryAsynchRequester iRequester) {
		acquireExclusiveLock();
		try {
			requestersQueue.add(iRequester);
		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	protected void setRequestResult(final int iClientTxId, final Object iResult) {
		OChannelBinaryAsynchRequester reqId;

		acquireExclusiveLock();

		try {
			for (int i = 0; i < requestersQueue.size(); ++i) {
				reqId = requestersQueue.get(i);
				if (reqId.getRequesterId() == iClientTxId) {
					// FOUND: SET THE RESULT, REMOVE THE REQUESTER AND UNLOCK THE WAITER THREAD
					try {
						reqId.getRequesterResponseQueue().put(iResult);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					requestersQueue.remove(i);

					return;
				}
			}

			// CLIENT TX NOT FOUND: LOOSE THE REQUEST
			OLogManager.instance().warn(this, "Request %d not found in queue. Requests in queue %d: ", iClientTxId,
					requestersQueue.size(), requestersQueue.toString());

		} finally {
			releaseExclusiveLock();
		}
	}

	public ReentrantLock getLockRead() {
		return lockRead;
	}

	public ReentrantLock getLockWrite() {
		return lockWrite;
	}
}