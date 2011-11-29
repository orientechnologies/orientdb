package com.orientechnologies.common.concur.resource;

import com.orientechnologies.common.concur.OTimeoutException;

public class OSharedResourceExternalTimeout extends OSharedResourceTimeout {

	public OSharedResourceExternalTimeout(final int timeout) {
		super(timeout);
	}

	@Override
	public void acquireExclusiveLock() throws OTimeoutException {
		super.acquireExclusiveLock();
	}

	@Override
	public void acquireSharedLock() throws OTimeoutException {
		super.acquireSharedLock();
	}

	@Override
	public void releaseExclusiveLock() {
		super.releaseExclusiveLock();
	}

	@Override
	public void releaseSharedLock() {
		super.releaseSharedLock();
	}
}
