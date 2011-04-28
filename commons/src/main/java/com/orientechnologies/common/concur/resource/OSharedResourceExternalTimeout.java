package com.orientechnologies.common.concur.resource;

public class OSharedResourceExternalTimeout extends OSharedResourceTimeout {

	public OSharedResourceExternalTimeout(final int timeout) {
		super(timeout);
	}

	@Override
	public void acquireExclusiveLock() throws InterruptedException {
		super.acquireExclusiveLock();
	}

	@Override
	public void acquireSharedLock() throws InterruptedException {
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
