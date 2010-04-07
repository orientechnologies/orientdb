package com.orientechnologies.common.concur.resource;

public class OSharedResourceExternal extends OSharedResource {

	@Override
	public void acquireExclusiveLock() {
		super.acquireExclusiveLock();
	}

	@Override
	public void acquireSharedLock() {
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
