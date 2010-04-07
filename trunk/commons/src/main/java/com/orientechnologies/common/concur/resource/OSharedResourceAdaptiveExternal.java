package com.orientechnologies.common.concur.resource;

/**
 * Optimize locks since they are enabled only when the resources is really shared among 2 or more users.
 * 
 * @author Luca Garulli
 * 
 */
public class OSharedResourceAdaptiveExternal extends OSharedResourceAdaptive {
	@Override
	public boolean acquireExclusiveLock() {
		return super.acquireExclusiveLock();
	}

	@Override
	public boolean acquireSharedLock() {
		return super.acquireSharedLock();
	}

	@Override
	public void releaseExclusiveLock(final boolean iLocked) {
		super.releaseExclusiveLock(iLocked);
	}

	@Override
	public void releaseSharedLock(final boolean iLocked) {
		super.releaseSharedLock(iLocked);
	}
}
