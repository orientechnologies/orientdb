package com.orientechnologies.common.concur.resource;

/**
 * Optimize locks since they are enabled only when the engine runs in MULTI-THREADS mode.
 * 
 * @author Luca Garulli
 * 
 */
public class OSharedResourceAdaptiveExternal extends OSharedResourceAdaptive {
	public OSharedResourceAdaptiveExternal(final boolean iConcurrent) {
		super(iConcurrent);
	}

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
