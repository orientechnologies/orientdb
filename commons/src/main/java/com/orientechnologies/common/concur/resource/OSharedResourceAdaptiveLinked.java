package com.orientechnologies.common.concur.resource;

/**
 * Optimize locks since they are enabled only when the resources is really shared among 2 or more users.
 * 
 * @author Luca Garulli
 * 
 */
public class OSharedResourceAdaptiveLinked extends OSharedResourceAbstract {
	private OSharedResourceAdaptive	source;

	public OSharedResourceAdaptiveLinked(OSharedResourceAdaptive source) {
		this.source = source;
	}

	@Override
	protected void acquireExclusiveLock() {
		if (source.getUsers() > 1)
			super.acquireExclusiveLock();
	}

	@Override
	protected void acquireSharedLock() {
		if (source.getUsers() > 1)
			super.acquireSharedLock();
	}

	@Override
	protected void releaseExclusiveLock() {
		if (source.getUsers() > 1)
			super.releaseExclusiveLock();
	}

	@Override
	protected void releaseSharedLock() {
		if (source.getUsers() > 1)
			super.releaseSharedLock();
	}
}
