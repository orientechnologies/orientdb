package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.profiler.OProfiler;

public class OLockException extends OException {
	private static final long	serialVersionUID	= 2215169397325875189L;

	public OLockException(String iMessage) {
		super(iMessage);
		OProfiler.getInstance().updateCounter("OLockException", +1);
	}

	public OLockException(String iMessage, Exception iException) {
		super(iMessage, iException);
		OProfiler.getInstance().updateCounter("OLockException", +1);
	}
}
