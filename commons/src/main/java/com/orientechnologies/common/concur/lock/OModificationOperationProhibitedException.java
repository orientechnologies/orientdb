package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OException;

/**
 * Exception is thrown in case DB is locked for modifications but modification request
 * ist trying to be acquired.
 *
 * @author Andrey Lomakin
 * @since 03.07.12
 */
public class OModificationOperationProhibitedException extends OException {
	public OModificationOperationProhibitedException() {
	}

	public OModificationOperationProhibitedException(String message) {
		super(message);
	}

	public OModificationOperationProhibitedException(Throwable cause) {
		super(cause);
	}

	public OModificationOperationProhibitedException(String message, Throwable cause) {
		super(message, cause);
	}
}
