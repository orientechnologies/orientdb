package com.orientechnologies.common.exception;

public class OException extends RuntimeException {

	private static final long	serialVersionUID	= 3882447822497861424L;

	public OException() {
	}

	public OException(final String message) {
		super(message);
	}

	public OException(final Throwable cause) {
		super(cause);
	}

	public OException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
