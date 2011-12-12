package com.orientechnologies.common.thread;

public abstract class OPollerThread extends OSoftThread {
	protected final long	delay;

	public OPollerThread(final long iDelay) {
		delay = iDelay;
	}

	public OPollerThread(long iDelay, final ThreadGroup iThreadGroup) {
		super(iThreadGroup, OPollerThread.class.getSimpleName());
		delay = iDelay;
	}

	public OPollerThread(final long iDelay, final String name) {
		super(name);
		delay = iDelay;
	}

	public OPollerThread(final long iDelay, final ThreadGroup group, final String name) {
		super(group, name);
		delay = iDelay;
	}

	@Override
	protected void afterExecution() throws InterruptedException {
		running = pauseCurrentThread(delay);
	}
}
