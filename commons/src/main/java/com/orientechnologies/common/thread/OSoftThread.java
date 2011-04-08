package com.orientechnologies.common.thread;

import com.orientechnologies.common.util.OService;

public abstract class OSoftThread extends Thread implements OService {
	protected volatile boolean	running	= true;

	public OSoftThread() {
	}

	public OSoftThread(final ThreadGroup iThreadGroup) {
		super(iThreadGroup, OSoftThread.class.getSimpleName());
		setDaemon(true);
	}

	public OSoftThread(final String name) {
		super(name);
		setDaemon(true);
	}

	public OSoftThread(final ThreadGroup group, final String name) {
		super(group, name);
		setDaemon(true);
	}

	protected abstract void execute() throws Exception;

	public void startup() {
		running = true;
	}

	public void shutdown() {
		running = false;
	}

	public void sendShutdown() {
		running = false;
		interrupt();
	}

	@Override
	public void run() {
		startup();

		while (running) {
			try {
				beforeExecution();
				execute();
				afterExecution();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

		shutdown();
	}

	public boolean isRunning() {
		return running;
	}

	/**
	 * Pauses current thread until iTime timeout or a wake up by another thread.
	 * 
	 * @param iTime
	 * @return true if timeout has reached, otherwise false. False is the case of wakeup by another thread.
	 */
	public static boolean pauseCurrentThread(long iTime) {
		try {
			if (iTime <= 0)
				iTime = Long.MAX_VALUE;

			Thread.sleep(iTime);
			return true;
		} catch (InterruptedException e) {
			return false;
		}
	}

	protected void beforeExecution() throws InterruptedException {
		return;
	}

	protected void afterExecution() throws InterruptedException {
		return;
	}
}
