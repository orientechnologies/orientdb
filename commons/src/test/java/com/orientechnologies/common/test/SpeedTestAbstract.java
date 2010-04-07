package com.orientechnologies.common.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.testng.annotations.Test;

@Test
public abstract class SpeedTestAbstract implements SpeedTest {
	protected final SpeedTestData	data;

	protected SpeedTestAbstract() {
		data = new SpeedTestData();
	}

	protected SpeedTestAbstract(final long iCycles) {
		data = new SpeedTestData(iCycles);
	}

	protected SpeedTestAbstract(final SpeedTestGroup iGroup) {
		data = new SpeedTestData(iGroup);
	}

	public abstract void cycle() throws Exception;

	public void init() throws Exception {
	}

	public void deinit() throws Exception {
	}

	public void beforeCycle() throws Exception {
	}

	public void afterCycle() throws Exception {
	}

	@Test
	public void test() {
		data.go(this);
	}

	public SpeedTestAbstract config(final Object... iArgs) {
		data.configuration = iArgs;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.common.test.SpeedTest#executeCycle(java.lang.reflect.Method, java.lang.Object)
	 */
	public long executeCycle(final Method iMethod, final Object... iArgs) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		data.startTimer(getClass().getSimpleName());

		int percent = 0;
		for (data.cyclesDone = 0; data.cyclesDone < data.cycles; ++data.cyclesDone) {
			iMethod.invoke(this, iArgs);

			if (data.cycles > 10 && data.cyclesDone % (data.cycles / 10) == 0)
				System.out.print(++percent);
		}

		return data.takeTimer();
	}

	public SpeedTestData data() {
		return data;
	}
}
