package com.orientechnologies.common.test;


public interface SpeedTest {
	public void cycle() throws Exception;

	public void init() throws Exception;

	public void deinit() throws Exception;

	public void beforeCycle() throws Exception;

	public void afterCycle() throws Exception;
}
