package com.orientechnologies.orient.core;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 20/11/14
 */
public interface OrientListener {
  public void onShutdown();
	public void onStartup();
}
