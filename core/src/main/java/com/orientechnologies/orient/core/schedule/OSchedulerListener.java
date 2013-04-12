/*
 * Copyright 2010-2012 henryzhao81@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.schedule;

import java.util.Map;

/**
 * Author : henryzhao81@gmail.com Mar 28, 2013
 */

public interface OSchedulerListener {
	public enum SCHEDULER_STATUS {
	    RUNNING, STOPPED, WAITTING
	  }
	
	public void addScheduler(OScheduler scheduler);
	
	public void removeScheduler(OScheduler scheduler);
	
	public Map<String, OScheduler> getSchedulers();
	
	public OScheduler getScheduler(String name);
	
	public void load();
	
	public void close();
	
	public void create();
}
