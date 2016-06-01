/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.stresstest.output;

/**
 * This class contains the execution times in ms of the operations for an executor (a single thread).
 *
 * @author Andrea Iacono
 */
public class OOperationsExecutorResults {

    private long createsTime;
    private long insertsTime;
    private long updatesTime;
    private long deletesTime;

    public OOperationsExecutorResults(long createsTime, long insertsTime, long updatesTime, long deletesTime) {
        this.createsTime = createsTime;
        this.insertsTime = insertsTime;
        this.updatesTime = updatesTime;
        this.deletesTime = deletesTime;
    }

    public long getCreatesTime() {
        return createsTime;
    }

    public long getReadsTime() {
        return insertsTime;
    }

    public long getUpdatesTime() {
        return updatesTime;
    }

    public long getDeletesTime() {
        return deletesTime;
    }
}
