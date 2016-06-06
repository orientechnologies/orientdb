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
package com.orientechnologies.orient.stresstest.util;

import com.orientechnologies.orient.stresstest.OMode;

import java.io.File;

/**
 * A class that contains all the info needed to access a database
 *
 * @author Andrea Iacono
 */
public class ODatabaseIdentifier {


    private final OMode mode;
    private final String remoteIp;
    private final int remotePort;
    private String dbName;
    private String rootPassword;

    public ODatabaseIdentifier(OMode mode, String dbName, String rootPassword, String remoteIp, int remotePort) {
        this.mode = mode;
        this.dbName = dbName;
        this.rootPassword = rootPassword;
        this.remotePort = remotePort;
        this.remoteIp = remoteIp;
    }

    public String getUrl() {

        switch (mode) {
            case MEMORY:
                return "memory:" + dbName;
            case REMOTE:
                return "remote:" + remoteIp + ":" + remotePort + "/" + dbName;
            case DISTRIBUTED:
                return null;
            case PLOCAL:
            default:
                return "plocal:" + System.getProperty("java.io.tmpdir") + File.separator + dbName;
        }
    }

    public OMode getMode() {
        return mode;
    }

    public String getPassword() {
        return rootPassword;
    }

    public String getName() {
        return dbName;
    }

    public void setPassword(String password) {
        this.rootPassword = password;
    }

    public String getRemoteIp() {
        return remoteIp;
    }

    public int getRemotePort() {
        return remotePort;
    }
}
