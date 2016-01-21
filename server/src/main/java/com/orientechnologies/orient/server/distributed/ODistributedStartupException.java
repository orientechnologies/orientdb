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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.exception.OException;

/**
 * Exception thrown during distributed topology startup failure.
 *
 * @author Roberto Franchini (r.franchini--at--orientdb.com)
 */
public class ODistributedStartupException extends OException {
    private static final long serialVersionUID = 1L;

    public ODistributedStartupException() {
    }

    public ODistributedStartupException(String message, Throwable cause) {
        super(message, cause);
    }

    public ODistributedStartupException(String message) {
        super(message);
    }

    public ODistributedStartupException(Throwable cause) {
        super(cause);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || !obj.getClass().equals(getClass()))
            return false;

        String message = ((ODistributedStartupException) obj).getMessage();
        return (getMessage() == message) || (getMessage() != null && getMessage().equals(message));
    }

    @Override
    public int hashCode() {
        return getMessage() != null ? getMessage().hashCode() : 0;
    }

}
