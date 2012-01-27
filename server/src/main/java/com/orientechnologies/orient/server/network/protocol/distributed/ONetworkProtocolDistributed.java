/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.network.protocol.distributed;

import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * Starting from 1.0rc8 the distributed and binary are different classes. This class is only used to maintain the compatibility with
 * old orientdb-server-config.xml files.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@Deprecated
public class ONetworkProtocolDistributed extends ONetworkProtocolBinary {
}
