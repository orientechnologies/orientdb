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
package com.orientechnologies.orient.kv.network.protocol.memcache;

import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;

public class ONetworkProtocolMemcache extends ONetworkProtocolHttpAbstract {

	enum MemcacheCommand {
		GET, GETS, APPEND, PREPEND, DELETE, DECR, INCR, REPLACE, ADD, SET, CAS, STATS, VERSION, QUIT, FLUSH_ALL
	}

	public enum BinaryMemcacheCommand {
		Get(0x00, MemcacheCommand.GET, false), Set(0x01, MemcacheCommand.SET, false), Add(0x02, MemcacheCommand.ADD, false), Replace(
				0x03, MemcacheCommand.REPLACE, false), Delete(0x04, MemcacheCommand.DELETE, false), Increment(0x05, MemcacheCommand.INCR,
				false), Decrement(0x06, MemcacheCommand.DECR, false), Quit(0x07, MemcacheCommand.QUIT, false), Flush(0x08,
				MemcacheCommand.FLUSH_ALL, false), GetQ(0x09, MemcacheCommand.GET, false), Noop(0x0A, null, false), Version(0x0B,
				MemcacheCommand.VERSION, false), GetK(0x0C, MemcacheCommand.GET, false, true), GetKQ(0x0D, MemcacheCommand.GET, true, true), Append(
				0x0E, MemcacheCommand.APPEND, false), Prepend(0x0F, MemcacheCommand.PREPEND, false), Stat(0x10, MemcacheCommand.STATS,
				false), SetQ(0x11, MemcacheCommand.SET, true), AddQ(0x12, MemcacheCommand.ADD, true), ReplaceQ(0x13,
				MemcacheCommand.REPLACE, true), DeleteQ(0x14, MemcacheCommand.DELETE, true), IncrementQ(0x15, MemcacheCommand.INCR, true), DecrementQ(
				0x16, MemcacheCommand.DECR, true), QuitQ(0x17, MemcacheCommand.QUIT, true), FlushQ(0x18, MemcacheCommand.FLUSH_ALL, true), AppendQ(
				0x19, MemcacheCommand.APPEND, true), PrependQ(0x1A, MemcacheCommand.PREPEND, true);

		public byte							code;
		public MemcacheCommand	correspondingCommand;
		public boolean					noreply;
		public boolean					addKeyToResponse	= false;

		BinaryMemcacheCommand(int code, MemcacheCommand correspondingCommand, boolean noreply) {
			this.code = (byte) code;
			this.correspondingCommand = correspondingCommand;
			this.noreply = noreply;
		}

		BinaryMemcacheCommand(int code, MemcacheCommand correspondingCommand, boolean noreply, boolean addKeyToResponse) {
			this.code = (byte) code;
			this.correspondingCommand = correspondingCommand;
			this.noreply = noreply;
			this.addKeyToResponse = addKeyToResponse;
		}
	}
}
