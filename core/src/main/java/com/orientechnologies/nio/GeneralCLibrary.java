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

package com.orientechnologies.nio;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 11/20/13
 */
public class GeneralCLibrary implements CLibrary {
  public static native Pointer memmove(Pointer dest, Pointer src, NativeLong len);

  static {
    Native.register(Platform.C_LIBRARY_NAME);
  }

  @Override
  public void memoryMove(long src, long dest, long len) {
    memmove(new Pointer(dest), new Pointer(src), new NativeLong(len));
  }
}
