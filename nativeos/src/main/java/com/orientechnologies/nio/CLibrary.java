/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.nio;

import com.orientechnologies.common.log.OLogManager;
import com.sun.jna.Function;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * @author Andrey Lomakin
 * @since 5/6/13
 */
public class CLibrary {
  private static final Function memmove;
  private static final Function bcopy;

  static {
    Function memmoveFc;
    try {
      memmoveFc = Function.getFunction(Platform.C_LIBRARY_NAME, "memmove");
    } catch (UnsatisfiedLinkError linkError) {
      memmoveFc = null;
    }

    Function bcopyFc;
    try {
      bcopyFc = Function.getFunction(Platform.C_LIBRARY_NAME, "bcopy");
    } catch (UnsatisfiedLinkError linkError) {
      bcopyFc = null;
    }

    memmove = memmoveFc;
    bcopy = bcopyFc;
    OLogManager.instance().debug(CLibrary.class, "Following c library functions were found memmove : %s , bcopy : %s.",
        memmoveFc != null ? "yes" : "no", bcopyFc != null ? "yes" : "no");
  }

  public static void memoryMove(long src, long dest, long len) {
    final Pointer srcPointer = new Pointer(src);
    final Pointer destPointer = new Pointer(dest);

    if (memmove != null)
      memmove.invoke(Pointer.class, new Object[] { destPointer, srcPointer, new NativeLong(len) });
    else if (bcopy != null)
      bcopy.invokeVoid(new Object[] { srcPointer, destPointer, new NativeLong(len) });
    else {
      if (src > dest)
        for (long n = 0; n < len; n++)
          destPointer.setByte(n, srcPointer.getByte(n));
      else
        for (long n = len - 1; n >= 0; n--)
          destPointer.setByte(n, srcPointer.getByte(n));
    }
  }
}
