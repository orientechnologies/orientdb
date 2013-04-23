SRC_DIR=src/main/native
SOURCES=$(SRC_DIR)/nativeos.c


OS=$(shell uname | sed -e 's/CYGWIN.*/win32/g' \
	                -e 's/MINGW32.*/win32/g' \
                        -e 's/SunOS.*/solaris/g' \
                        -e 's/NetBSD/netbsd/g' \
                        -e 's/FreeBSD/freebsd/g' \
                        -e 's/OpenBSD/openbsd/g' \
                        -e 's/Darwin.*/darwin/g' \
                        -e 's/AIX.*/aix/g' \
                        -e 's/Linux.*/linux/g')


ifeq ($(OS),win32)
LIB_NAME=nativeos.dll

CC_64=x86_64-w64-mingw32-gcc
CFLAGS_64=-m64 -mno-cygwin -Wall -O3 -Wl,--kill-at -shared
BUILD_DIR_64=src/main/resources/com/orientechnologies/nio/win32-amd64
LIBRARY_64=$(BUILD_DIR_64)/$(LIB_NAME)

CC_32=i686-w64-mingw32-gcc
CFLAGS_32=-m32 -mno-cygwin -Wall -O3 -Wl,--kill-at -shared
BUILD_DIR_32=src/main/resources/com/orientechnologies/nio/win32-x86
LIBRARY_32=$(BUILD_DIR_32)/$(LIB_NAME)
endif

ifeq ($(OS),linux)
LIB_NAME=nativeos.so

CC_64=gcc
CFLAGS_64=-m64 -Wall -O3 -shared
BUILD_DIR_64=src/main/resources/com/orientechnologies/nio/linux-amd64
LIBRARY_64=$(BUILD_DIR_64)/$(LIB_NAME)

CC_32=gcc
CFLAGS_32=-m32 -Wall -O3 -shared
BUILD_DIR_32=src/main/resources/com/orientechnologies/nio/linux-i386
LIBRARY_32=$(BUILD_DIR_32)/$(LIB_NAME)
endif


all:
	$(CC_64) $(CFLAGS_64) $(SOURCES) -o $(LIBRARY_64)
	$(CC_32) $(CFLAGS_32) $(SOURCES) -o $(LIBRARY_32)