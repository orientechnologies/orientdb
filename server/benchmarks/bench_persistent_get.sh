#!/bin/sh
#
# Copyright (c) 1999-2014 Luca Garulli
#
ab -n10000 -A admin:admin -k -v -c16 http://127.0.0.1:2480/document/GratefulDeadConcerts/9:1
