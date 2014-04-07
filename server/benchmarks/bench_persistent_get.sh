#!/bin/sh
#
# Copyright (c) 1999-2014 Luca Garulli
#
ab -n1000 -A admin:admin -k -v -c10 http://127.0.0.1:2480/document/GratefulDeadConcerts/9:1
