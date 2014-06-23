#!/bin/sh
#
# Copyright (c) 1999-2010 Luca Garulli
#
ab -n100000 -A admin:admin -k -c10 -p post.txt http://127.0.0.1:2480/GratefulDeadConcerts/9:1
