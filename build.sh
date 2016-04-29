#!/usr/bin/env bash

STUDIO=studio-2.2.zip

bower install
grunt build
cd dist/studio
zip -r ../../bin/$STUDIO plugin.json www
