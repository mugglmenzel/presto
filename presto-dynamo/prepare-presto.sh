#!/usr/bin/env bash

cd "$(dirname "$0")"
cd ..

mkdir -p dist/etc

cp -R defaults/ dist/etc/

cp -R presto-server/target/presto-server-*-SNAPSHOT/ dist/

cp presto-cli/target/presto-cli-*-SNAPSHOT-executable.jar dist/
mv dist/presto-cli-*-SNAPSHOT-executable.jar dist/presto
chmod +x dist/presto
