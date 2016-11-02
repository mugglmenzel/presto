#!/usr/bin/env bash

cd "$(dirname "$0")"
cd ..

mkdir -p dist/etc

cp -R defaults/ dist/etc/

cp -R presto-server/target/presto-server-0.155-SNAPSHOT/ dist/

cp presto-cli/target/presto-cli-0.155-SNAPSHOT-executable.jar dist/
mv dist/presto-cli-0.155-SNAPSHOT-executable.jar dist/presto
chmod +x dist/presto