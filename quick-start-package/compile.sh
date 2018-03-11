#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
mkdir -p build/release
cd build/release
cmake -DCMAKE_BUILD_TYPE=Release -DFORCE_TESTS=OFF ../..
make -j8
# g++ -I ./include -o ./build/release/harness harness.cpp -Ofast -march=native
# g++ -I ./include -o ./build/release/Query2SQL Query2SQL.cpp -Ofast -march=native
# g++ -I ./include/ functions.cpp Joiner.cpp Parser.cpp QueryGraph.cpp Relation.cpp treegen.cpp Utils.cpp -o ./build/release/Joiner -Ofast -march=native
