#!/bin/bash

function print_green {
    reset='\033[0m'
    green='\033[0;32m'
    echo -e "${green}$1${reset}"
}

clear

# compile
cd ../source && make 2>&1 > /dev/null

# run benchmarks
print_green "Threads in thread pool: 1"
echo $(./join 1 < ../inputfiles/inputfile_medium | tail -1)
print_green "Threads in thread pool: 2"
echo $(./join 2 < ../inputfiles/inputfile_medium | tail -1)
print_green "Threads in thread pool: 3"
echo $(./join 3 < ../inputfiles/inputfile_medium | tail -1)
print_green "Threads in thread pool: 4"
echo $(./join 4 < ../inputfiles/inputfile_medium | tail -1)

# clean
make clean 2>&1 > /dev/null