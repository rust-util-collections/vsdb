#!/usr/bin/env bash

#################################################
#### Ensure we are in the right path. ###########
#################################################
if [[ 0 -eq $(echo $0 | grep -c '^/') ]]; then
    # relative path
    EXEC_PATH=$(dirname "`pwd`/$0")
else
    # absolute path
    EXEC_PATH=$(dirname "$0")
fi

EXEC_PATH=$(echo ${EXEC_PATH} | sed 's@/\./@/@g' | sed 's@/\.*$@@')
cd $EXEC_PATH || exit 1
#################################################

export LC_ALL=en_US.UTF-8 # perl
export LANGUAGE=en_US.UTF-8 # perl

for file in $(find .. -path "../target" -a -prune \
    -a -type f \
    -o -name "*.rs" \
    -o -name "*.c" \
    -o -name "*.h" \
    -o -name "*.sh" \
    -o -name "*.toml" \
    -o -name "*.json" \
    -o -name "*.md" \
    -o -name "rc.local" \
    | grep -v "$(basename $0)" \
    | grep -v '\.git' \
    | grep -v 'submodules' \
    | grep -v 'target' \
    | grep -v '\.tmp' \
    | grep -v '/debug/' \
    | grep -v '/release/' \
    | grep -iv 'Makefile' \
    | grep -iv 'LICENSE' \
    | grep -v 'tendermint'); do

    perl -pi -e 's/　/ /g' $file
    perl -pi -e 's/！/!/g' $file
    perl -pi -e 's/（/(/g' $file
    perl -pi -e 's/）/)/g' $file

    perl -pi -e 's/：/: /g' $file
    perl -pi -e 's/， */, /g' $file
    perl -pi -e 's/。 */. /g' $file
    perl -pi -e 's/、 +/、/g' $file

    perl -pi -e 's/, +(\S)/, $1/g' $file
    perl -pi -e 's/\. +(\S)/. $1/g' $file

    perl -pi -e 's/\t/    /g' $file
    echo $file | grep -c '\.md$'>/dev/null || perl -pi -e 's/ +$//g' $file
done

cargo +nightly fmt
