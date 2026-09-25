#!/bin/sh
set -eu
test "$(cat /source/answer.txt)" = "violet"
