#!/usr/bin/expect
set pass "6qweb03y64"

spawn curl -u wind@weblyzard https://api.weblyzard.com/0.2/token

expect "Enter host password for user 'wind@weblyzard':" { send "6qweb03y64\r" }
interact
