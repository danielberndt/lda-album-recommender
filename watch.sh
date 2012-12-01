#! /bin/sh

sass -w `dirname $0`/assets/sass:`dirname $0`/static/css &
coffee -o `dirname $0`/static/js -cw `dirname $0`/assets/coffee
