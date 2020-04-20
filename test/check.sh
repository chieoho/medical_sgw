#!/bin/bash

set -e
shopt -s expand_aliases

export CLASSPATH=~/tla

alias tlc="java tlc2.TLC"
alias tla2sany="java tla2sany.SANY"
alias pcal="java pcal.trans"
alias tla2tex="java tla2tex.TLA"

SPEC=$1
shift

grep -q -e "--algorithm" $SPEC.tla && pcal -nocfg $SPEC.tla | tee $SPEC.log
if grep -q -e "^\s*ProcessEnabled(self)\s*==" $SPEC.tla; then
	sed -i -e 's%pc\[self\] = ".*"$%& /\\\ ProcessEnabled(self)%' $SPEC.tla
fi
tlc -workers $(nproc) -cleanup $@ $SPEC.tla | tee -a $SPEC.log
