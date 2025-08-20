export COMPILE_ROOT=`pwd`
export ERTS_ROOT="/usr/lib/erlang/erts-15.2.7"
mwc.pl -type gnuace ps_bench_default_dds_interface.mwc
make realclean
make
