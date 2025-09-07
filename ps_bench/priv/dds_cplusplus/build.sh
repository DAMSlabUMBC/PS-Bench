export COMPILE_ROOT=`pwd`
export ERTS_ROOT="REPLACE_ERTS_ROOT"
export XERCES_ROOT="REPLACE_XERCES_ROOT"
make realclean
mwc.pl -type gnuace ps_bench_default_dds_interface.mwc
make
# Duplication here is NOT a typo, since the first iteration only builds the IDL 
# files, we have to run again to include the CPP files the second time.
# This can probably be fixed with better constructed MPC files, but works for now
mwc.pl -type gnuace ps_bench_default_dds_interface.mwc
make
