/*
Copyright (c) 2024 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mosquitto_signal.h"

static void print_usage(void)
{
	printf("mosquitto_signal is a tool for sending control signals to mosquitto.\n");
	printf("                 it is primarily useful on Windows.\n");
	printf("                 on other systems the `kill` tool can be used.\n\n");
	printf("Usage: mosquitto_signal {-a | -p <pid>} <signal>\n");
	printf("       mosquitto_signal --help\n\n");
#ifdef WIN32
	printf(" -a :  signal all processes that match the name 'mosquitto.exe'.\n");
#else
	printf(" -a :  signal all processes that match the name 'mosquitto'.\n");
#endif
	printf(" -p :  specify a process ID to signal\n\n");
	printf("<signal> may be one of:\n");
	printf(" config-reload - reload the configuration file, if in use.\n");
	printf(" log-rotate    - if using `file` logging ask the broker to close and reopen the\n");
	printf("                 log file.\n");
	printf(" shutdown      - quit the broker.\n");
	printf(" tree-print    - (debug) print out subscription and retain tree information to\n");
	printf("                 stdout.\n");
	printf(" xtreport      - (debug) write internal data to xtmosquitto.kcg.<pid>.<iter>\n");
	printf("\nSee https://mosquitto.org/ for more information.\n\n");
}

int main(int argc, char *argv[])
{
	int idx;
	int pid = -2;
	int msig = 0;

	if(argc == 1){
		print_usage();
		return 1;
	}

	idx = 1;
	for(idx = 1; idx < argc; idx++){
		if(!strcmp(argv[idx], "--help")){
			print_usage();
			return 1;
		}else if(!strcmp(argv[idx], "-a")){
			pid = -1;
		}else if(!strcmp(argv[idx], "-p")){
			if(idx+1 == argc){
				fprintf(stderr, "Error: -p argument given but process ID missing.\n");
				return 1;
			}
			pid = atoi(argv[idx+1]);
			if(pid < 1){
				fprintf(stderr, "Error: Process ID must be >0.\n");
				return 1;
			}
			idx++;
		}else{
			break;
		}
	}
	if(pid == -2){
		fprintf(stderr, "Error: One of -a or -p must be used.\n");
		return 1;
	}
	if(idx == argc){
		fprintf(stderr, "Error: No signal given.\n");
		return 1;
	}
	if(!strcmp(argv[idx], "config-reload")){
		msig = MSIG_CONFIG_RELOAD;
	}else if(!strcmp(argv[idx], "log-rotate")){
		msig = MSIG_LOG_ROTATE;
	}else if(!strcmp(argv[idx], "shutdown")){
		msig = MSIG_SHUTDOWN;
	}else if(!strcmp(argv[idx], "tree-print")){
		msig = MSIG_TREE_PRINT;
	}else if(!strcmp(argv[idx], "xtreport")){
		msig = MSIG_XTREPORT;
	}else{
		fprintf(stderr, "Error: Unknown signal '%s'.\n", argv[idx]);
		return 1;
	}

	send_signal(pid, msig);

	return 0;
}
