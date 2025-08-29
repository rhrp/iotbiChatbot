#!/usr/bin/python3
LEVEL_ERROR=3
LEVEL_INFO=2
LEVEL_DEBUG=1
LEVEL_NONE=99

DEBUG_LEVEL=LEVEL_INFO
DEBUG_LEVELS={}
DEBUG_LEVELS[LEVEL_ERROR]="ERROR"
DEBUG_LEVELS[LEVEL_DEBUG]="DEBUG"
DEBUG_LEVELS[LEVEL_INFO]="INFO"

def debug(p_msg,p_level):
	if p_level>=DEBUG_LEVEL:
		td=DEBUG_LEVELS[p_level]
		from time import gmtime, strftime
		tm=strftime("%Y-%m-%d %H:%M:%S", gmtime())
		print(f"{tm} :: {__name__} :: {td} :: {p_msg}");
def debugError(p_msg):
	debug(p_msg,LEVEL_ERROR)
def debugDebug(p_msg):
	debug(p_msg,LEVEL_DEBUG)
def debugInfo(p_msg):
	debug(p_msg,LEVEL_INFO)
