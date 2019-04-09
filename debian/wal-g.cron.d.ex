#
# Regular cron jobs for the wal-g package
#
0 4	* * *	root	[ -x /usr/bin/wal-g_maintenance ] && /usr/bin/wal-g_maintenance
