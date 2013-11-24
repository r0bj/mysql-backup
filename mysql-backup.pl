#!/usr/bin/perl

# Copyright (c) 2012-2013 Robert Jerzak
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

use strict;
use warnings;
use POSIX 'strftime';
use Sys::Hostname;
use File::stat;
use Time::Local;

use constant {
	SUCCESS => '1',
	FAIL => '0',
};

my $log_file = '/var/log/mysql-backup.log';
my $debian_cnf = '/etc/mysql/debian.cnf';
my $backup_root = '/var/mysql-backup/';
my $innobackupex = '/usr/bin/innobackupex';
my $zabbix_sender = '/usr/bin/zabbix_sender';
my $zabbix_agentd_conf = '/etc/zabbix/zabbix_agentd.conf';
my $zabbix_key = 'mysql.backup';
my $lock_file = '/var/run/mysql-backup.lock';

my $backup_retention = 7; # days
my $long_term_backups = 3; # items
my $long_term_backup_day = '01';
my $min_backups = 5; # minimum number of backups - do not remove backups if there are less than min_backups even if they are outdated
my $max_backups = 30; # maximum number of backups without long term backups; 0 - unlimited

my $lock_file_expired = 129600; # seconds
my $now_str = strftime ("%Y-%m-%d_%H-%M-%S", localtime);
my $now_timestamp = time ();
my $current_backup_dir = $backup_root . $now_str;
my $hostname = hostname ();
my $interactive = 0;
my $initial_sleep = 1800;
my $notify_zabbix = 1;
my $compress = 2; # 0 - no compression; 1 - gzip; 2 - xz
my $check_mountpoint = 0;
my $my_cnf = '';

$SIG{'TERM'} = \&sig_handler;
$SIG{'INT'} = \&sig_handler;

sub sig_handler {
	write_log ('TERM/INT signal caught, exiting');
	unlock ();
	exit;
}

sub write_log {
	my @txt = @_;

	open (FILE, ">>$log_file")
		or return undef;

	foreach my $line (@txt) {
		printf (FILE "%s: %s\n", strftime ("%Y%m%d %H%M%S", localtime ()), $line);
	}
	close (FILE);
}

sub check_mountpoint {
	my $root_path = shift;
	$root_path =~ s/\/$//;

	foreach my $line (`df`) {
		if ($line =~ /(\S+)\s+\S+\s+\S+\s+\S+\s+\S+\s+$root_path/) {
			return ($1);
		}
	}
	return (undef);
}

sub lock {
	if (-e $lock_file) {
		my $mtime = (stat ($lock_file))->[9];
		if (time () - $mtime > $lock_file_expired) {
			write_log ("WARNING: lock file $lock_file expired, ignoring");
			system ("touch $lock_file");
		}
		else {
			write_log ('WARNING: program is locked, exiting');
			exit;
		}
	}
	else {
		if (!open (FILE, ">$lock_file")) {
			write_log ("ERROR: cannot create file $lock_file");
			exit;
		}
		print (FILE "$$");
		close (FILE);
	}
}

sub unlock {
	if (-e $lock_file) {
		if (!unlink ($lock_file)) {
			write ("ERROR: cannot delete file $lock_file");
			exit;
		}
	}
}

sub notify_zabbix {
	my $value = shift;
	my $output = `$zabbix_sender -c $zabbix_agentd_conf -s $hostname -k $zabbix_key -o $value 2>&1`;
	write_log ('zabbix_sender:' . $output);
}

sub get_items_from_backup_root {
	my $backup_root = shift;
	my $items;

	opendir (my $dh, $backup_root);
	while (readdir $dh) {
		# gzip or xz compress
		if (/^((\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})\.tar.(?:gz|xz))/) {
			my ($year, $mon, $mday, $hour, $min, $sec) = ($2, $3, $4, $5, $6, $7);
			$mon -= 1;
			my $time = timelocal ($sec, $min, $hour, $mday, $mon, $year);
			$items->{$1} = {
				'timestamp' => $time,
				'type' => 'file',
			};
			if ($mday == $long_term_backup_day) {
				$items->{$1}->{'long_term_backup'} = $year . sprintf ("%.2d", $mon + 1);
			}
		}
		# uncompressed
		elsif (/^((\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2}))/) {
			my ($year, $mon, $mday, $hour, $min, $sec) = ($2, $3, $4, $5, $6, $7);
			$mon -= 1;
			my $time = timelocal ($sec, $min, $hour, $mday, $mon, $year);
			$items->{$1} = {
				'timestamp' => $time,
				'type' => 'dir',
			};
			if ($mday == $long_term_backup_day) {
				$items->{$1}->{'long_term_backup'} = $year . sprintf ("%.2d", $mon + 1);
			}
		}
	}
	close ($dh);
	$items = unmark_multiple_long_term_backups ($items);
	$items = mark_items ($items);
	return ($items);
}

sub mark_items {
	my $items = shift;

	# mark backups as "do not delete" due to "store minimum number of backups"
	my $ctr = 1;
	foreach my $item (sort { $items->{$b}->{'timestamp'} <=> $items->{$a}->{'timestamp'} } keys ($items)) {
		if ($item =~ /^$now_str\.tar/) {
			$items->{$item}->{'current'} = 1;
		} 
		if ($ctr <= $min_backups) {
			$items->{$item}->{'do_not_delete'} = 1;
		}
		if ($max_backups != 0 &&
			$ctr > $max_backups &&
			!defined ($items->{$item}->{'current'})) {
			$items->{$item}->{'force_delete'} = 1;
		}
		$ctr++;
	}
	return ($items);
}
	
sub unmark_multiple_long_term_backups {
	my $items = shift;

	# if there is more than one backup from day $long_term_backup_day choose first - only one long term backup per day
	my $tmp = 0;
	foreach my $item (sort {$items->{$a}->{'timestamp'} <=> $items->{$b}->{'timestamp'}} keys ($items)) {
		next if (!defined ($items->{$item}->{'long_term_backup'}));
		# remove 'long_term_backup' key
		if ($tmp eq $items->{$item}->{'long_term_backup'}) {
			delete ($items->{$item}->{'long_term_backup'});
		}
		# $item is long term backup - first from long term backup day $long_term_backup_day
		else {
			$tmp = $items->{$item}->{'long_term_backup'};
		}
	}
	return ($items);
}

sub rm_backups {
	my $items = shift;
	my $days = shift;

	# deleting backups
	foreach my $item (keys %$items) {
		if (!defined ($items->{$item}->{'long_term_backup'})) {
			if (($items->{$item}->{'timestamp'} =~ /^\d+$/ &&
				$now_timestamp - $items->{$item}->{'timestamp'} > 86400 * $days &&
				!defined ($items->{$item}->{'do_not_delete'})) ||
				defined ($items->{$item}->{'force_delete'})) {

				write_log ("INFO: deleting old backup $item");
				# compressed archives
				if ($items->{$item}->{'type'} eq 'file') {
					if (system ("rm -f ${backup_root}${item} 2>&1 >>$log_file") != 0) {
						write_log ("ERROR: cannot delete old backup $item");
						return (undef);
					}
				}
				# uncompressed backups
				elsif ($items->{$item}->{'type'} eq 'dir') {
					if (system ("rm -rf ${backup_root}${item} 2>&1 >>$log_file") != 0) {
						write_log ("ERROR: cannot delete old backup $item");
						return (undef);
					}
				}
			}
		}
	}
	return (1);
}

sub rm_long_term_backups {
	my $items = shift;
	my $long_term_days = shift;

	# deleting long term backups
	my $ctr = 1;
	foreach my $item (sort { $items->{$b}->{'timestamp'} <=> $items->{$a}->{'timestamp'} } keys ($items)) {
		if (defined ($items->{$item}->{'long_term_backup'})) {
			if (!defined ($items->{$item}->{'current'}) && $ctr > $long_term_backups) {
				write_log ("INFO: deleting old long term backup $item");
				# compressed archives
				if ($items->{$item}->{'type'} eq 'file') {
					if (system ("rm -f ${backup_root}${item} 2>&1 >>$log_file") != 0) {
						write_log ("ERROR: cannot delete old backup $item");
						return (undef);
					}
				}
				# uncompressed backups
				elsif ($items->{$item}->{'type'} eq 'dir') {
					if (system ("rm -rf ${backup_root}${item} 2>&1 >>$log_file") != 0) {
						write_log ("ERROR: cannot delete old backup $item");
						return (undef);
					}
				}
			}
			$ctr++;
		}
	}
	return (1);
}

sub delete_old_backups {
	my $backup_root = shift;
	my $days = shift;
	my $long_term_backups = shift;
	my $items;

	$items = get_items_from_backup_root ($backup_root);
	if (!rm_backups ($items, $days)) {
		return (undef);
	}
	if (!rm_long_term_backups ($items, $long_term_backups)) {
		return (undef);
	}
	return (1);
}

sub init_sleep {
	my $range = shift;
	sleep (int (rand ($range)));
}

sub get_mysql_credentials {
	my $debian_cnf = shift;
	my $conf;
	my $bracket;

	open (FILE, "<$debian_cnf");
	while (<FILE>) {
		$bracket = $1 if /^\[(.*)\]/;
		$conf->{$bracket}->{'user'} = $1 if /^user\s+=\s+([a-zA-Z0-9\._-]+)/;
		$conf->{$bracket}->{'password'} = $1 if /^password\s+=\s+([a-zA-Z0-9\._-]+)/;
	}
	close (FILE);

	if (defined $conf->{'client'}->{'user'} && defined $conf->{'client'}->{'password'}) {
		my $creds;
		$creds->{'user'} = $conf->{'client'}->{'user'};
		$creds->{'password'} = $conf->{'client'}->{'password'};
		return ($creds);
	}
	return (undef);
}

sub make_xtrabackup {
	my $creds = shift;

	my $cmd = "$innobackupex --user=$creds->{'user'} --password=$creds->{'password'} " . ($my_cnf ? "--defaults-file=$my_cnf " : '') . "--no-timestamp $current_backup_dir 2>&1 | tee -a $log_file";
	my $output = `$cmd`;

	if ($output =~ /\d{6}\s+\d{2}:\d{2}:\d{2}\s+innobackupex: completed OK!/) {
		if ($compress) {
			write_log ("INFO: starting $current_backup_dir commpression");
			my $cmd = "tar " . (($compress == 2) ? "cvpJf ${current_backup_dir}.tar.xz" : "cvpzf ${current_backup_dir}.tar.gz") . " -C $backup_root $now_str 2>&1 >>$log_file";
			if (system ($cmd) == 0) {
				write_log ("INFO: commpression ok, deleting current backup dir $current_backup_dir");
				if (system ("rm -rf $current_backup_dir 2>&1 >>$log_file") == 0) {
					#notify_zabbix (SUCCESS) if ($notify_zabbix);
					if (delete_old_backups ($backup_root, $backup_retention, $long_term_backups)) {
						notify_zabbix (SUCCESS) if ($notify_zabbix);
					}
					else {
						notify_zabbix (FAIL) if ($notify_zabbix);
					}
				}
				else {
					write_log ("ERROR: cannot delete $current_backup_dir");
					notify_zabbix (FAIL) if ($notify_zabbix);
				}
			}
			else {
				write_log ("ERROR: cannot compress $current_backup_dir");
				notify_zabbix (FAIL) if ($notify_zabbix);
			}
		}
		else {
			notify_zabbix (SUCCESS) if ($notify_zabbix);
			if (delete_old_backups ($backup_root, $backup_retention, $long_term_backups)) {
				notify_zabbix (SUCCESS) if ($notify_zabbix);
			}
			else {
				notify_zabbix (FAIL) if ($notify_zabbix);
			}
		}
	}
	else {
		write_log ("ERROR: cannot make backup, deleting failed backup dir $current_backup_dir");
		if (system ("rm -rf $current_backup_dir 2>&1 >>$log_file") != 0) {
			write_log ("ERROR: cannot delete failed backup dir $current_backup_dir");
		}
		notify_zabbix (FAIL) if ($notify_zabbix);
	}
}

## MAIN

if (! -d $backup_root) {
	write_log ("ERROR: backup root directory $backup_root does not exists");
	exit;
}

if ($check_mountpoint && !check_mountpoint ($backup_root)) {
	write_log ("ERROR: mount point for $backup_root does not exists");
	exit;
}

if (! -e $innobackupex) {
	write_log ("ERROR: $innobackupex does not exists");
	exit;
}

if ($min_backups >= $max_backups) {
	write_log ("ERROR: min_backups >= max_backups");
	exit;
}

my $creds = get_mysql_credentials ($debian_cnf);

if (!defined $creds->{'user'} || !defined $creds->{'password'}) {
	write_log ('ERROR: cannot find valid mysql credentials');
	exit;
}

lock ();

if (-t 1) {
	$interactive = 1;
	write_log ("INFO: interactive executing");
}
else {
	init_sleep ($initial_sleep);
}

make_xtrabackup ($creds);

unlock ();