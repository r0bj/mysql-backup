#!/usr/bin/perl

# Copyright (c) 2012-2014 Robert Jerzak
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

# Requirements for compression:
# - lbzip2 (parallel bzip2)
# - pigz (parallel gzip)
# - pxz (parallel xz)

use strict;
use warnings;
use POSIX 'strftime';
use Sys::Hostname;
use Time::Local;
use Config::Simple;
use File::Temp ':mktemp';
use LWP::UserAgent;

use constant {
	SUCCESS => '1',
	FAIL => '0',
};

my %conf = (
	'logfile' => '/var/log/mysql-backup.log',
	'client_auth_file' => '/etc/mysql/debian.cnf',
	'backup_root' => '/var/mysql-backup/',
	'innobackupex' => '/usr/bin/innobackupex',
	'zabbix_sender' => '/usr/bin/zabbix_sender',
	'zabbix_agentd_conf' => '/etc/zabbix/zabbix_agentd.conf', # discarded if zabbix_server defined
	'zabbix_server' => '', # if set zabbix_agentd_conf is discarded
	'zabbix_key' => 'mysql.backup',
	'zabbix_key_duration' => 'mysql.backup.duration',
	'lock_file' => '/var/run/mysql-backup.lock',
	'my_cnf' => '', # if empty use default /etc/mysql/my.cnf
	'mysql_user' => '',
	'mysql_passwd' => '',
	'mysql_host' => '127.0.0.1',
	'influxdb_url' => '',
	'influxdb_database' => '',
	'influxdb_measurement' => '',
	'influxdb_connection_timeout' => 10,
	'pushgateway_url' => '',
	'pushgateway_connection_timeout' => 10,
	'prometheus_job' => 'mysql-backup',

	'backup_retention' => 7, # days
	'long_term_backups' => 3, # items
	'long_term_backup_day' => '01',
	'min_backups' => 3, # minimum number of backups - do not remove backups if there are less than min_backups even if they are outdated
	'max_backups' => 30, # maximum number of backups without long term backups; 0 - unlimited
	'notify_zabbix' => 0,
	'compress' => 0, # 0 - no compression; 1 - qpress (internal xtrabackup compression); 2 - gzip; 3 - bzip2; 4 - xz
	'check_mountpoint' => 0,
	'lock_file_expiration' => 129600, # seconds
	'initial_sleep' => 600, # seconds

	'parallel' => 1, # number of threads to use for parallel datafiles transfer, does not have any effect in the stream mode
	'compress-threads' => 1, # number of threads for parallel data compression (qpress)
	'stream' => 1, # 0 - no stream compression, better suitable for local backup, 1 - stream compression, local and remote (eg. sshfs) backups
);

my $start_timestamp_str;
my $start_timestamp;
my $hostname = hostname ();
my ($tmpfile_fh, $tmp_logfile) = mkstemp ('/tmp/mysql-backup.XXXXXX');
my $influxdb_annotation;

$SIG{'TERM'} = \&sig_handler;
$SIG{'INT'} = \&sig_handler;

sub sig_handler {
	write_log ('TERM/INT signal caught, exiting');
	cleanup ();
	unlink ($tmp_logfile);
	send_influxdb_annotation('interrupted') if $influxdb_annotation;
	unlock ();
	exit 1;
}

sub cleanup {
	close $tmpfile_fh;
}

sub write_log {
	my @txt = @_;

	open (FILE, ">>$conf{'logfile'}")
		or return undef;

	foreach my $line (@txt) {
		printf (FILE "%s: %s\n", strftime ("%Y%m%d %H%M%S", localtime ()), $line);
	}
	close (FILE);
}

sub sec_to_human { 
	my $time = shift; 
	my $days = int($time / 86400); 
	$time -= ($days * 86400); 
	my $hours = int($time / 3600); 
	$time -= ($hours * 3600); 
	my $minutes = int($time / 60); 
	my $seconds = $time % 60; 

	$days = $days < 1 ? '' : $days .'d '; 
	$hours = $hours < 1 ? '' : $hours .'h '; 
	$minutes = $minutes < 1 ? '' : $minutes . 'm '; 
	$time = $days . $hours . $minutes . $seconds . 's'; 
	return ($time); 
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
	if (-e $conf{'lock_file'}) {
		my $mtime = (stat ($conf{'lock_file'}))[9];
		if (time () - $mtime > $conf{'lock_file_expiration'}) {
			write_log ("WARNING: lock file $conf{'lock_file'} expired, ignoring");
			system ("touch $conf{'lock_file'}");
		}
		else {
			write_log ('WARNING: program is locked, exiting');
			exit 1;
		}
	}
	else {
		if (!open (FILE, ">$conf{'lock_file'}")) {
			write_log ("ERROR: cannot create file $conf{'lock_file'}");
			exit 1;
		}
		print (FILE "$$");
		close (FILE);
	}
}

sub unlock {
	if (-e $conf{'lock_file'}) {
		if (!unlink ($conf{'lock_file'})) {
			write ("ERROR: cannot delete file $conf{'lock_file'}");
			exit 1;
		}
	}
}

sub notify_zabbix {
	my $zabbix_key = shift;
	my $value = shift;
	my $output;
	if (length ($conf{'zabbix_server'})) {
		$output = `$conf{'zabbix_sender'} -z $conf{'zabbix_server'} -s $hostname -k $zabbix_key -o $value 2>&1`;
	}
	else {
		$output = `$conf{'zabbix_sender'} -c $conf{'zabbix_agentd_conf'} -s $hostname -k $zabbix_key -o $value 2>&1`;
	}
	$output =~ s/\n/ /g;
	write_log ("INFO: zabbix key: $zabbix_key, zabbix_sender: " . $output);
}

sub send_influxdb_annotation {
	my $state = shift;

	my $ua = LWP::UserAgent->new(use_eval => 0);
	$ua->timeout($conf{'influxdb_connection_timeout'});
	my $req = HTTP::Request->new(POST => $conf{'influxdb_url'}.'/write?db='.$conf{'influxdb_database'});
	$req->content($conf{'influxdb_measurement'}.',host='.$hostname.' title="backup mysql '.$state.'"');

	my $res;
	eval {
		local $SIG{ALRM} = sub { die "timeout"; };
		alarm($conf{'influxdb_connection_timeout'});
		$res = $ua->request($req);
		alarm(0);
	};

	if ($@ =~ /timeout/) {
		write_log ("WARNING: sending influxdb annotation timeout");
	}
	else {
		if ($res->is_success) {
			write_log ("INFO: sending influxdb annotation success");
		}
		else {
			write_log ("WARNING: sending influxdb annotation failed: ".$res->code);
		}
	}
}

sub send_pushgateway_metrics {
	my $state = shift;
	my $job = shift;
	my $duration = shift;

	my $ua = LWP::UserAgent->new(use_eval => 0);
	$ua->timeout($conf{'pushgateway_connection_timeout'});
	my $req = HTTP::Request->new(POST => $conf{'pushgateway_url'}.'/metrics/job/'.$job.'/instance/'.$hostname);

	my $timestamp = time ();
	if ($state eq 'success') {
		my $content = <<"END_MESSAGE";
# HELP mysql_backup_duration_seconds Duration of mysql backup
# TYPE mysql_backup_duration_seconds gauge
mysql_backup_duration_seconds{mysql_host="$conf{'mysql_host'}"} $duration
# HELP mysql_backup_last_success_timestamp_seconds Unixtime mysql backup last succeeded
# TYPE mysql_backup_last_success_timestamp_seconds gauge
mysql_backup_last_success_timestamp_seconds{mysql_host="$conf{'mysql_host'}"} $timestamp
# HELP mysql_backup_last_success Success of mysql backup
# TYPE mysql_backup_last_success gauge
mysql_backup_last_success{mysql_host="$conf{'mysql_host'}"} 1
END_MESSAGE

		$req->content($content);
	}
	elsif ($state eq 'fail') {
		my $content = <<"END_MESSAGE";
# HELP mysql_backup_last_success Success of mysql backup
# TYPE mysql_backup_last_success gauge
mysql_backup_last_success{mysql_host="$conf{'mysql_host'}"} 0
END_MESSAGE
		$req->content($content);
	}

	my $res;
	eval {
		local $SIG{ALRM} = sub { die "timeout"; };
		alarm($conf{'pushgateway_connection_timeout'});
		$res = $ua->request($req);
		alarm(0);
	};

	if ($@ =~ /timeout/) {
		write_log ("WARNING: sending pushgateway metrics timeout");
	}
	else {
		if ($res->is_success) {
			write_log ("INFO: sending pushgateway metrics success");
		}
		else {
			write_log ("WARNING: sending pushgateway metrics failed: ".$res->code);
		}
	}
}

sub get_items_from_backup_root {
	my $backup_root = shift;
	my $items;

	opendir (my $dh, $backup_root);
	while (readdir $dh) {
		if (/^((\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})(\.tar\.(?:gz|bz2|xz))?)/) {
			my ($year, $mon, $mday, $hour, $min, $sec) = ($2, $3, $4, $5, $6, $7);
			my $time = timelocal ($sec, $min, $hour, $mday, $mon - 1, $year);
			my $type = length $8 ? 'file' : 'dir';

			$items->{$1} = {
				'timestamp' => $time,
				'type' => $type,
			};
			if ($mday == $conf{'long_term_backup_day'}) {
				$items->{$1}->{'long_term_backup'} = $year . sprintf ("%.2d", $mon);
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
	foreach my $item (sort { $items->{$b}->{'timestamp'} <=> $items->{$a}->{'timestamp'} } keys (%$items)) {
		if ($item =~ /^$start_timestamp_str/) {
			$items->{$item}->{'current'} = 1;
		} 
		if ($ctr <= $conf{'min_backups'}) {
			$items->{$item}->{'do_not_delete'} = 1;
		}
		if ($conf{'max_backups'} != 0 &&
			$ctr > $conf{'max_backups'} &&
			!defined ($items->{$item}->{'current'})) {
			$items->{$item}->{'force_delete'} = 1;
		}
		$ctr++;
	}
	return ($items);
}
	
sub unmark_multiple_long_term_backups {
	my $items = shift;

	# if there is more than one backup from a day period $conf{'long_term_backup_day'} choose only first - only one long term backup per day
	my $tmp = 0;
	foreach my $item (sort {$items->{$a}->{'timestamp'} <=> $items->{$b}->{'timestamp'}} keys (%$items)) {
		next if (!defined ($items->{$item}->{'long_term_backup'}));
		# remove 'long_term_backup' key
		if ($tmp eq $items->{$item}->{'long_term_backup'}) {
			delete ($items->{$item}->{'long_term_backup'});
		}
		# $item is long term backup - first from long term backup day $conf{'long_term_backup_day'}
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
				$start_timestamp - $items->{$item}->{'timestamp'} > 86400 * $days &&
				!defined ($items->{$item}->{'do_not_delete'})) ||
				defined ($items->{$item}->{'force_delete'})) {

				write_log ("INFO: deleting old backup $item");
				# compressed backups
				if ($items->{$item}->{'type'} eq 'file') {
					if (system ("rm -f $conf{'backup_root'}${item} 2>&1 >>$conf{'logfile'}") != 0) {
						write_log ("ERROR: cannot delete old backup $item");
						return (undef);
					}
				}
				# uncompressed backups
				elsif ($items->{$item}->{'type'} eq 'dir') {
					if (system ("rm -rf $conf{'backup_root'}${item} 2>&1 >>$conf{'logfile'}") != 0) {
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
	my $long_term_backups = shift;

	# deleting long term backups
	my $ctr = 1;
	foreach my $item (sort { $items->{$b}->{'timestamp'} <=> $items->{$a}->{'timestamp'} } keys (%$items)) {
		if (defined ($items->{$item}->{'long_term_backup'})) {
			if (!defined ($items->{$item}->{'current'}) && $ctr > $long_term_backups) {
				write_log ("INFO: deleting old long term backup $item");
				# compressed backups
				if ($items->{$item}->{'type'} eq 'file') {
					if (system ("rm -f $conf{'backup_root'}${item} 2>&1 >>$conf{'logfile'}") != 0) {
						write_log ("ERROR: cannot delete old backup $item");
						return (undef);
					}
				}
				# uncompressed backups
				elsif ($items->{$item}->{'type'} eq 'dir') {
					if (system ("rm -rf $conf{'backup_root'}${item} 2>&1 >>$conf{'logfile'}") != 0) {
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

sub make_xtrabackup {
	my $current_backup_dir = shift;
	my $cmd;
	my $output;
	my $backup_product;
	my $opts;

	if (length ($conf{'mysql_user'}) && length ($conf{'mysql_passwd'})) {
		$opts = "--user=$conf{'mysql_user'} --password=$conf{'mysql_passwd'} --host=$conf{'mysql_host'} ";
	}
	else {
		$opts = "--defaults-extra-file=$conf{'client_auth_file'} "
	}

	$opts = $opts . ($conf{'parallel'} != 1 ? "--parallel=$conf{'parallel'} " : '') .
		($conf{'compress-threads'} != 1 ? "--compress-threads=$conf{'compress-threads'} " : '') .
		($conf{'my_cnf'} ? "--defaults-file=$conf{'my_cnf'} " : '');

	if ($conf{'compress'} == 0 || ($conf{'stream'} == 0 && $conf{'compress'} > 1)) {
		$backup_product = $current_backup_dir;
		$opts = $opts . "--no-timestamp $backup_product 2> >(tee -a $conf{'logfile'} $tmp_logfile >/dev/null)";
	}
	elsif ($conf{'compress'} == 1) {
		$backup_product = $current_backup_dir;
		$opts = $opts . "--compress --no-timestamp $backup_product 2> >(tee -a $conf{'logfile'} $tmp_logfile >/dev/null)";
	}
	elsif ($conf{'compress'} == 2 && $conf{'stream'}) {
		$backup_product = "${current_backup_dir}.tar.gz";
		$opts = $opts . "--stream=tar ./ 2> >(tee -a $conf{'logfile'} $tmp_logfile >/dev/null) | pigz - > $backup_product";
	}
	elsif ($conf{'compress'} == 3 && $conf{'stream'}) {
		$backup_product = "${current_backup_dir}.tar.bz2";
		$opts = $opts . "--stream=tar ./ 2> >(tee -a $conf{'logfile'} $tmp_logfile >/dev/null) | lbzip2 - > $backup_product";
	}
	elsif ($conf{'compress'} == 4 && $conf{'stream'}) {
		$backup_product = "${current_backup_dir}.tar.xz";
		$opts = $opts . "--stream=tar ./ 2> >(tee -a $conf{'logfile'} $tmp_logfile >/dev/null) | pxz - > $backup_product";
	}
	else {
		write_log ("ERROR: wrong compress method");
		return undef;
	}

	$cmd = "$conf{'innobackupex'} " . $opts;

	my $cmd_sec = $cmd;
	$cmd_sec =~ s/\s--password=\S+\s/ --password=XXX /;
	write_log ("INFO: backup command: $cmd_sec");
	
	# to execute bash instead of default sh
	system ("bash -c '$cmd'");

	my $success = 0;
	my $result = join '', <$tmpfile_fh>;
	if ($result =~ /\d{6}\s+\d{2}:\d{2}:\d{2}\s+innobackupex: completed OK!/ || $result =~ /\d{6}\s+\d{2}:\d{2}:\d{2}\s+completed OK!/) {
		if ($conf{'stream'} == 0 && $conf{'compress'} > 1) {
			my $taropts;
			if ($conf{'compress'} == 2) {
				$taropts = "cvpf ${current_backup_dir}.tar.gz -I pigz "
			}
			elsif ($conf{'compress'} == 3) {
				$taropts = "cvpf ${current_backup_dir}.tar.bz2 -I lbzip2 "
			}
			elsif ($conf{'compress'} == 4) {
				$taropts = "cvpf ${current_backup_dir}.tar.xz -I pxz "
			}
			else {
				write_log ("ERROR: wrong compress method");
				return undef;
			}
			my $tarcmd = 'tar ' . $taropts . "-C $conf{'backup_root'} $start_timestamp_str 2>&1 >>$conf{'logfile'}";
			write_log ("INFO: starting compression $current_backup_dir, command: $tarcmd");

			if (system ($tarcmd) == 0) {
				write_log ("INFO: compression ok, deleting current backup dir $current_backup_dir");
				if (system ("rm -rf $current_backup_dir 2>&1 >>$conf{'logfile'}") == 0) {
					if (delete_old_backups ($conf{'backup_root'}, $conf{'backup_retention'}, $conf{'long_term_backups'})) {
						$success = 1;
					}
					else {
						$success = 0;
					}
				}
				else {
					$success = 0;
					write_log ("ERROR: cannot delete $current_backup_dir");
				}
			}
			else {
				$success = 0;
				write_log ("ERROR: cannot compress $current_backup_dir");
			}

		}
		else {
			if (delete_old_backups ($conf{'backup_root'}, $conf{'backup_retention'}, $conf{'long_term_backups'})) {
				$success = 1;
			}
			else {
				$success = 0;
			}
		}
	}
	else {
		write_log ("ERROR: cannot make backup, deleting failed backup $backup_product");
		if (system ("rm -rf $backup_product 2>&1 >>$conf{'logfile'}") != 0) {
			write_log ("ERROR: cannot delete failed backup $backup_product");
		}
		$success = 0;
	}

	if ($success) {
		my $duration = time () - $start_timestamp;
		write_log ('INFO: backup duration: '.sec_to_human ($duration));
		notify_zabbix ($conf{'zabbix_key'}, SUCCESS) if ($conf{'notify_zabbix'});
		notify_zabbix ($conf{'zabbix_key_duration'}, $duration) if ($conf{'notify_zabbix'});
		send_pushgateway_metrics ('success', $conf{'prometheus_job'}, $duration) if ($conf{'pushgateway_url'});
	}
	else {
		notify_zabbix ($conf{'zabbix_key'}, FAIL) if ($conf{'notify_zabbix'});
		send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	}

	unlink ($tmp_logfile);
}

sub parse_config {
	my $cnf_file;
	if (-e '/etc/backup/mysql-backup.conf') {
		$cnf_file = '/etc/backup/mysql-backup.conf';
	}
	elsif (-e '/matrix/mysql-backup.conf') {
		$cnf_file = '/matrix/mysql-backup.conf';
	}
	elsif (-e '~/.mysql-backup') {
		$cnf_file = '~/.mysql-backup';
	}
	else {
		return;
	}

	Config::Simple->import_from ($cnf_file, \%conf);
	foreach my $param (keys %conf) {
		$conf{$1} = $conf{$param} if ($param =~ /default\.(\S+)/);
	}
}

## MAIN

parse_config ();

if ($conf{'notify_zabbix'} && (!length ($conf{'zabbix_server'}) && ! -e $conf{'zabbix_agentd_conf'})) {
	write_log ("ERROR: zabbix_server not defined and zabbix_agentd_conf does not exists");
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if ($conf{'notify_zabbix'} && (! -e $conf{'zabbix_sender'})) {
	write_log ("ERROR: zabbix_sender $conf{'zabbix_sender'} does not exists");
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if ($> != 0) {
	print ("ERROR: you must be root to run this program\n");
	notify_zabbix ($conf{'zabbix_key'}, FAIL) if ($conf{'notify_zabbix'});
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if (! -e $conf{'innobackupex'}) {
	write_log ("ERROR: $conf{'innobackupex'} does not exists");
	notify_zabbix ($conf{'zabbix_key'}, FAIL) if ($conf{'notify_zabbix'});
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if (! -d $conf{'backup_root'}) {
	write_log ("ERROR: backup root directory $conf{'backup_root'} does not exists");
	notify_zabbix ($conf{'zabbix_key'}, FAIL) if ($conf{'notify_zabbix'});
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if ($conf{'check_mountpoint'} && !check_mountpoint ($conf{'backup_root'})) {
	write_log ("ERROR: mount point for $conf{'backup_root'} does not exists");
	notify_zabbix ($conf{'zabbix_key'}, FAIL) if ($conf{'notify_zabbix'});
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if ($conf{'min_backups'} >= $conf{'max_backups'}) {
	write_log ("ERROR: min_backups >= max_backups");
	notify_zabbix ($conf{'zabbix_key'}, FAIL) if ($conf{'notify_zabbix'});
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if (!length ($conf{'client_auth_file'}) && (!length ($conf{'mysql_user'}) || !length ($conf{'mysql_passwd'}))) {
	write_log ('ERROR: cannot find valid mysql credentials');
	notify_zabbix ($conf{'zabbix_key'}, FAIL) if ($conf{'notify_zabbix'});
	send_pushgateway_metrics ('fail', $conf{'prometheus_job'}, 0) if ($conf{'pushgateway_url'});
	exit 1;
}

if (length ($conf{'influxdb_url'}) && length ($conf{'influxdb_database'}) && length ($conf{'influxdb_measurement'})) {
	$influxdb_annotation = 1;
}

# short hostname
if ($hostname =~ /^([^\.]+)\./) {
	$hostname = $1;
}

if (-t 1) {
	write_log ("INFO: backup start, interactive executing");
}
else {
	init_sleep ($conf{'initial_sleep'}) if ($conf{'initial_sleep'});
	write_log ("INFO: backup start");
}

lock ();
$start_timestamp = time ();
$start_timestamp_str = strftime ("%Y-%m-%d_%H-%M-%S", localtime ($start_timestamp));
send_influxdb_annotation('start') if $influxdb_annotation;

make_xtrabackup ($conf{'backup_root'}.$start_timestamp_str);

send_influxdb_annotation('stop') if $influxdb_annotation;
cleanup ();
unlock ();
