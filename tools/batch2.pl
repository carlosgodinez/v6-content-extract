#!/usr/bin/perl
#Wed Jun 11 14:29:46 EDT 2014
#Carlos A. Godinez

use strict;
use warnings;

use DBI;
use Data::Dumper;
use Log::Log4perl qw(:easy);
use Benchmark;
use WWW::Curl::Easy;
use File::Temp qw/tempfile/;
use Fcntl qw(:flock);

my $DEBUG = 0;

my $HD='/data/timeinc/content/qa/feeds/htdocs/migrate/ew';
my $DB = "$HD/files/ewmigration.db";

my $OLD= 'http://www.ew.com';
my $NEW= 'http://dev-uat.ew.com';

my $CURL= '/usr/bin/curl';
my $SENDMAIL = '/usr/lib/sendmail';

my $FROM = 'ics@timeinc.net';
my $CC='carlos_godinez@timeinc.com,kevin_wiechmann@ew.com'; 

umask 000;
chomp(my $ts = `/bin/date '+%m%d%y'`);
Log::Log4perl->easy_init( { level  => $INFO,
	file   => ">>logs/batch2_$ts.log",
    layout => '[%d] %m%n'
});

# exit if script is already running
open(LOCK, ">/data/tmp/$0.lck") or LOGDIE "Cannot open /data/tmp/$0.lck: $!";
flock(LOCK, LOCK_EX | LOCK_NB) or LOGDIE "$0: already running. Aborting";

my $command = "$0 @ARGV";
ALWAYS ">>> START: $command";

my $SQL1 = qq(
	SELECT
		id,
		email,
		manifesturl,
		description	
	FROM
		manifesturls
	WHERE
		timestamp IS NULL
		AND email IS NOT NULL
);

my $dbh = DBI->connect( "dbi:SQLite:dbname=$DB", "", "", {
	PrintError => 1,
	RaiseError => 1,
	ShowErrorStatement => 1,
	AutoCommit => 0
	}) or LOGDIE("Could not create database connection: " . DBI->errstr);

my %rl = %{$dbh->selectall_hashref($SQL1, 'id')};
$DEBUG && print '%rl: ' . Dumper(\%rl) . "\n";

$dbh->disconnect;

my $t0 = Benchmark->new;

# process selected set if any
if( scalar keys(%rl) ) {
	foreach my $id (keys %rl) {
		my @urls = split(',', $rl{$id}->{'manifesturl'});
		$DEBUG && print '@urls: ' . Dumper(\@urls) . "\n";
		INFO "Processing batch id $id ...";
		
		my %all = ();
		my $manfname;
		my $curl = WWW::Curl::Easy->new;
		foreach my $url (@urls) {
			chomp($url);

			my $out = '';
			open (my $fh, '>', \$out) or die "$!";

			INFO "CURLing $url ...";
			$curl->setopt(CURLOPT_HEADER, 0); 
			$curl->setopt(CURLOPT_URL, $url);
			$curl->setopt(CURLOPT_WRITEDATA, \$fh);
			my $rc = $curl->perform;

			if ($rc == 0) {
				#$DEBUG && print("curl out: $out\n");
				if( $out ) {
					$all{$url} = [ split('\r', $out) ];
					INFO " ... manifest produced " . scalar @{$all{$url}} . " URLs";
				}
			} else {
				 INFO "Unable to render $url: $curl->getinfo(CURLINFO_HTTP_CODE)";
			}
			close($fh);
		}
		$DEBUG && print '%all: ' . Dumper(\%all) . "\n";

		chomp(my $lts = `date '+%m/%d/%y %H:%M'`);
		my ($to, $cc, $from, $subject, $message);

		my $dbh = DBI->connect( "dbi:SQLite:dbname=$DB", "", "", {
			PrintError => 1,
			RaiseError => 1,
			ShowErrorStatement => 1,
			AutoCommit => 0
			}) or LOGDIE("Could not create database connection: " . DBI->errstr);

		if( scalar keys(%all) ) {
			$manfname = $HD . '/files/manifest' . $id . $ts;
			open (M, ">$manfname") or LOGDIE("Unable to open $manfname file");

			foreach my $a (keys %all) {
				$DEBUG && print '$all{$a}} curl array: ' . Dumper(\@{$all{$a}}) . "\n";
				if ( @{$all{$a}} ) {
			 		foreach ( @{$all{$a}} ) { print M "$_\n"; }; 
				} else {
					WARN "Manifest $a renderend NO URLS";
					next;
				}
			}
			close(M);

			INFO "Built manifest $manfname";

			my $sth = $dbh->prepare("INSERT INTO manifest (manfname, email, timestamp, description) VALUES (?, ?, ?, ?)");
			$sth->execute($manfname, $rl{$id}->{'email'}, $lts, $rl{$id}->{'description'});
		} else {
			$manfname = 'Warning: NO URLs produced any manifests!';
			WARN "Warning: NO URLs produced any manifests";
		}

		eval {
			$dbh->do("UPDATE manifesturls SET timestamp = '$lts', description = '$manfname' WHERE id = $id");
			$dbh->commit;
		};
		if ($@) {
			$dbh->rollback or LOGDIE "Couldn't rollback transaction: " . DBI->errstr;
			LOGDIE "UPDATE into manifest aborted because: $@";
		}   
		$dbh->disconnect;

		my $t1 = Benchmark->new;
		INFO "Completed manifest builds in " . timestr(timediff($t1, $t0));

		$message = ( $manfname =~ /NO URLs/ ? "ERROR: Custom request is empty!\nPlease contact us." : 'Your "' . $rl{$id}->{'description'} . "\" request was queued successfully!\nYou'll receive an email you when it's done.");

		# sent email to requestor and others
		$to = $rl{$id}->{'email'};
		$subject = "EW Content Type migration alert: " . $rl{$id}->{'description'} . " queued.";
		sendEmail($to, $FROM, $CC, $subject, $message);
	}
} else {
	WARN "No records to process.";
}
ALWAYS ">>> END <<<";

sub sendEmail
{
	my ($to, $from, $cc, $subject, $message) = @_;

	open(MAIL, "|$SENDMAIL -oi -t");
	print MAIL "From: $from\n";
	print MAIL "To: $to\n";
	print MAIL "CC: $cc\n";
	print MAIL "Subject: $subject\n\n";
	print MAIL "\n$message\n\nRegards,\n\nTimeInc";
	close(MAIL);

	INFO "Alerted $to that output file is available";
} 
