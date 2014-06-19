#!/usr/bin/perl
#Tue Jun 17 14:37:52 EDT 2014
#Carlos A. Godinez

use strict;
use warnings;

$| = 1;
use DBI;
use Data::Dumper;
use Log::Log4perl qw(:easy);
use Benchmark;
use WWW::Curl::Easy;
use File::Temp qw/tempfile/;
use Fcntl qw(:flock);
use Term::ANSIColor qw(:constants);
use IO::Handle;

my $DEBUG = 0;

my $MaxConcurrentChildren = 20;

my $HD= '/data/timeinc/content/qa/feeds/htdocs/migrate/ew';
my $DB = "$HD/files/ewmigration.db";


my $CURL = '/usr/bin/curl';
my $XMLLINT = '/usr/bin/xmllint';

my $SENDMAIL = '/usr/lib/sendmail';
my $FROM = 'ics@timeinc.net';
my $CC='carlos_godinez@timeinc.com,kevin_wiechmann@ew.com';

my $HEAD = qq!<?xml version="1.0" standalone="yes"?>\n<root>\n!;
my $TAIL = qq!</root>\n!;

umask 000;
chomp(my $ts = `/bin/date '+%m%d%y'`);
Log::Log4perl->easy_init( { level  => $INFO,
	file   => ">>logs/batch3_$ts.log",
    layout => '[%d] %m%n'
});

# exit if script is already running
open(LOCK, ">/data/tmp/$0.lck") or LOGDIE "Cannot open /data/tmp/$0.lck: $!";
flock(LOCK, LOCK_EX | LOCK_NB) or LOGDIE "$0: already running. Aborting";

my $command = "$0 @ARGV";
ALWAYS ">>> START: $command";

# Open database
my $dbh = DBI->connect( "dbi:SQLite:dbname=$DB", "", "", {
	PrintError => 1,
	RaiseError => 1,
	ShowErrorStatement => 1,
	AutoCommit => 0
	}) or LOGDIE("Could not create database connection: " . DBI->errstr);

my $SQL1 = qq(
	SELECT
		id,
		manfname,
		outfname,
		email,
		timestamp
	FROM
		manifest
	WHERE
		outfname IS NULL
		AND email IS NOT NULL
);

my %rl = %{$dbh->selectall_hashref($SQL1, 'id')};
$DEBUG && print '%rl: ' . Dumper(\%rl) . "\n";

$dbh->disconnect;

# Process resulting set if any
if (scalar(keys %rl)) {
	foreach my $r (keys %rl) {
		my $t0 = Benchmark->new;
		my($to, $from, $cc, $subject, $body, $outfname);

		my $xmlfile = "$r-$ts.xml";
		$DEBUG && print "XML file: $xmlfile\n";

		open (my $fh, '>',  "$HD/files/$xmlfile") or LOGDIE("Unable to open $xmlfile file");
		print $fh $HEAD;
		close ($fh) or WARN "Could not close $HD/files/$xmlfile : $!";

		open ($fh, '<', "$rl{$r}->{'manfname'}") or LOGDIE("Unable to open $rl{$r}->{'manfname'} manifest file");
		my @manifest = <$fh>;
		close ($fh);
		INFO "Processing $rl{$r}->{'manfname'} : " . scalar(@manifest) . " URLs";

		if (@manifest) {
			my $ChildrenCount = 0;

			foreach (@manifest) {	
				if($ChildrenCount >= $MaxConcurrentChildren) {
					wait();   #Wait for some child to finish
					$ChildrenCount--;
				}

				my $pid = fork();
				LOGDIE "Could not fork: $!" unless defined($pid);

				if (!$pid ) {
					$DEBUG && print "\nCHILD: enter ...\n";

					my $out = urlRender($_);
					$DEBUG && print RED, "\nURLrENDER OUT\n", RESET, ">>>\n$out\n<<<\n";
					if( $out ) {
						$DEBUG && print RED, "\nCHILD: writing file\n", RESET;
						open (my $fh, '>>',  "$HD/files/$xmlfile") or LOGDIE("Unable to open $xmlfile file");
						flock($fh, LOCK_EX); # wait until unlock;
						$| = 1;
						print $fh $out . "\n";
						close ($fh) or WARN "Could not close $HD/files/$xmlfile : $!";
						$DEBUG && print RED, "\nCHILD: closing file\n", RESET;
						sleep 2;
					}
					$DEBUG && print "\nCHILD: exit ...\n";
					exit 0;
				}
				$ChildrenCount++;
			}
		} else {
			ERROR "EMPTY manifest: " . $rl{$r}->{'manfname'};
			next;
		}
		$DEBUG && print "\nPARENT: done rendering manifest, continuing ...\n";

		# No zombies!
		sleep 1 while wait > 0;

		open ($fh, '>>',  "$HD/files/$xmlfile") or LOGDIE("Unable to open $xmlfile file");
		print $fh $TAIL;
		close ($fh) or WARN "Could not close $HD/files/$xmlfile : $!";

		if( system("/bin/gzip -f $HD/files/$xmlfile 2>$HD/files/$xmlfile.gziperr") != 0 ) {
			ERROR "FAILED gzip HD/files/$xmlfile.gziperr";
			$outfname = "$xmlfile.gziperr";
			$to = 'carlos_godinez@timeinc.com';
			$subject = "EW Content Type migration gzip ERROR";
			$body = "gzip failed for manifest file $rl{$r}->{'manfname'}";
		} else {
			$outfname = "${xmlfile}.gz";
			$to = $rl{$r}->{'email'};
			$subject = "EW Content Type migration alert";
			#$body = "Content migration file ${xmlfile}.gz now available at:\nhttp://qa-feeds.timeinc.net/migrate/ew/download.php";
			#$body = "Content migration file ${xmlfile}.gz now available at:\n https://dcms-tools.timeinc.net/migrate/ew/download.php";
			$body = "Content migration file is available at:\n\thttps://dcms-tools.timeinc.net/migrate/ew/files/${xmlfile}.gz";
		}
	
		# Update manifest table with output file name
		my $dbh = DBI->connect( "dbi:SQLite:dbname=$DB", "", "", {
			PrintError => 1,
			RaiseError => 1,
			ShowErrorStatement => 1,
			AutoCommit => 0
			}) or LOGDIE("Could not create database connection: " . DBI->errstr);

		eval {
			$dbh->do("UPDATE manifest SET outfname = '$outfname' WHERE id = $r");
			$dbh->commit;
		};
		if ($@) {
	        $dbh->rollback or LOGDIE "Couldn't rollback transaction: " . DBI->errstr;
	        LOGDIE "UPDATE into manifest aborted because: $@";
	    }

		$dbh->disconnect;

		my $t1 = Benchmark->new;
		INFO "Finish processing $rl{$r}->{'manfname'} (" . scalar(@manifest) . " URLs)";
		INFO "GZIP output sent to: $xmlfile.gz";
		INFO "Processing completed in: " . timestr(timediff($t1, $t0));

		#  send email to requestor
		$subject = "EW Content Type migration alert";
		sendEmail($to, $FROM, $CC, $subject, $body);
	}
} else {
	INFO "No manifests to process";
}

$DEBUG && print "\nPARENT: exiting  ...\n";

# clean logs
unlink grep { -M > 60 } <./logs/*>;
unlink grep { -z } <../files/*>;

ALWAYS ">>> END <<<";

sub validateXML {
	my( $url, $curlout ) = @_;

	my $DEBUG = 0;

	my $rc = 1;
	my $TO = 'kevin_wiechmann@ew.com';
	
	my $tf = File::Temp->new(DIR => '/data/tmp');
	my $tfn = $tf->filename;
	print $tf $curlout;	
	close $tf;

	my $linterr = qx( $XMLLINT --noout $tfn );
	
	$DEBUG && print RED, "\nCURLOUT\n" , RESET, ">>>\n$curlout\n<<<\n";
	$DEBUG && print YELLOW, "\nTemp file name: $tfn\n", RESET;
	$DEBUG && print RED, "\nLINTERR\n", RESET, ">>>\n$linterr<<<\n";

	if( $linterr ) {
		sendEmail( $TO, $FROM, $CC, "xmllint error", "\nxmllint error output for $url\n$linterr" );
		ERROR "FAILED xmllint: $linterr";
		$rc = 0;
	}	
	$DEBUG && print RED, "\nRETURN\n", RESET, ">>>\n$rc<<<\n";
	return $rc;
}

sub urlRender {
	my( $url ) = @_;
	chomp($url);

	my $DEBUG = 0;

	my $curlout;
	open (my $fh, '>', \$curlout) or LOGDIE("curlout error: $!");
	my $curl = WWW::Curl::Easy->new;

	INFO "CURLing $url ...";
	$curl->setopt(CURLOPT_HEADER, 0);
	$curl->setopt(CURLOPT_URL, $url);
	$curl->setopt(CURLOPT_WRITEDATA, \$fh);
	my $rc = $curl->perform;

	$DEBUG && print RED, "\nCURLOUT for $url (return code: $rc)", RESET, "\n>>>\n$curlout\n<<<";

	if( $rc == 0 ) {
		$curlout =~ s/<\?xml .*\?>//;
		if( validateXML( $url, $curlout ) ) {
			return $curlout;
		} else {
			WARN "validateXML function return an error";
			return;
		}
	} else {
		WARN "Unable to render $url: $curl->getinfo(CURLINFO_HTTP_CODE)";
		return;
	}
	close ($fh);
}

sub sendEmail {
	my ($to, $from, $cc, $subject, $message) = @_;

	open(MAIL, "|$SENDMAIL -oi -t");
	print MAIL "From: $FROM\n";
	print MAIL "To: $to\n";
	print MAIL "CC: $cc\n";
	print MAIL "Subject: $subject\n\n";
	print MAIL "\n$message\n\nRegards,\n\nTimeInc";
	close(MAIL);

	INFO "Alerted $to that output file is available";
} 

