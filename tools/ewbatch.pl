#!/usr/bin/perl
#Fri May 16 17:31:30 EDT 2014
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
my $HD='/data/home/cgodinez/public_html';
my $DB = "$HD/files/ewmigration.db";
my $OLD= 'http://www.ew.com';
my $NEW= 'http://dev-uat.ew.com';
my $CURL= '/usr/bin/curl';
my $SENDMAIL = '/usr/lib/sendmail';

# @TODO: validate that these are the right values
my $OHEAD = '<?xml version="1.0" encoding="utf-8" ?>';
my $HEAD = qq!<?xml version="1.0" standalone="yes"?>\n<root>!;
my $TAIL = qq!</root>!;

umask 000;
chomp(my $ts = `/bin/date '+%m%d%y'`);
Log::Log4perl->easy_init( { level  => $INFO,
	file   => ">>logs/ewbatch_$ts.log",
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
	ShowErrorStatement => 1
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

if (scalar(keys %rl)) {
	foreach my $r (keys %rl) {
		my $t0 = Benchmark->new;
		my $xmlfile = "$r-$ts.xml";
		open (FX, ">$HD/files/$xmlfile") or LOGDIE("Unable to open $xmlfile file");
		print FX "$HEAD\n";
		$DEBUG && print "$xmlfile\n";

		INFO "Processing $rl{$r}->{'manfname'} ...";
		open (FU, $rl{$r}->{'manfname'}) or LOGDIE("Unable to open $rl{$r}->{'manfname'} manifest file");
		my $curl = WWW::Curl::Easy->new;
		while (my $url = <FU>) {
			chomp($url);
			$url =~ s/$OLD/$NEW/;
			my $response_body = '';
			open (my $fh, '>', \$response_body) or die "$!";

			INFO "CURLing $url ...";
			$curl->setopt(CURLOPT_HEADER, 0);
			$curl->setopt(CURLOPT_URL, $url);
			$curl->setopt(CURLOPT_WRITEDATA, \$fh);
			my $rc = $curl->perform;

			if ($rc == 0) {
				$DEBUG && print("Received response: $response_body\n");
				$response_body =~ s/<\?xml.*\?>//;
				print FX "$response_body\n";
			} else {
				#@TODO: need to break
				INFO "Unable to render $url: $curl->getinfo(CURLINFO_HTTP_CODE)";
			}
			close ($fh);
		}

		close(FU);
		print FX "$TAIL\n";
		close(FX);

		# validate xml
		if( system("/usr/bin/xmllint --noout $HD/files/$xmlfile") != 0 ) {
			ERROR "HD/files/$xmlfile -> malformed";
			next;
		}
		# validate gzip
		if( system("/bin/gzip -f $HD/files/$xmlfile") != 0 ) {
			ERROR "HD/files/$xmlfile -> gzip failed";
			next;
		}
	
		# Update manifest table with output file name
		# @TODO: eval that insert worked
		$dbh->do("UPDATE manifest SET outfname = '${xmlfile}.gz' WHERE id = $r");

		my $t1 = Benchmark->new;
		my $td = timestr(timediff($t1, $t0));
		INFO "Output sent to file: $xmlfile ($td)";			

		#  send emial to requestor
		my $to = $rl{$r}->{'email'};
		my $from = 'carlos_godinez@timeinc.com';
		my $cc = 'Josh_Hoeltzel@timeinc.com';
		my $subject = "EW Content Type migration alert";
		my $body = "Your migration file ${xmlfile}.gz is now available at:\nhttp://test-ics-lamp1.timeinc.net/~cgodinez/download.php\nRegards\n";
		sendEmail($to, $from, $subject, $body);
	}
} else {
	INFO "No records to process";
}

# clean logs
unlink grep { -M > 60 } <./logs/*>;
$dbh->disconnect;
ALWAYS ">>> END <<<";

sub sendEmail
{
	my ($to, $from, $subject, $message) = @_;
	open(MAIL, "|$SENDMAIL -oi -t");
	print MAIL "From: $from\n";
	print MAIL "To: $to\n";
	print MAIL "Subject: $subject\n\n";
	print MAIL "$message\n";
	close(MAIL);

	INFO "Alerted $to that output file is available";
} 
