<?php
error_reporting(E_ALL); ini_set('display_errors', 'On');

/* 
 * @TODO: Document
 */

require_once ("includes/functions.php");
require_once('lib/bwSQLite3.php');

define('TITLE', 'EW Data Migration TOOL');
define('DB_FILENAME', 'files/ewmigration.db');
define('TABLE_NAME', 'manifest');

$quantityErr = "";
$count = "";
$type = "";
$result = 0;

global $G;
$G['TITLE'] = TITLE;
$G['ME'] = basename($_SERVER['SCRIPT_FILENAME']);

if ($_SERVER["REQUEST_METHOD"] == "GET") {
#if (isset($_GET['submit'])) {
    //echo "<pre>"; var_dump($_GET); echo"</pre>";

	$type = $_GET["type"];
	$subtype = $_GET["subtype"];
	$count = isset($_GET['count']) ? $_GET['count'] : 0;
	$startDate = isset($_GET['startDate']) ? $_GET['startDate'] : "";
	$endDate = isset($_GET['endDate']) ? $_GET['endDate'] : "";

	$manifest = array_filter(explode("\r", fetch_manifest("$count", "$type", "$subtype", "$startDate", "$endDate")));
	//echo "<pre>"; var_dump($manifest); echo"</pre>";
	//echo "<pre>"; print_r($manifest); echo"</pre>";

	if(!empty($manifest)) {
		////echo "<pre>"; var_dump($manifest); echo"</pre>";

		// save manifest into a file
		$manfname = tempnam(getcwd() . '/files', 'manifest');
		chmod ($manfname, 0666);
		$fh = fopen($manfname, "w");
		foreach( $manifest as $key => $value) (fwrite($fh, "$value\n"));
		fflush($fh);
		fclose($fh);

		// insert manifest name and timestamp
		$tn = TABLE_NAME;
		$db = new bwSQLite3(DB_FILENAME);
		$count = count($manifest);
		$description = "$count $type $subtype $startDate $endDate";
		try {
		  $db->sql_do("insert into $tn (manfname, timestamp, description) values (?, ?, ?)", $manfname, date('m/d/Y'), $description);
		} catch (PDOException $e) {
		  error($e->getMessage());
		}

		$result = "
			<h2>Content Items returned: " . $count . "</h2>
			<form method=\"post\" action=\"finish.php\">
			Enter email to proceed: 
			<input name=\"email\" type=\"email\" required>
			<input type=\"hidden\" name=manfname value=\"$manfname\">
			<input type=\"submit\" name=\"submit\" id=\"submit\" value=\"Submit\">
			</form>
		";
	} else {
		$result = "<h2>No content items returned.</h2>";
	}
}
?>

<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title> V6 Content Extraction Tool </title>
<link  rel="stylesheet" type="text/css" href="styles.css" />
<script type="text/javascript" src="forms.js"></script>
</head>
<body>

<h1>V6 Content Extraction Tool</h1>

<?php echo $result; ?>

</body>
</html>
