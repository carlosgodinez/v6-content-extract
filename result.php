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
		  $db->sql_do("INSERT INTO $tn (manfname, timestamp, description) VALUES (?, ?, ?)", $manfname, date('m/d/Y H:i'), $description);
		} catch (PDOException $e) {
		  echo "<pre>" . $e->getMessage() . "</pre>";
		  exit;
		}

		$result = "
			<h2 style='color:teal; text-align:center; font-size:20px;'>Content Items returned: " . $count . "</h2>
			<form method=\"post\" action=\"finish.php\">
			    <div class='main'>
			      <div class='field'>
				<label class='labelField'> Enter email to proceed: </label>
				<input class='inputField' name=\"email\" type=\"email\" required>
			     </div>
				<input type=\"hidden\" name=manfname value=\"$manfname\">
				<div class='field'>
					<input type=\"submit\" name=\"submit\" id=\"submit\" value=\"Submit\">
				</div>
			    </div>
			</form>
		";
	} else {
		$result = "<h2 style='color:teal; text-align:center; font-size:20px;'>No content items returned.</h2>";
	}
}
?>

<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title> V6 Content Extraction Tool </title>
<link  rel="stylesheet" type="text/css" href="./includes/extract.css" />
<script type="text/javascript" src="forms.js"></script>
</head>
<body>

<div class="head">V6 Content Extraction Tool</div>

<?php echo $result; ?>

</body>
</html>
