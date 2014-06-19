<?php
// error_reporting(E_ALL); ini_set('display_errors', 'On');

/*
 * @TODO: Document
 */

require_once ("includes/functions.php");
require_once('lib/bwSQLite3.php');

define('TITLE', 'EW Data Migration TOOL');
define('DB_FILENAME', 'files/ewmigration.db');
define('TABLE_NAME', 'manifest');

global $G;
$G['TITLE'] = TITLE;
$G['ME'] = basename($_SERVER['SCRIPT_FILENAME']);

if (isset($_POST['submit'])) {
	$email = $_POST['email'];
	$manfname = $_POST['manfname'];

	$tn = TABLE_NAME;
	$db = new bwSQLite3(DB_FILENAME);

	try {
  		$db->sql_do("UPDATE $tn SET email = ? WHERE manfname = ?", $email, $manfname);
	} catch (PDOException $e) {
		echo "<pre>" . $e->getMessage() . "</pre>";
		exit;
	}
	$message = "Request submitted successfully! You'll receive an email when the request is ready.";
} else {
	$message = "Unable to submit request. Please try again.";
}
?>

<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>V6 Content Extraction Tool</title>
  <link  rel="stylesheet" type="text/css" href="styles.css" />
  <script type="text/javascript" src="forms.js"></script>
</head>
<body>

<h1>V6 Content Extraction Tool</h1>

<h2><?php echo $message?></h2>

</body>
</html>
