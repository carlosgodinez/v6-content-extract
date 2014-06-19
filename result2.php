<?php
error_reporting(E_ALL); ini_set('display_errors', 'On');

/* 
 * @TODO: Document
 */

require_once ("includes/functions.php");
require_once('lib/bwSQLite3.php');

define('TITLE', 'EW Data Migration TOOL');
define('DB_FILENAME', 'files/ewmigration.db');
define('TABLE_NAME', 'manifesturls');

global $G;
$G['TITLE'] = TITLE;
$G['ME'] = basename($_SERVER['SCRIPT_FILENAME']);

?>

<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title> V6 Content Extraction Tool </title>
<!--link  rel="stylesheet" type="text/css" href="styles.css" /-->
<link  rel="stylesheet" type="text/css" href="./includes/extract.css" />
<script type="text/javascript" src="forms.js"></script>
</head>
<body>

<div class="head">V6 Content Extraction Tool</div>

<?php

if( isset($_POST['manifestURLs']) && is_array($_POST['manifestURLs']) ) {
	//echo "<pre>"; var_dump($_POST['manifestURLs']); echo"</pre>";
	$manifestlist = array_filter($_POST['manifestURLs']);

?>
<form method="post" action="result2.php">
<div class="result2main">
	<div class="field">
		<label class="labelField"> Enter email:</label> 
		<input class="resultinputField" name="email" type="email" required>
	</div>
	<div class="field">
		<label class="labelField"> Enter description:</label> 
		<input class="resultinputField" name="description" type="text" required>
	</div>
	<input type="hidden" name="manifestlist" value="<?php echo implode(',', $manifestlist); ?>">
	<div class="field">
		<input type="submit" name="submit" id="submit" value="Submit">
	</div>
</div>
</form>
</body>
</html>     

<?php

} elseif ( isset($_POST['email']) ) {
	$email = $_POST['email'];
	$manifestlist = $_POST['manifestlist'];
	$description = $_POST['description'];
	//$manifestlist = explode(',', ($_POST['manifestlist']));
	//echo "<pre>"; var_dump($email); echo"</pre>";

	$tn = TABLE_NAME;
	$db = new bwSQLite3(DB_FILENAME);
	try {
		$db->sql_do("INSERT INTO $tn (manifesturl, email, description) VALUES (?, ?, ?)", $manifestlist, $email, $description);
	} catch (PDOException $e) {
		echo "<pre>" . $e->getMessage() . "</pre>";
		// syslog($e->getMessage());
		exit;
	}
	echo "
		<h2 style='color:teal; text-align:center; font-size:20px;'> Request submitted successfully! You'll receive an email when the request is ready.</h2>
	    </body>
		</html> 
	";
} else {
	echo "
		<h2 style='color:teal; text-align:center; font-size:20px;'> No POST data was found. Please try again. </h2>
	    </body>
		</html> 
	";
	
}
?>
