<?php
error_reporting(E_ALL); ini_set('display_errors', 'On');
/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

require_once('lib/bwSQLite3.php');

define('DB_FILENAME', 'files/ewmigration.db');
define('TABLE_NAME', 'manifest');
$base = pathinfo($_SERVER['PHP_SELF']);
//$files = $hd['dirname'] . '/files';

if (isset($_POST['submit'])) {
  $email = $_POST['email'];
  
  $tn = TABLE_NAME;
  $db = new bwSQLite3(DB_FILENAME);
  
  //print_r($email);
  try {
        $rows = $db->sql_query_all("SELECT * FROM $tn WHERE email = '$email' AND outfname is not NULL ORDER BY timestamp DESC LIMIT 10");
        //var_dump($rows);
        
        if (!empty($rows)) {
          $table = "<table>\n<tr class=\"tablefield\">\n<th class=\"labelField\">File Name</th>\n<th>Description</th>\n</tr>";
          foreach($rows as $row) {
            $file = $row['outfname'];
            $description = $row['description'];
            $table .= "
			  <tr>
				<td>
				  <a href=\"files/$file\">$file</a>
				</td>
				<td>
				  $description
				</td> 
			  </tr>";            
          }
          $table .= "</table>";
        } else {
          $table = "<h2>No files found for this email address: $email</h2>";
        }
    } catch(PDOException $e) {
        echo "PDOException: " . $e->getMessage();
    }
} else {
	$table = "<h2>Please call download URL first</h2>";
}
?>

<html>
<head>
  <meta charset="utf-8" />
  <title>V6 Content Extraction Tool
  </title>
  <link href="./includes/style.css" rel="stylesheet">
  <link href="./includes/extract.css" rel="stylesheet">
  <script type="text/javascript" src="forms.js"></script>
</head>
<body>

<div class="head">V6 Content Extraction Tool</div>
<div class ="getfilemain">
	<?php echo $table ?>
</div>

</body>
</html>
