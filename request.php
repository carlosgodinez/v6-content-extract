<!doctype html>
<html>
<head>
    <meta charset="utf-8" />
    <title> V6 Content Extraction Tool </title>
    
	<! -- <link href="./includes/style.css" rel="stylesheet"--> 
	<link href="./includes/extract.css" rel="stylesheet">
    <script type="text/javascript" src="forms.js"></script>
</head>
<body>

<div class="head">V6 Content Extraction Tool</div>

<form id="f1" method="get" action="result.php">
	<div class="main">
    <div class="field">
        <label class="labelField" for="type">Content Type:</label>
        <select name="type" id="type">
            <option value="article" selected>Article</option>
            <option value="component">Component</option>
            <option value="topics">TOPIC Class</option>
        </select>
    </div>
    <div class="field">
        <label class="labelField" align="left" for="subtype">Sub Type:</label>
        <input class="inputField" name="subtype" type="text" id="subtype" required>
    </div>
    <div class="field">
        <label class="labelField" align="left" for="count">Count:</label>
        <input class="inputField" name="count" type="number" id="number" value=0 required>
    </div>
    <div class="field">
        <label class="labelField" for="startDate">Start Date:</label>
        <input class="inputField" name="startDate" type="date" id="startDate">
    </div>
	<div class="field">
		<label class="labelField" for="endDate">End Date:</label>
        <input class="inputField" name="endDate" type="date" id="endDate">
	</div>
    <div class="field">
	    <input id="submit" type="submit" />
    </div>
	</div>
</form>

</body>
</html>
