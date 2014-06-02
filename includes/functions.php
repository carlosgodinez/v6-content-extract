<?php
/**
 * 
 * @param $url
 * @param $count
 * @param $starDate
 * @param $endDate
 * @param $type
 * @return $output
 *
 * http://dev-uat.ew.com/ew/drupal/manifest/articles/1,,,00.txt?count=0&startDate=10/01/2013&endDate=10/31/2013&columnName=flexible_article
 * http://dev-uat.ew.com/ew/drupal/manifest/components/1,,,00.txt?count=0&startDate=07/01/2013&endDate=10/31/2013&componentTypeName=image
 * http://dev-uat.ew.com/ew/drupal/manifest/components/1,,,00.txt?count=0&startDate=07/01/2013&endDate=10/31/2013&componentTypeName=author
 *
 * Taxonomy Manifests:
 * ·         http://dev-uat.ew.com/ew/drupal/manifest/topics/taxonomy/1,,,00.txt?taxonomy=personsTax&count=0
 * ·         http://dev-uat.ew.com/ew/drupal/manifest/topics/taxonomy/1,,,00.txt?taxonomy=MediaProductsTax&count=0
 * Movies
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,MediaProductsTax:MovieAugustOsageCounty2013,00.xml
 * TV
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,MediaProductsTax:TVSurvivorSeason1,00.xml
 * Music
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,MediaProductsTax:AlbumBeyonce2013,00.xml
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,MediaProductsTax:AlbumHighHopes,00.xml
 * Books
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,MediaProductsTax:BookOnSuchAFullSea,00.xml
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,MediaProductsTax:BookTheInventionOfWings,00.xml
 * Persons
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,personsTax:SigourneyWeaver,00.xml
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,personsTax:WillSmith,00.xml
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,personsTax:WillSmith,00.xml
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,personsTax:VictoriaBeckham,00.xml
 * ·         http://dev-uat.ew.com/ew/drupal/data/topics/entity/1,,personsTax:TomCruise,00.xml
*/

function fetch_manifest($count, $type, $subtype, $startDate, $endDate) {
 
    // is cURL installed yet?
    if (!function_exists('curl_init')) {
        die('Sorry cURL is not installed!');
    }
    
	//echo "<pre> Type: "; print_r($type); echo"</pre>";

	switch ($type) {
		case 'article':
			$url = 'http://dev-uat.ew.com/ew/drupal/manifest/articles/1,,,00.txt';
    		$qry_str = "{$url}?count={$count}&startDate={$startDate}&endDate={$endDate}&columnName={$subtype}";
			break;
		case 'component':
			$url = 'http://dev-uat.ew.com/ew/drupal/manifest/components/1,,,00.txt';
    		$qry_str = "{$url}?count={$count}&startDate={$startDate}&endDate={$endDate}&componentTypeName={$subtype}";
			break;
		case 'topics':
			$url = 'http://dev-uat.ew.com/ew/drupal/manifest/topics/1,,,00.txt';
    		$qry_str = "{$url}?class=$subtype&count={$count}";
			break;
		default:
			return "";
			break;
	}
    //echo "<pre>"; var_dump($qry_str); echo"</pre>";
    
    // OK cool - then let's create a new cURL resource handle
    $ch = curl_init();
 
    // Now set some options (most are optional)
 
    // Set URL to download
    curl_setopt($ch, CURLOPT_URL, "$qry_str" );
 
    // Include header in result? (0 = yes, 1 = no)
    curl_setopt($ch, CURLOPT_HEADER, 0);
 
    // Should cURL return or print out the data? (true = return, false = print)
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
 
    // Timeout in seconds
    curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 0);
 
    // Timeout in seconds
    curl_setopt($ch, CURLOPT_TIMEOUT, 300);
 
    // Download the given URL, and return output
    $output = trim(curl_exec($ch));
 
    // Close the cURL resource, and free system resources
    curl_close($ch);
 
    //echo "<pre>"; var_dump($output); echo"</pre>";
    return $output;
}
