<?php

  $indexes = '';
  $host = $_SERVER['HTTP_HOST'];
  $searchTerms = $_POST['search'];
  //echo(exec('whoami'));
 // $deleteString("hdfs dfs -rmr /web1search");
  //echo(exec($deleteString));
  $bash1 = 'bash ../scripts/hdfs_clear.sh';
  $bash2 = 'bash ../scripts/generate_results_spark.sh "'.$searchTerms.'"';
  $bash3= 'bash ../scripts/json_clear.sh';
  $bash4 = 'bash ../scripts/json_move_spark.sh';
  echo($bash2);
  exec($bash1);
  exec($bash2);
  exec($bash3);
  exec($bash4);
?>
