<?php
  $host = $_SERVER['HTTP_HOST'];
  $bash1 = 'bash ../scripts/hdfs_clear.sh';
  $bash2 = 'bash ../scripts/clear_upload.sh';
  $bash3 = 'bash ../scripts/add_file.sh';
  exec($bash1);
  exec($bash2);
  
  $fileName = $_FILES['file']['name'];
  $fileType = $_FILES['file']['type'];
  $fileError = $_FILES['file']['error'];
  $fileContent = file_get_contents($_FILES['file']['tmp_name']);
  
  
  
  if($fileError == UPLOAD_ERR_OK){
   $fileholder = fopen("../uploaded/".$fileName, "w");
   fwrite($fileholder,$fileContent);
   fclose($fileholder);
   exec($bash3);
   echo("1");
  }
  
?>