/**
* @Author Jake Halloran and Andrew Levandoski
* Map Reduce code for Tiny Google Project of Pitt CS1699
**/ 


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.*;
import java.io.*;
//import org.json.simple.JSONObject;
//import org.json.simple.JSONArray;

public class TinyGoogleMapReduce {

  //hashmap representing inverted index
  static HashMap <String, ArrayList> invertedIndex = new HashMap <String, ArrayList>();
  
  public static void main(String[] args) throws Exception {
    if (args.length != 2 && args.length != 3 && args.length!=4) {
      System.err.println("Usage: TinyGoogleMapReduce <input path> <output path> <option_num>");
      System.exit(-1);
    }
    
    //use mapreduce to build inverted index
    if(args.length == 2 || args[2].equals("1")){
      //delete anything currently occupying the output location
      FileSystem fs = FileSystem.get(new Configuration());
      fs.delete(new Path(args[1]), true);
      
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf,"TinyGoogleMR");
      job.setJarByClass(TinyGoogleMapReduce.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));    
      job.setMapperClass(TinyGoogleMapper.class);
      job.setReducerClass(TinyGoogleReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);   
      
      //take job output and build index
      if(job.waitForCompletion(true)){
        Path mr_output = new Path(args[1]+"/part-r-00000");       
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(mr_output)));
        String line;
        line = br.readLine();
        while(line != null){
          line = br.readLine();
          if(line != null){
            String[] splitted = line.split("\\s+");
            if(splitted.length > 2){
              
              //if the word is already in the inverted index add this books words
              if(invertedIndex.containsKey(splitted[1])){
                ArrayList al = invertedIndex.get(splitted[1]);
                String[] arr = new String[2];
                //book name
                arr[0] = splitted[0];
                //number
                arr[1] = splitted[2].trim();
                al.add(arr);//splitted[0], Integer.parseInt(splitted[2].trim()));
              }
              //if the word is not in the inverted index add a place for it and store this books words
              else{
                ArrayList<String[]> al = new ArrayList<String[]>();
                String[] arr = new String[2];
                arr[0] = splitted[0];
                arr[1] = splitted[2].trim();
                al.add(arr);       //splitted[0], Integer.parseInt(splitted[2].trim()));
                invertedIndex.put(splitted[1], al);
              }
            }
          }
        }
        //close the reader and write the inverted index
        br.close();
        writeIndexToFile();
      }
      
    }
    
    //uses mapreduce to run a search of the inverted index
    else if(args[2].equals("2")){
     
      
     /* Scanner searchScanner = new Scanner(System.in); //read input from system.in
      System.out.println("Please enter search terms.");
      String query = searchScanner.nextLine();*/
      String search = args[3];
      String[] splitted = search.split(" ");
      
      //Move the search terms to the hdfs
      Path searchPath = new Path("query.txt");
      FileSystem fs = FileSystem.get(new Configuration());
      
      //delete anything currently occupying the output location
      fs.delete(new Path(args[1]), true);
      
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(searchPath, true)));
      for(int i=0; i<splitted.length; i++){
        bw.write(splitted[i]+"\n");
      }
      bw.close();
      
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "TinyGoogleQuery");
      job.setJarByClass(TinyGoogleMapReduce.class);
      FileInputFormat.addInputPath(job, new Path("query.txt"));
      FileOutputFormat.setOutputPath(job, new Path(args[1]+"search"));    
      job.setMapperClass(SearchMapper.class);
      job.setReducerClass(SearchReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
    //  job.addArchiveToClasspath(new Path(Path.CUR_DIR));
      
      if(job.waitForCompletion(true)){
        //read results
        Path resultsPath = new Path(args[1]+"search/part-r-00000");
        FileSystem fs2 = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs2.open(resultsPath)));
        String line= br.readLine();
        
        //get size of results for reading
        int count = 0;
        int largest = 0;
        while(line!=null){
          count++;
          String[] resultsSplit = line.split("\\s+");
          if(resultsSplit[1].length() > largest){
            largest = resultsSplit[1].length();
          }
          line = br.readLine();
        }
        br.close();
        
        //Read in results
        String[][] books = new String[count][2];
        br = new BufferedReader(new InputStreamReader(fs2.open(resultsPath)));
        line = br.readLine();
        int booksCount = 0;
        while(line!=null && booksCount < count){
          String[] resultsSplit = line.split("\\s+");
          String format = "%0"+largest+"d";
          books[booksCount][0] = String.format(format,Integer.parseInt(resultsSplit[1]));
          books[booksCount++][1] = resultsSplit[0];
          line = br.readLine();
        }
        br.close();
        
        //Sort Results
        Arrays.sort(books, new Comparator<String[]>() {
            @Override
            public int compare(final String[] entry1, final String[] entry2) {
                final String time1 = entry1[0];
                final String time2 = entry2[0];
                return time1.compareTo(time2);
            }
        });
        
        //output results to console
      /*  int count = 0;
        for(int i=count-1;i>=0;i--){
          System.out.println(books[i][0]+ " "+books[i][1]);
        }
      }*/
        //output results to json
        writeToJSON(books,splitted);
      }
    }
  
    //adds a new book to the index
    else if(args[2].equals("3")){
      //Read the hdfs to ensure current file has not been previously added
      
      //remove / from path
      String[] newBookPath = args[3].split("/");      
      Path bookPath = new Path("/TinyGoogle/books/"+newBookPath[newBookPath.length-1]);
      Path iiPath = new Path("invertedindex.txt");
      FileSystem fs = FileSystem.get(new Configuration());
      if(!fs.exists(iiPath)){
        System.err.println("Error inverted index not yet started cannot add to it");
        System.exit(-1);
      }
      if(fs.exists(bookPath)){
        System.err.println("Error book already added to index");
        System.exit(-1);
      }
      
      //if we reach here the file is not a repeat and the index exists
      //add the new book to hdfs
      fs.copyFromLocalFile(false,new Path(args[3]),bookPath);
      
      //delete anything currently occupying the output location
      fs.delete(new Path(args[1]), true);
      
      //Starts a mr job
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf,"TinyGoogleMR");
      job.setJarByClass(TinyGoogleMapReduce.class);
      FileInputFormat.addInputPath(job, bookPath);
      FileOutputFormat.setOutputPath(job, new Path(args[1]));    
      job.setMapperClass(TinyGoogleMapper.class);
      job.setReducerClass(TinyGoogleReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class); 
      
      //take job output and build index
      if(job.waitForCompletion(true)){
        Path mr_output = new Path(args[1]+"/part-r-00000");
        Path existingNode = new Path("invertedindex.txt");
        FileSystem fsmap = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fsmap.open(existingNode)));
        String line;
        line = br.readLine();
        
        //fill hashmap with existing index
        while(line!=null){
          String[] splitted = line.split("\\s+");
          ArrayList<String[]> al = new ArrayList<String[]>();
          String[] arr = new String[splitted.length-1];
          for(int i=1; i<splitted.length;i+=2){
            arr[i-1] = splitted[i];
            arr[i] = splitted[i+1];
          }
          al.add(arr);
          invertedIndex.put(splitted[0],al);
          line = br.readLine();
        }
        br.close();
        
        //Add the new book's lines
        br = new BufferedReader(new InputStreamReader(fsmap.open(mr_output)));
        line = br.readLine();
        while(line != null){
          line = br.readLine();
          if(line != null){
            String[] splitted = line.split("\\s+");
            if(splitted.length > 2){
              
              //if the word is already in the inverted index add this books words
              if(invertedIndex.containsKey(splitted[1])){
                ArrayList al = invertedIndex.get(splitted[1]);
                String[] arr = new String[2];
                //book name
                arr[0] = splitted[0];
                //number
                arr[1] = splitted[2].trim();
                al.add(arr);//splitted[0], Integer.parseInt(splitted[2].trim()));
              }
              //if the word is not in the inverted index add a place for it and store this books words
              else{
                ArrayList<String[]> al = new ArrayList<String[]>();
                String[] arr = new String[2];
                arr[0] = splitted[0];
                arr[1] = splitted[2].trim();
                al.add(arr);       //splitted[0], Integer.parseInt(splitted[2].trim()));
                invertedIndex.put(splitted[1], al);
              }
            }
          }
        }
        //close the reader and write the inverted index
        br.close();
        writeIndexToFile();
      }
    }
  } 
  public static class TinyGoogleReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context)
        throws IOException, InterruptedException {
      
      int minValue = Integer.MAX_VALUE;
      int count = 0;
      for (IntWritable value : values) {
        minValue = Math.min(minValue, value.get());
        count++;
      }
      //key = new Text(new String(key.toString() + " " +Integer.toString(minValue)));
      context.write(key, new IntWritable(count));
    }
  }
  
  public static class TinyGoogleMapper
  extends Mapper<Object, Text, Text, IntWritable> {

    private static final int MISSING = 9999;
    
    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); //get file name to append to key
      int count = 0; //count number of words processed
      
      StringTokenizer itr = new StringTokenizer(value.toString());
      while(itr.hasMoreTokens()){
        String strippedInput = itr.nextToken().toLowerCase().replaceAll("\\W", "");
        Text output = new Text(new String(fileName + " " + strippedInput));
        context.write(output,new IntWritable(1)); //write the fule name + the word + 1
      }      
    }
  }
  
  //writes the inverted index to hdfs://usr/ubuntu/invertedindex.txt
  public static void writeIndexToFile() throws Exception {
    Path pt=new Path("invertedindex.txt");
    FileSystem fs = FileSystem.get(new Configuration());
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));

    for (Map.Entry<String, ArrayList> entry : invertedIndex.entrySet()) {
      bw.write(entry.getKey()+" ");
      for(int i=0; i<entry.getValue().size(); i++){
        String[] arr = (String[])entry.getValue().get(i);
        bw.write(arr[0]+" "+arr[1]+" ");
      }
      bw.write("\n");
    }
    bw.close();
    
  }

  //Search Mapper
  public static class SearchMapper
  extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
      //load the inverted index from disk
      Path path = new Path("invertedindex.txt");
      FileSystem fs = FileSystem.get(new Configuration());
      
      //loop over each keyword
      StringTokenizer itr = new StringTokenizer(value.toString());
      while(itr.hasMoreTokens()){
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String searchTerm = itr.nextToken();
        String line = br.readLine();
        while(line != null){
          String[] splitted = line.split("\\s+");
          if(splitted[0].equals(searchTerm)){
            //write key = doc name, val = numOccurances
            for(int i=1;i<splitted.length;i+=2)
              context.write(new Text(splitted[i]),new IntWritable(Integer.parseInt(splitted[i+1])));
          }
          line = br.readLine();
        }
        //buffered reader doesnt have a rewind so close and open is what has to happen
        br.close();
      }
    }
  }
  
  //Search Reducer
  public static class SearchReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
      //count the total number of matched keywords for this book
      int sum = 0;
      for(IntWritable val : values){
        sum+=val.get();
      }
      //write key = doc, val = total keywords hit
      context.write(key,new IntWritable(sum));
    }
  }

  public static void writeToJSON(String[][] books, String[] keywords) throws IOException{
    int count =0;
  //  JSONObject obj = new JSONObject();
  //  JSONArray list = new JSONArray();
    
    //manual json because classpath issue
    String output = "{\"results\":[";
    
    //for getting keyword context
     Path bookPath;
     FileSystem fs = FileSystem.get(new Configuration());
     BufferedReader br;
    
    String context = "";
    for(int i=books.length-1; i>=0; i--){
      //get context for 1st three results
      context = "";
      if(count++<3){
        bookPath = new Path("/TinyGoogle/books/"+books[i][1]);
        br = new BufferedReader(new InputStreamReader(fs.open(bookPath)));
        String line = br.readLine();
        boolean flag = false;
        while(line!=null){
          String[] splitted = line.split("\\s+");
          for(int j=0;j<splitted.length;j++){
             String strippedWord = splitted[j].toLowerCase().replaceAll("\\W", "");
             for(int k=0;k<keywords.length;k++){
               if(keywords[k].equals(strippedWord)){
                 context = line;
                 flag = true;
                 break;
               }
             }
             if(flag){
               break;
             }
          }
          if(flag){
            break;
          }
          else{
            line = br.readLine();
          }
        }
      }
      else{
        context = "";
      }
      
      //store in manual json
      if(count == 1){
        output = output + "{ \"title\":\""+books[i][1]+"\", \"occurances\":\""+books[i][0]+"\", \"context\":\""+context+"\" }";
      }
      else{
        output = output + ", { \"title\":\""+books[i][1]+"\", \"occurances\":\""+books[i][0]+"\", \"context\":\""+context+"\" }";
      }     
    }
    output = output+" ] }";
    FileWriter fw = new FileWriter("results.json");
    fw.write(output);
    fw.flush();
  }
}