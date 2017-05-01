//Spark Code for CS1699 Final Project
//compiled using maven, impossible from CLI to best of my knowledge

//Uses hadoop library calls for file system
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//java libraries for various file manipulations
import java.util.*;
import java.io.*;
import java.lang.Iterable;

//Spark Libraries for actual functionality
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class TinyGoogleSpark{


  //hashmap representing inverted index
  static HashMap <String, ArrayList> invertedIndex = new HashMap <String, ArrayList>();

  public static void main(String[] args) throws Exception{
    //spark context
    SparkConf sparkconf = new SparkConf().setAppName("TinyGoogleSpark").setMaster("yarn");
    JavaSparkContext sc = new JavaSparkContext(sparkconf);
    //use spark to build inverted index
    if(args[2].equals("1")){
      FileSystem fs = FileSystem.get(new Configuration());
      fs.delete(new Path(args[1]),true);


      String inputFile = args[0];
      String outputFile = args[1]+"/spark";

      //load book data
      JavaPairRDD<String,String> input2 = sc.wholeTextFiles(inputFile);

      //Split book data using lambdas
      JavaPairRDD<String, String> words = input2.flatMapValues(
        new Function<String,Iterable<String>>(){
          @Override
          public Iterable<String> call(String s){
            //strip punctuation and decapitalize words
            String[] splitted = s.split("\\s+");
            for(int i=0;i<splitted.length;i++){
              splitted[i] = splitted[i].toLowerCase().replaceAll("\\W","");
            }
            return Arrays.asList(splitted);
          }
        });

        //map to filename and word again using lambdas
        JavaRDD<String> count = words.map(new Function<Tuple2<String, String>, String>(){
          @Override
          public String call(Tuple2<String,String> fileName) throws Exception{
            return fileName._1() +" "+fileName._2();
          }
        });

        //transform map into filename word and 1 and then reduce
        JavaPairRDD<String, Integer> counts = count.mapToPair(
          new PairFunction<String, String, Integer>(){
            public Tuple2<String,Integer> call(String x){
              return new Tuple2(x+" ",1);
            }
          }).reduceByKey(new Function2<Integer,Integer,Integer>(){
            public Integer call(Integer a, Integer b){return a+b;}
          });

        JavaPairRDD<String,Integer> results = counts.coalesce(1);
        results.saveAsTextFile(outputFile);

        //generate inverted index
        Path sparkoutput = new Path(args[1]+"/spark/part-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(sparkoutput)));
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

    else if(args[2].equals("2")){
      /* Scanner searchScanner = new Scanner(System.in); //read input from system.in
       System.out.println("Please enter search terms.");
       String query = searchScanner.nextLine();*/
       String search = args[3];
       System.out.println(search);
       String[] splitted = search.split(" ");

       //Move the search terms to the hdfs
       Path searchPath = new Path("query.txt");
       FileSystem fs = FileSystem.get(new Configuration());

       //delete anything currently occupying the output location
    //   fs.delete(new Path(args[1]), true);

       BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(searchPath, true)));
       for(int i=0; i<splitted.length; i++){
         bw.write(splitted[i]+"\n");
       }
       bw.close();

       System.out.println("hdfs stiff complete");

       // Load our query with yet another lambda
       JavaRDD<String> queryRDD = sc.textFile("query.txt");
       JavaRDD<String> words = queryRDD.flatMap(new FlatMapFunction<String, String>() {
         @Override public Iterator<String> call(String s) {
           return Arrays.asList(s.split("\\r?\\n")).iterator();
         }
       });

       //Read each line of the invertedindex and at each line generate a tuple for
       //words that match any part of the query text using another lambda
       JavaPairRDD<String, String> pairs = words.mapToPair(new PairFunction<String, String, String>() {
         public Tuple2<String, String> call(String s) throws IOException{
           Path pt=new Path("invertedindex.txt");
           FileSystem fs = FileSystem.get(new Configuration());
           BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
           String queryWord = s;
           String line=br.readLine();
           String str = "";

           while (line != null){
             String [] parts = line.split(" ");
             if(parts[0].equals(queryWord.toLowerCase().replaceAll("\\W",""))){
               for(int i=1; i<parts.length; i+=2){
                 //write key=document, value = number
                 String[] arr = new String[2];
                 str = str + parts[i] + " " + parts[i+1].trim()+ " ";
               }
             }
             line = br.readLine();
           }
           br.close();
           return new Tuple2<String, String>(s, str);
         }
       });

       //Reduces the tuples down to a map of document:occurance pairs
       //for once is not using a lambda!
       //collect the values
       JavaRDD<String> vals = pairs.values();
       List<String> result = vals.collect();
       //Define Maps to use for the output
       HashMap<String, Integer> done = new HashMap<String, Integer>();
       //second map that has number:doc pair in reverse order for processing
       TreeMap<Integer, String> results = new TreeMap<Integer, String>(Collections.reverseOrder());

       //parse the values into the map for output
       for(int i=0; i<result.size(); i++){
         String[] line = result.get(i).split("\\s+");
         for(int j=0;j<line.length;j+=2)
         {
           if(line.length >= 2){
             if(done.containsKey(line[j])){
               int sum = done.get(line[j]);
               sum = sum + Integer.parseInt(line[j+1].trim());
               done.put(line[j], sum);
             }
             else{
               done.put(line[j], Integer.parseInt(line[j+1].trim()));
             }
           }
         }
       }

       //move from doc:value map to value:doc map
       for(Map.Entry<String,Integer> entry : done.entrySet()){
         results.put(entry.getValue(),entry.getKey());
       }

       //use the correctly sorted tree map to generate arrays that can be processed to generate output
       String[][] books = new String[results.size()][2];
       int i = 0;
       for(Map.Entry<Integer,String> entry : results.entrySet()){
         books[results.size()-1-i][0] = String.valueOf(entry.getKey());
         books[results.size()-1-i++][1] = entry.getValue();
       }

       //strip capitalization and punctuation from queries
       List<String> temp = words.collect();
       String[] queryWords = temp.toArray(new String[temp.size()]);
       for(int j =0;i<queryWords.length;j++){
         queryWords[j] = queryWords[j].toLowerCase().replaceAll("\\W","");
       }
       //generate JSON results
       writeToJSON(books,queryWords);
    }
  }

  public static void writeToJSON(String[][] books, String[] keywords) throws IOException{
    int count =0;
    System.out.println("Jsoning Results");
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
    Path pt=new Path("results.json");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
    bw.write(output);
    bw.close();
    System.out.println("JSON RESULTS: "+output);
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
        String[] splitted = arr[0].split("/");
        String to_print = splitted[splitted.length-1].replaceAll("\\W", "");
        bw.write(to_print+" "+arr[1].replaceAll("\\W","")+" ");
      }
      bw.write("\n");
    }
    bw.close();
  }
}
