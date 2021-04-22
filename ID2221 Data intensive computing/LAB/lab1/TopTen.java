package id2221.topten;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }
    //     List<String> list = new ArrayList<String>();
    //     for (String s: map.values()){
    //         list.add(s);
    //     }
    //     for(int i=0; i<2; i++) {
    //         System.out.println(list.get(i));
    //     }
        return map;
     }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();
        // Parses XML file
        Map<String, String> map_input =  new HashMap<String, String>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            map_input = transformXmlToMap(value.toString());
            if(map_input.get("Id") != null && !map_input.get("Id").equals("") && map_input.get("Reputation") != null && !map_input.get("Reputation").equals("")){
                 Integer reputation = Integer.parseInt(map_input.get("Reputation"));
                 String id = map_input.get("Id");
                 String content = id + "_" + reputation;
                 repToRecordMap.put(reputation,content);
                 if(repToRecordMap.size()>10){
                    repToRecordMap.remove(repToRecordMap.firstKey());
                  }
            }
            //System.out.println(repToRecordMap.values());
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            for(String text:repToRecordMap.values()){
                if (text.toString() != null && !text.toString().equals("")) {
                    context.write(NullWritable.get(), new Text(text));
                }
            }
        }
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
        // Stores a map of user reputation to the record
        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        for(Text item: values){
	            String value[] = item.toString().split("_");
	            Integer reputation = Integer.parseInt(value[1]);
	            repToRecordMap.put(reputation,item.toString());
	            if(repToRecordMap.size()>10){
	                repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }

            int id = 0;

            // Put insHBase = new Put(Bytes.toBytes("row"));
	        for(String text: repToRecordMap.values()){
                String value[] = text.toString().split("_"); 
                Put insHBase = new Put(Bytes.toBytes("row"+id));
                id = id + 1; 
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(value[0]));
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("reputation"), Bytes.toBytes(value[1]));
                context.write(null, insHBase);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuration conf = new Configuration();
        Configuration conf = HBaseConfiguration.create();

        // define scan and define column families to scan
        Scan scan = new Scan(); 
        scan.addFamily(Bytes.toBytes("info"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(TopTen.class);
    
        job.setMapperClass(TopTenMapper.class);
        //job.setCombinerClass(TopTenReducer.class);
       // job.setReducerClass(TopTenReducer.class);

        // define output table

        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
        job.setNumReduceTasks(1);

    
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        job.waitForCompletion(true);
  }
}
