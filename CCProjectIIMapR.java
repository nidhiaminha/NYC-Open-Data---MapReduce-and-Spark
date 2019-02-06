import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class CCProjectIIMapR {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable Date_max_accidents = new IntWritable(1);
    private IntWritable Fatality = new IntWritable();
    private IntWritable Number_persons_pedes_Injured = new IntWritable();
    private IntWritable Number_persons_pedes_Killed = new IntWritable();
    private IntWritable Number_cyclists_IK = new IntWritable();
    private IntWritable Number_motorists_Ik = new IntWritable();
    Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
     {
      String line = value.toString();
      String[] word_array = line.split(",");
      int Cal_NumberofAccidents=0,Accident_fatality=0,Cal_Numberof_PerPed_Injured=0,Cal_Numberof_PerPed_Killed=0,
      Cal_Numberof_Cyc_IK=0,Cal_Numberof_Mot_IK=0,j=0;
      context.write(new Text("Date_"+word_array[0].toString()),Date_max_accidents);

    for(int i=4;i<11;i=i+2)
      {
        Accident_fatality=Accident_fatality+Integer.parseInt(word_array[i].trim());
      }
      
         Fatality.set(Accident_fatality);
         context.write(new Text("B_Fatal"+word_array[1].toString()),Fatality);
         context.write(new Text("Z_Fatal"+word_array[2].toString()),Fatality);
         context.write(new Text("Veh_type"+word_array[11].toString()),Date_max_accidents);
         
         for(int i=3;i<11;i++)
      { 
        if(i==3 || i==5)  {Cal_Numberof_PerPed_Injured=Cal_Numberof_PerPed_Injured+Integer.parseInt(word_array[i].toString());}
        if(i==4 || i==6){Cal_Numberof_PerPed_Killed=Cal_Numberof_PerPed_Killed+Integer.parseInt(word_array[i].toString());}
        if(i==7 || i==8){Cal_Numberof_Cyc_IK=Cal_Numberof_Cyc_IK+Integer.parseInt(word_array[i].toString());}
        if(i==9 || i==10){Cal_Numberof_Mot_IK=Cal_Numberof_Mot_IK+Integer.parseInt(word_array[i].toString());}
      }
    Number_persons_pedes_Injured.set(Cal_Numberof_PerPed_Injured);
    Number_persons_pedes_Killed.set(Cal_Numberof_PerPed_Killed);
    Number_cyclists_IK.set(Cal_Numberof_Cyc_IK);
    Number_motorists_Ik.set(Cal_Numberof_Mot_IK);

    String[] yr= word_array[0].split("/");
    context.write(new Text(("Num_pp_inj"+yr[2].toString())),Number_persons_pedes_Injured);
    context.write(new Text(("Num_pp_kil"+yr[2].toString())),Number_persons_pedes_Killed);
    context.write(new Text(("Num_c_ik"+yr[2].toString())),Number_cyclists_IK);
    context.write(new Text(("Num_m_ikol"+yr[2].toString())),Number_motorists_Ik);
    }
  }
    
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(CCProjectIIMapR.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}