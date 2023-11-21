import java.io.IOException;
import model.PatternInfo;
import model.SentimentInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import phase1.SentimentMapper;
import phase1.SentimentReducer;

public class Main {

    public static void main(String[] args) {
        try{
            Configuration conf1 = new Configuration();
            String[] inputArgs = new GenericOptionsParser(conf1,args).getRemainingArgs();
            if(inputArgs.length < 2){
                System.err.println("Usage: Main <in> <out>");
                System.exit(2);
            }
            Job job1 = Job.getInstance(conf1, "Git request analyzer");
            job1.setJarByClass(Main.class);
            job1.setMapperClass(SentimentMapper.class);
            job1.setReducerClass(SentimentReducer.class);
            job1.setMapOutputKeyClass(PatternInfo.class);
            job1.setMapOutputValueClass(SentimentInfo.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, new Path(inputArgs[0]));
            FileOutputFormat.setOutputPath(job1,new Path(inputArgs[1]));
            System.exit(job1.waitForCompletion(true) ? 0:1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
