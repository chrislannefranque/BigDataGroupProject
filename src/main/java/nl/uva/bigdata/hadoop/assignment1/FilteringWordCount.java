package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class FilteringWordCount extends HadoopJob {

    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String, String> parsedArgs = parseArgs(args);

        Path inputPath = new Path(parsedArgs.get("--input"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job wordCount = prepareJob(onCluster, jobConf, inputPath, outputPath, TextInputFormat.class,
                FilteringWordCount.TokenizerMapper.class, Text.class, IntWritable.class,
                FilteringWordCount.IntSumReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
        wordCount.waitForCompletion(true);

        return 0;
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {​​
            String token_1 = "to";
            String token_2 = "the";
            String new_string = value.toString().replace(token_1, "");
            String new_string_2 = new_string.replace(token_2, "");
            StringTokenizer tokenizer = new StringTokenizer(new_string_2);
            while (tokenizer.hasMoreTokens()) {​​
                word.set(tokenizer.nextToken().toLowerCase());
                context.write(word, ONE);
            }​​
        }​​
        
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {​​

        private final IntWritable result = new IntWritable();
        private final Text OUTPUT = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {​​
    
            int sum = 0;
            for (IntWritable val : values) {​​
                sum += val.get();
            }​​
    
            result.set(sum);
            OUTPUT.set(key.toString() + "\t" + result.get());
            OUTPUT.set(key.toString() + "\t" + sum);
            context.write(OUTPUT, NullWritable.get()); //needs to be textual output with tab
        }​
}
