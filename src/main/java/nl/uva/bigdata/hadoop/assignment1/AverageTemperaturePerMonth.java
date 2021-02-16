package nl.uva.bigdata.hadoop.assignment1;


import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AverageTemperaturePerMonth extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job temperatures = prepareJob(onCluster, jobConf,
        inputPath, outputPath, TextInputFormat.class, MeasurementsMapper.class,
        YearMonthWritable.class, IntWritable.class, AveragingReducer.class, Text.class,
        NullWritable.class, TextOutputFormat.class);

    temperatures.getConfiguration().set("__UVA_minimumQuality", Double.toString(minimumQuality));

    temperatures.waitForCompletion(true);

    return 0;
  }

  static class YearMonthWritable implements WritableComparable {

    private int year;
    private int month;

    public YearMonthWritable() {}

    public int getYear() {
      return year;
    }

    public void setYear(int year) {
      this.year = year;
    }

    public int getMonth() {
      return month;
    }

    public void setMonth(int month) {
      this.month = month;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // TODO Implement me
      out.writeInt(this.year);
      out.writeInt(this.month);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // TODO Implement me
      this.year = in.readInt();
      this.month = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      YearMonthWritable that = (YearMonthWritable) o;
      return year == that.year && month == that.month;
    }

    @Override
    public int hashCode() {
      return Objects.hash(year, month);
    }

    @Override
    public int compareTo(Object o) {
      YearMonthWritable other = (YearMonthWritable) o;
      int byYear = Integer.compare(this.year, other.year);

      if (byYear == 0) {
        return Integer.compare(this.month, other.month);
      } else {
        return byYear;
      }
    }
  }

  public static class MeasurementsMapper extends Mapper<Object, Text, YearMonthWritable, IntWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("\t");
    private final static IntWritable YEAR_MONTH = new YearMonthWritable();
    private final Text TEMP = new IntWritable();
        
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
      // TODO Implement me
      
      // 1) Split columns
      String[] tokens = SEPARATOR.split(value.toString());
      int year = Integer.parseInt(tokens[0]);
      int month = Integer.parseInt(tokens[1]);
      int temp = Integer.parseInt(tokens[2]);
      int quality = Integer.parseInt(tokens[3]);
      
      // 2) get minimin quality parameter
      int min_quality = context.getConfiguration().get("__UVA_minimumQuality");

      // 3) Check if quality is over the minimum
      if(quality >= min_quality){
        
        // 4) Set Hadoop variables
        YEAR_MONTH.setYear(year);
        YEAR_MONTH.setMonth(month);
        TEMP.set(temp);

        // 5) Send to context
        context.write(YEAR_MONTH, TEMP);
      }
    }
  }

  public static class AveragingReducer extends Reducer<YearMonthWritable,IntWritable,Text,NullWritable> {

    private final Text OUTPUT = new Text();

    public void reduce(YearMonthWritable yearMonth, Iterable<IntWritable> temperatures, Context context)
            throws IOException, InterruptedException {
      // TODO Implement me
      // 1) Get year and month
      int year = yearMonth.getYear();
      int month = yearMonth.getMonth();

      // 2) Calculate average
      int totalTemp = 0
      int i = 0;
      
      // 2.1) Iterate over temperatures. Sum temperatures in totalTemp variable and count how many in i variable
      for (IntWritable temp : temperatures) {
          totalTemp += temp.get();
          i++;
      }

      // 2.2) Calculate mean
      int meanTemp = totalTemp/i;
      
      // 3) Create Output Text
      OUTPUT.set(year.toString() + "\t" + month.toString() + "\t" + meanTemp.toString());
      
      // 4) Send to context
      context.write(OUTPUT, NullWritable.get());
      }
      // Average
    }
  }
}