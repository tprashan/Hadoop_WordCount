import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinTables extends Configured  implements Tool{

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "join tables");
        job.setJarByClass(JoinTables.class);
        job.setMapperClass(RowIdMapper.class);
        job.setReducerClass(JoinTablesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class RowIdMapper
            extends Mapper<Object,Text,Text,Text>{

        private Text rowId = new Text();

        private String[] getColumns(Text value) {
            return value.toString().split(",");
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] columns = getColumns(value);
            rowId.set(columns[0]);
            context.write(rowId,value);
        }


    }

    public static class JoinTablesReducer
            extends Reducer<Text,Text,Text,Text>{

        private String[] getColumns(Text value) {
            return value.toString().split(",");
        }

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {

            List<String> equipments = new ArrayList<String>();
            List<String> repairs = new ArrayList<String>();

            for (Text val : values) {

                String columns[] = getColumns(val);
                if(columns[2].equals("E"))
                    equipments.add(String.valueOf(val));
                else
                    repairs.add(String.valueOf(val));
            }

            for (String eq : equipments){
                Text equipment = new Text(eq);
                for (String re :repairs){
                    Text repair = new Text(re);
                    context.write(equipment,repair);
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),new JoinTables(),args);
        System.exit(exitCode);
    }
}
