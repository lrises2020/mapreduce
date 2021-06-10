package topn;

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/9 15:09
 **/

import java.io.File;
import java.io.IOException;

import dto.MovieBean2;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;


public class TopN2 {
    public static class MapTask extends Mapper<LongWritable, Text, MovieBean2, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MovieBean2, NullWritable>.Context context) throws IOException, InterruptedException {
            try {
                ObjectMapper mapper = new ObjectMapper();
                MovieBean2 bean = mapper.readValue(value.toString(), MovieBean2.class);
                context.write(bean, NullWritable.get());
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
    }

    public static class ReduceTask extends Reducer<MovieBean2, NullWritable, MovieBean2, NullWritable> {
        @SuppressWarnings("unused")
        @Override
        protected void reduce(MovieBean2 key, Iterable<NullWritable> values, Reducer<MovieBean2, NullWritable, MovieBean2, NullWritable>.Context context) throws IOException, InterruptedException {
            int num = 0;
            //虽然是一个空的，但是key能够根据迭代进行相应的得到对应空值的结果
            for (NullWritable nullWritable : values) {
                if (num >= 2) {
                    break;
                }
                num++;
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "topn3");

        //设置map和reduce，以及提交的jar
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(TopN2.class);
        //job.setNumReduceTasks(2);
        job.setPartitionerClass(MyPartition.class);
        job.setGroupingComparatorClass(MyGroup.class);


        //设置输入输出类型
        job.setMapOutputKeyClass(MovieBean2.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(MovieBean2.class);
        job.setOutputValueClass(NullWritable.class);

        //输入和输出目录
        FileInputFormat.addInputPath(job, new Path("E:/data/rating.json"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\data\\out\\topN3"));

        //判断文件是否存在
        File file = new File("E:\\data\\out\\topN3");
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //提交任务
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion ? "你很优秀！！！" : "滚去调bug！！");
    }
}