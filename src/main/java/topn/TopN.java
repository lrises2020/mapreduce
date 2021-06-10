package topn;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

import dto.MovieBean;
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

import com.alibaba.fastjson.JSON;
/**
 * {"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
 * {"movie":"661","rate":"3","timeStamp":"978302109","uid":"1"}
 * {"movie":"914","rate":"3","timeStamp":"978301968","uid":"1"}
 * {"movie":"3408","rate":"4","timeStamp":"978300275","uid":"1"}
 * {"movie":"2355","rate":"5","timeStamp":"978824291","uid":"1"}
 * {"movie":"1197","rate":"3","timeStamp":"978302268","uid":"1"}
 * {"movie":"1287","rate":"5","timeStamp":"978302039","uid":"1"}
 * {"movie":"2804","rate":"5","timeStamp":"978300719","uid":"1"}
 * {"movie":"594","rate":"4","timeStamp":"978302268","uid":"1"}
 * {"movie":"919","rate":"4","timeStamp":"978301368","uid":"1"}
 * {"movie":"595","rate":"5","timeStamp":"978824268","uid":"1"}
 * {"movie":"938","rate":"4","timeStamp":"978301752","uid":"1"}
 * {"movie":"2398","rate":"4","timeStamp":"978302281","uid":"1"}
 * {"movie":"2918","rate":"4","timeStamp":"978302124","uid":"1"}
 * 该方式利用小顶堆和集合重复原理的方式 , 每过来一个数据 , 跟堆顶数据进行比较 , 如果比最小的大 , 则踢掉换新的 , 否则直接跳过数据 . 以此对数据进行排序
 */
public class TopN {

    public static class MapTask extends Mapper<LongWritable, Text, Text, MovieBean> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieBean>.Context context) throws IOException, InterruptedException {
            try {
                MovieBean movieBean = JSON.parseObject(value.toString(), MovieBean.class);
                String movie = movieBean.getMovie();
                context.write(new Text(movie), movieBean);
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
    }

    public static class ReduceTask extends Reducer<Text, MovieBean, MovieBean, NullWritable> {
        @Override
        protected void reduce(Text movieId, Iterable<MovieBean> movieBeans, Reducer<Text, MovieBean, MovieBean, NullWritable>.Context context) throws IOException, InterruptedException {
            TreeSet<MovieBean> tree = new TreeSet<>(new Comparator<MovieBean>() {
                @Override
                public int compare(MovieBean o1, MovieBean o2) {
                    if (o1.getRate() - o2.getRate() == 0) {
                        return o1.getUid().compareTo(o2.getUid());
                    } else {
                        return o1.getRate() - o2.getRate();
                    }
                }
            });
            for (MovieBean movieBean : movieBeans) {
                MovieBean movieBean2 = new MovieBean();
                movieBean2.set(movieBean);
                if (tree.size() <= 2) {
                    tree.add(movieBean2);
                } else {
                    MovieBean first = tree.first();
                    if (first.getRate() < movieBean2.getRate()) {
                        //做替换
                        tree.remove(first);
                        tree.add(movieBean2);
                    }
                }
            }
            for (MovieBean movieBean : tree) {
                context.write(movieBean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "topN2");

        //设置map和reduce，以及提交的jar
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(TopN.class);

        //设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);

        job.setOutputKeyClass(MovieBean.class);
        job.setOutputValueClass(NullWritable.class);

        //输入和输出目录
        FileInputFormat.addInputPath(job, new Path("E:/data/rating.json"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\data\\out\\topN2"));

        //判断文件是否存在
        File file = new File("E:\\data\\out\\topN2");
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //提交任务
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion ? "你很优秀！！！" : "滚去调bug！！");

    }
}