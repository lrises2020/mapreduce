package topn;

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/9 15:04
 **/

import dto.MovieBean2;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义一个分区器,将map阶段读取到的所有订单数据按照movieId分区，发送到同一个reduce。
 */
public class MyPartition extends Partitioner<MovieBean2, NullWritable> {
    /**
     * munPartitions 代表有多少个reducetask
     * key map端输出的key
     * value map端输出的value
     */
    @Override
    public int getPartition(MovieBean2 key, NullWritable value, int numPartitions) {
        return (key.getMovie().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}