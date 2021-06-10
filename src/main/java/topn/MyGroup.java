package topn;

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/9 15:07
 **/

import dto.MovieBean2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器：movieid相同的kv聚合成组，然后取第一个即是最大值。
 *
 * @author root
 */
public class MyGroup extends WritableComparator {

    public MyGroup() {
        super(MovieBean2.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MovieBean2 bean1 = (MovieBean2) a;
        MovieBean2 bean2 = (MovieBean2) b;
        return bean1.getMovie().compareTo(bean2.getMovie());
    }
}