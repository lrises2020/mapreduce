package dto;

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/9 15:04
 **/

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Writeable  hadoop的序列化接口
 * 能够排序
 * MovieBean2 必须实现  WritableComparable 接口：write()  readFields()   compareTo()
 * @author Administrator
 */


public class MovieBean2 implements WritableComparable<MovieBean> {
    //{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
    private String movie;
    private int rate;
    private String timeStamp;
    private String uid;

    @Override
    public int compareTo(MovieBean o) {
        if (o.getMovie().compareTo(this.getMovie()) == 0) {
            return o.getRate() - this.getRate();
        } else {
            return o.getMovie().compareTo(this.getMovie());
        }
        //return 0;
    }

    public void set(String movie, int rate, String timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(movie);
        out.writeInt(rate);
        out.writeUTF(timeStamp);
        out.writeUTF(uid);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movie = in.readUTF();
        rate = in.readInt();
        timeStamp = in.readUTF();
        uid = in.readUTF();
    }


    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "MovieBean [movie=" + movie + ", rate=" + rate + ", timeStamp=" + timeStamp + ", uid=" + uid + "]";
    }

    public void set(MovieBean movieBean) {
        this.movie = movieBean.getMovie();
        this.rate = movieBean.getRate();
        this.timeStamp = movieBean.getTimeStamp();
        this.uid = movieBean.getUid();

    }
}
