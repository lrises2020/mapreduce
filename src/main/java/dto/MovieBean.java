package dto;

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/9 14:02
 **/
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * Writable hadoop 序列化接口
 * @author root
 *
 */
public class MovieBean implements Writable{
    //{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
    private String movie;
    private int rate;
    private String timeStamp;
    private String uid;
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
    public void set(String string, int i, String string2, String string3) {
        // TODO Auto-generated method stub

    }
}