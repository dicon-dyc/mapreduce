package fenqu_tongjiliuliang;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义类实现Writable接口
 * 2.重写空参构造
 * 3.重写序列化和反序列化方法
 * 4.构造toString
 */

public class FTBean implements Writable {

    private long upflow;
    private long downflow;
    private long sumflow;

    public FTBean() {
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    public void setSumflow(){
        this.sumflow = this.upflow+this.downflow;
    }
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upflow);
        out.writeLong(downflow);
        out.writeLong(sumflow);
    }


    @Override
    public void readFields(DataInput in) throws IOException {

        this.upflow = in.readLong();
        this.downflow = in.readLong();
        this.sumflow = in.readLong();
    }

    @Override
    public String toString() {
        return  upflow + "\t" + downflow + "\t" + sumflow;
    }
}
