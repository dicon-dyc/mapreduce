package xuliehua_tongjiliuliang;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参构造
 * 4，toString方法
 */
public class FlowBean implements Writable {
    private long upflow;
    private long downflow;
    private long sumflow;

    //空参构造
    public FlowBean() {
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
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumflow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + sumflow;
    }
}
