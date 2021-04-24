package quanpaixu_tongjiliuliang;

import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class QTBean implements WritableComparable<QTBean> {
    private long upflow;
    private long downflow;
    private long sumflow;

    public QTBean() {
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
        this.sumflow = this.upflow + this.downflow;
    }

    @Override
    public int compareTo(@NotNull QTBean o) {

        //按照总流量的倒序排序
        if (this.sumflow > o.sumflow){
            return -1;
        }else if(this.sumflow < o.sumflow){
            return 1;
        }else{
            return 0;
        }
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
        return upflow +"\t"+ downflow +"\t"+ sumflow;
    }
}
