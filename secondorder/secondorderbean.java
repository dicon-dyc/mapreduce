package secondorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class secondorderbean implements WritableComparable<secondorderbean> {

    int first;

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    int second;

    @Override
    public int compareTo(@NotNull secondorderbean o) {
        if (first!=o.first){
            return first < o.first ? 1:-1;
        }else if (second != o.second){
            return second < o.second ? -1 :1;
        }else{
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public String toString() {
        return "secondorderbean{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
