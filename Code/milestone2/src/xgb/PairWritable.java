package xgb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable<PairWritable> {

    private Text L;
    private Text R;

    public PairWritable() {
        set(new Text(), new Text());
    }

    public PairWritable(Text L, Text R) {
        set(L, R);
    }
    
    public PairWritable(PairWritable p) {
        set(p.getL(), p.getR());
    }
    
    public void set(Text L, Text R) {
        this.L = L;
        this.R = R;
    }

    public Text getL() {
        return L;
    }

    public Text getR() {
        return R;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        L.readFields(in);
        R.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        L.write(out);
        R.write(out);
    }

    @Override
    public int compareTo(PairWritable o) {
        int res = L.compareTo(o.L);
        if (res != 0) {
            return res;
        }
        return R.compareTo(o.R);

    }
    @Override
    public String toString() {
        String res = L.toString()+","+R.toString();
        return res;
    }

}