package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SentimentPercent implements Writable {

    private double negativePercent;
    private double positivePercent;
    private double neutralPercent;
    public SentimentPercent() {
    }

    public SentimentPercent(double negativePercent, double positivePercent, double neutralPercent) {
        this.negativePercent = negativePercent;
        this.positivePercent = positivePercent;
        this.neutralPercent = neutralPercent;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.negativePercent);
        out.writeDouble(this.positivePercent);
        out.writeDouble(this.neutralPercent);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.negativePercent=in.readDouble();
        this.positivePercent=in.readDouble();
        this.neutralPercent=in.readDouble();
    }

    public double getNegativePercent() {
        return negativePercent;
    }

    public double getPositivePercent() {
        return positivePercent;
    }

    public double getNeutralPercent() {
        return neutralPercent;
    }

    @Override
    public String toString() {
        return String.format("%.2f,%.2f,%.2f",negativePercent,positivePercent,neutralPercent);
    }
}
