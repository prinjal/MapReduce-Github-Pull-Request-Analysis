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

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public String toString() {
        return "SentimentPercent{" +
                "negativePercent=" + negativePercent +
                ", positivePercent=" + positivePercent +
                ", neutralPercent=" + neutralPercent +
                '}';
    }
}
