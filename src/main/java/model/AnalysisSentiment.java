package model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisSentiment implements WritableComparable<AnalysisSentiment> {

    private AnalysisPattern analysisPattern;
    private SentimentPercentage sentimentPercentage;

    public AnalysisSentiment() {
    }

    public AnalysisSentiment(AnalysisPattern analysisPattern, SentimentPercentage sentimentPercentage) {
        this.analysisPattern = analysisPattern;
        this.sentimentPercentage = sentimentPercentage;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.analysisPattern.write(out);
        this.sentimentPercentage.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            this.analysisPattern=new AnalysisPattern(PatternType.valueOf(in.readUTF()),in.readUTF());
            this.sentimentPercentage=new SentimentPercentage(in.readDouble(),in.readDouble(),in.readDouble());
        }
        catch (NullPointerException e){
            e.printStackTrace();
        }

    }

    public AnalysisPattern getAnalysisPattern() {
        return analysisPattern;
    }

    public SentimentPercentage getSentimentPercentage() {
        return sentimentPercentage;
    }

    @Override
    public String toString() {
        return "AnalysisSentiment{" +
                "analysisPattern=" + analysisPattern +
                ", sentimentPercent=" + sentimentPercentage +
                '}';
    }

    @Override
    public int compareTo(AnalysisSentiment o) {
        return this.analysisPattern.compareTo(o.analysisPattern);
    }
}
