package phase2;

import model.AnalysisSentiment;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AnalysisGroupComparator extends WritableComparator {

  public AnalysisGroupComparator() {
    super(AnalysisSentiment.class,true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    AnalysisSentiment analysisSentiment1 = (AnalysisSentiment)a;
    AnalysisSentiment analysisSentiment2 = (AnalysisSentiment)b;
    return analysisSentiment1.compareTo(analysisSentiment2);
  }
}
