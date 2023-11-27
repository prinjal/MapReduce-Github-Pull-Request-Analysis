package phase2;

import model.AnalysisSentiment;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AnalysisSortComparator extends WritableComparator {

  public AnalysisSortComparator() {
    super(AnalysisSentiment.class,true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    AnalysisSentiment analysisSentiment1 = (AnalysisSentiment)a;
    AnalysisSentiment analysisSentiment2 = (AnalysisSentiment)b;
    int compareType = analysisSentiment1.compareTo(analysisSentiment2);
    if(compareType != 0)//pattern type is different
      return compareType;
    double compareSentiment = (analysisSentiment2.getSentimentPercentage().getNegativePercent() - analysisSentiment1.getSentimentPercentage().getNegativePercent());
    if(compareSentiment != 0)
      return compareSentiment < 0 ? -1 : 1;
    return analysisSentiment1.getAnalysisPattern().getPatternValue()
        .compareTo(analysisSentiment2.getAnalysisPattern().getPatternValue());
  }
}
