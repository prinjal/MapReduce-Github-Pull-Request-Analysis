package phase2;

import model.AnalysisSentiment;
import model.PatternType;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysisPartitioner extends Partitioner<AnalysisSentiment,AnalysisSentiment> {

  @Override
  public int getPartition(AnalysisSentiment analysisSentiment1, AnalysisSentiment analysisSentiment2,
      int numPartition) {
    PatternType patternType = analysisSentiment1.getAnalysisPattern().getPatternType();
    if(PatternType.LANGUAGE.equals(patternType))
      return 0;
    else if(PatternType.AUTHOR.equals(patternType))
      return 1;
    else if(PatternType.YEARLY.equals(patternType))
      return 2;
    else if(PatternType.MONTHLY.equals(patternType))
      return 3;
    else if(PatternType.WEEKLY.equals(patternType))
      return 4;
    return 0;
  }
}
