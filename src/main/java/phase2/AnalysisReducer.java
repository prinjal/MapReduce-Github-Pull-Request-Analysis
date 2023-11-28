package phase2;

import java.io.IOException;
import model.AnalysisSentiment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducer extends Reducer<AnalysisSentiment,AnalysisSentiment, Text,Text> {

  @Override
  protected void reduce(AnalysisSentiment key, Iterable<AnalysisSentiment> values,
      Reducer<AnalysisSentiment, AnalysisSentiment, Text, Text>.Context context)
      throws IOException, InterruptedException {
    for(AnalysisSentiment analysisSentiment: values)
      context.write(new Text(analysisSentiment.getAnalysisPattern().getPatternValue()), new Text(analysisSentiment.getSentimentPercentage().toString()));
  }
}
