package phase2;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvMalformedLineException;
import com.opencsv.exceptions.CsvValidationException;
import model.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;

import static util.Constants.*;

public class AnalysisMapper extends Mapper<LongWritable, Text, AnalysisSentiment, AnalysisSentiment> {

    protected void map(LongWritable key, Text value,
        Mapper<LongWritable, Text, AnalysisSentiment, AnalysisSentiment>.Context context)
        throws IOException, InterruptedException {
        try{
            CSVReader csvReader = new CSVReader(new StringReader(value.toString()));
            while(csvReader.peek() != null){
                String[] recordArr = csvReader.readNext();
                String patternType = recordArr[PATTERN_TYPE];
                String patternValue = recordArr[PATTERN_VALUE];
                String negativePercent = recordArr[NEGATIVE_PERCENT];
                String positivePercent = recordArr[POSITIVE_PERCENT];
                String neutralPercent = recordArr[NEUTRAL_PERCENT];

                AnalysisPattern analysisPattern=new AnalysisPattern(PatternType.valueOf(patternType),patternValue);
                SentimentPercentage sentimentPercentage=new SentimentPercentage(Double.parseDouble(negativePercent),
                        Double.parseDouble(positivePercent),
                        Double.parseDouble(neutralPercent));

                AnalysisSentiment analysisSentiment=new AnalysisSentiment(analysisPattern,sentimentPercentage);

                context.write(analysisSentiment,analysisSentiment);

            }
        } catch (CsvValidationException | CsvMalformedLineException | RuntimeException e) {
        e.printStackTrace();//skip errors
        }
    }
}
