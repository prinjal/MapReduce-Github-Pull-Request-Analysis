import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileConvertor {
    public static void main(String[] args) throws IOException, CsvValidationException {
        String outputFilePath = "outputfile_path";

        List<String> inputFiles = new ArrayList<>();
//    inputFiles.add("/Users/midhungopalakrishnan/Desktop/Reshmi/NeuClasses/Fall2023/ParallelDataProcess/Project/Data/archive-9/Sample1.csv");
//    inputFiles.add("/Users/midhungopalakrishnan/Desktop/Reshmi/NeuClasses/Fall2023/ParallelDataProcess/Project/Data/archive-9/sample.csv");
//    inputFiles.add("/Users/midhungopalakrishnan/Desktop/Reshmi/NeuClasses/Fall2023/ParallelDataProcess/Project/Data/archive-9/first_10000_1.csv");
//    inputFiles.add("/Users/midhungopalakrishnan/Desktop/Reshmi/NeuClasses/Fall2023/ParallelDataProcess/Project/Data/archive-9/ghtorrent-2019-01-07.csv");
//    inputFiles.add("/Users/midhungopalakrishnan/Desktop/Reshmi/NeuClasses/Fall2023/ParallelDataProcess/Project/Data/archive-9/ghtorrent-2019-02-04.csv");
//    inputFiles.add("/Users/midhungopalakrishnan/Desktop/Reshmi/NeuClasses/Fall2023/ParallelDataProcess/Project/Data/archive-9/ghtorrent-2019-03-11.csv");
        inputFiles.add("inputfile_path");
//        inputFiles.add("/Users/midhungopalakrishnan/Desktop/Reshmi/NeuClasses/Fall2023/ParallelDataProcess/Project/Data/archive-9/ghtorrent-2019-05-20.csv");

        for(String input: inputFiles) {
            System.out.println("Processing "+input);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(input));
            String line;
            String[] nameArr = input.split("/");
            String name = outputFilePath + nameArr[nameArr.length-1];
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(name));
            bufferedReader.readLine();//skip header
            while ((line = bufferedReader.readLine()) != null) {
                String[] lineArr = line.split(",");
                if(lineArr.length > 11){
                    int commentStart = line.indexOf(lineArr[3]);
                    int commentEnd = line.indexOf(lineArr[lineArr.length-7],commentStart)-1;
                    //System.out.println("\nline: "+line+"  len: "+lineArr.length+" val: "+lineArr[lineArr.length-7]);
                    //System.out.println("commentStart: "+commentStart+"  commentEnd: "+commentEnd);
                    String comment = line.substring(commentStart,commentEnd);
                    comment = comment.replace("\"","");
                    String line1 = line.substring(0,commentStart) + "\"" + comment + "\"" + line.substring(commentEnd);
                    line = line1;
                    //System.out.println("modified: "+line);
                }
                line += "\n";
                bufferedWriter.write(line);
            }
            bufferedReader.close();
            bufferedWriter.close();
        }
    }
}
