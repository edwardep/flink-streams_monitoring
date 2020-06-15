package utils;

import datatypes.InputRecord;
import datatypes.WCStruct;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;

public class DataSetTransformer {

    String input_path = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
    private FileReader file;
    private BufferedReader reader;
    private FileReader file2;
    private BufferedReader reader2;

    private int windowSize = 1000;
    private long firstTs;

    private boolean isRunning = true;

    public DataSetTransformer() throws FileNotFoundException, IOException {
        file = new FileReader(input_path);
        file2 = new FileReader(input_path);
        reader = new BufferedReader(file);
        reader2 = new BufferedReader(file2);

        String[] tokens;
        if (reader2.ready()) {
            tokens = reader2.readLine().split(";");
            firstTs = Long.parseLong(tokens[WCStruct.timestamp.ordinal()]);
        }

        execute();
    }


    private void execute() throws IOException {
        int count = 0;

        while (isRunning) {
            String[] tokens;
            if (reader.ready() && reader2.ready()) {
                tokens = reader.readLine().split(";");

                // multiplying by 1000 because world cup dataset has unix timestamps in seconds
                long timestampMillis = Long.parseLong(tokens[WCStruct.timestamp.ordinal()]);

                StringBuilder concat = new StringBuilder();
                for(String tok : tokens) concat.append(tok).append(";");
                concat.append(1.0);

                writeToText(concat.toString()+"\n","D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1_slide.txt");


                while (timestampMillis - firstTs >= windowSize) {
                    tokens = reader2.readLine().split(";");
                    firstTs = Long.parseLong(tokens[WCStruct.timestamp.ordinal()]);

                    concat = new StringBuilder();
                    concat.append(timestampMillis).append(";");
                    for(int i = 1; i < tokens.length; i++) concat.append(tokens[i]).append(";");
                    concat.append(-1.0);

                    writeToText(concat.toString()+"\n","D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1_slide.txt");

                    count++;
                }

                count++;
                if (count % 100000 == 0) System.out.println(count);

            } else {
                cancel();
            }
        }

    }
    private void cancel() {
        isRunning = false;
        try {
            file.close();
            file2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            reader.close();
            reader2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToText(String string, String filename) {
        try {
            FileWriter myWriter = new FileWriter(filename, true);
            myWriter.write(string);
            myWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
