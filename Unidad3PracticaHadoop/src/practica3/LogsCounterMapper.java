package practica3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogsCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable ONE = new LongWritable(1);
    private Text logType = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Verificar si la línea contiene el nombre de la aplicación y [INFO], [SEVERE], [WARN]
        if (line.contains("wallet-rest-api")) {
            if (line.contains("[INFO]")) {
                logType.set("INFO");
            } else if (line.contains("[SEVERE]")) {
                logType.set("SEVERE");
            } else if (line.contains("[WARN]")) {
                logType.set("WARN");
            }

            context.write(logType, ONE);
        }
    }
}