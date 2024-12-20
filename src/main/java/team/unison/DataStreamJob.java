package team.unison;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import team.unison.model.AggregatedOutputEntry;
import team.unison.model.CsvDataEntry;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.*;

@Slf4j
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        log.info("Read source file {}", args[0]);
         final File sourceFile = new File(args[0]);
        final int ratePerSecond = args.length > 1 ? Integer.parseInt(args[1]) : 1000;

        CsvToBean<CsvDataEntry> csv = new CsvToBeanBuilder<CsvDataEntry>(new FileReader(sourceFile))
                .withType(CsvDataEntry.class)
                .build();
        // load all data to memory, just for this demo
        List<CsvDataEntry> data = csv.parse();

        log.info("Create data generator source from csv file");
        DataGeneratorSource<CsvDataEntry> source = new DataGeneratorSource<>(
                idx -> data.get(idx.intValue()),
                data.size(),
                RateLimiterStrategy.perSecond(ratePerSecond),
                TypeInformation.of(CsvDataEntry.class));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set to 1 to run task on single task manager to write result to single file
        env.setParallelism(1);
        DataStream<CsvDataEntry> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "data-source");

        inputStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<CsvDataEntry>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> System.currentTimeMillis()))
                .filter(entry -> entry.getLivingArea() != null && entry.getPrice() != null)
                .filter(entry -> entry.getLivingArea() >= 20 || entry.getPrice() >= 50000)
                // do not try to convert to lambda removing type information
                // (see details in https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/java_lambdas/)
                .keyBy(new KeySelector<CsvDataEntry, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CsvDataEntry csvDataEntry) throws Exception {
                        return Tuple2.of(csvDataEntry.getDistrict(), csvDataEntry.getType());
                    }
                })
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
                .apply(new OffersAggregator())
                .sinkTo(FileSink.forRowFormat(
                        new Path(sourceFile.getParent()),
                        new SimpleStringEncoder<AggregatedOutputEntry>())
                            .withRollingPolicy(
                                    DefaultRollingPolicy.builder()
                                            .withMaxPartSize(MemorySize.ofMebiBytes(100))
                                            .withRolloverInterval(Duration.ofHours(1))
                                            .build())
                        .build());

/* Below are unsuccessful attempts to use Table API for the same task. If there are any windowing in tables
then result calculated only at the end of stream instead of sink aggregated result after each window

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// convert data stream to table as table api fits better to our task requirements
// put only selected fields to the table and also enrich table with internal timestamp column 'rowtime'
// it is necessary for windowing processing in streaming environment
        Table inputTable = tableEnv.fromDataStream(inputStream,
                $("price"),
                $("livingArea"),
                $("district"),
                $("type"),
                $("rowtime").rowtime()
        );

// Create schema for sink to csv
        final Schema schema = Schema.newBuilder()
                .column("district", DataTypes.STRING())
                .column("type", DataTypes.STRING())
                .column("averagePrice", DataTypes.FLOAT())
                .column("averageLivingArea", DataTypes.FLOAT())
                .column("averagePricePerSqm", DataTypes.FLOAT())
                .column("totalAmount", DataTypes.BIGINT())
                .build();
// Create sink table connected to the csv file
        tableEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .partitionedBy("district", "type")
                .option("path", sourceFile.getParent() + "/result")
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", "|")
                        .build())
                .build());

        Table result =
                inputTable
                .filter(
                        or(
                                $("price").isGreaterOrEqual(50000),
                                $("livingArea").isGreaterOrEqual(20)
                        )
                )
// enrich table with new column which contains calculated price per square meter if area is not null
                .select(
                        $("district"),
                        $("type"),
                        $("price"),
                        $("livingArea"),
// use construction similar to the java ternary operation " <condition> ? <if true> : <if false> "
                        $("livingArea").isNull().then(null, $("price").dividedBy($("livingArea"))).as("pricePerSqm"),
                        $("rowtime")
                )
// group rows to the tumbling window over every 30 seconds interval by row event time
                .window(
                        Tumble.over(lit(30L).seconds()).on($("rowtime")).as("window")
                )
// group by district and type over tumbling window of 30sec
                .groupBy($("district"), $("type"), $("window"))
                .select(
                        $("district"),
                        $("type"),
                        $("price").avg().as("averagePrice"),
                        $("livingArea").avg().as("averageLivingArea"),
                        $("pricePerSqm").avg().as("averagePricePerSqm"),
                        $("rowtime").count().as("totalAmount")
                );
// use insert data to sink table to save result
                result.insertInto("CsvSinkTable").execute();


//  group and select over time window using Flink SQL API (Error on sink to csv)

//        tableEnv.executeSql("INSERT INTO CsvSinkTable SELECT " +
//                "district, " +
//                "type, " +
//                "AVG(price) as averagePrice, " +
//                "AVG(livingArea) as averageLivingArea, " +
//                "AVG(pricePerSqm) as averagePricePerSqm, " +
//                "COUNT(rowtime) as totalAmount " +
//                "FROM TABLE(TUMBLE(DATA => TABLE EnrichedTable, TIMECOL => DESCRIPTOR(rowtime), SIZE => INTERVAL '30' SECONDS)) " +
//                "GROUP BY district, type");


// sink to file from DataStream using custom encoder

//		FileSink<Row> sink = FileSink.forRowFormat(new Path(sourceFile.getParent()), (Encoder<Row>) (rowData, stream) -> {
//            for (int index = 0; index < rowData.getArity(); index++) {
//                stream.write(
//                        Optional.ofNullable(rowData.getField(index))
//                                .map(Object::toString)
//                                .orElse("NULL")
//                                .getBytes(StandardCharsets.UTF_8));
//                if (index != rowData.getArity() - 1) {
//                    stream.write(',');
//                }
//            }
//            stream.write("\n".getBytes(StandardCharsets.UTF_8));
//        }).withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withMaxPartSize(MemorySize.ofMebiBytes(100))
//                                .withRolloverInterval(Duration.ofMinutes(10))
//                                .build()
//                )
//        .build();
// Convert table env to DataStream and sink to file
//        tableEnv.toDataStream(result).sinkTo(sink);
//        tableEnv.toChangelogStream(result, schema, ChangelogMode.insertOnly()).sinkTo(sink);

*/
//         execute data processing pipeline
        env.execute("Real Estate aggregator");
    }

    public static class OffersAggregator implements WindowFunction<CsvDataEntry, AggregatedOutputEntry, Tuple2<String, String>, TimeWindow> {

        @Override
        public void apply(Tuple2<String, String> key, TimeWindow window, Iterable<CsvDataEntry> input, Collector<AggregatedOutputEntry> out) throws Exception {
            AggregatedOutputEntry outputEntry = new AggregatedOutputEntry();
            outputEntry.setDistrict(key.f0);
            outputEntry.setType(key.f1);
            float priceSum = 0;
            float livingAreaSum = 0;
            float pricePerSqmSum = 0;
            int counter = 0;
            for(CsvDataEntry item: input) {
                priceSum += item.getPrice();
                livingAreaSum += item.getLivingArea();
                pricePerSqmSum += (item.getPrice() / item.getLivingArea());
                counter++;
            }
            outputEntry.setAveragePrice(priceSum / counter);
            outputEntry.setAverageLivingArea(livingAreaSum / counter);
            outputEntry.setAveragePricePerSqm(pricePerSqmSum / counter);
            outputEntry.setTotalAmount(counter);
            out.collect(outputEntry);
        }
    }
}
