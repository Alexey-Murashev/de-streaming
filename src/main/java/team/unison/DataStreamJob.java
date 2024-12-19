package team.unison;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
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
        File sourceFile = new File(args[0]);
        CsvToBean<CsvDataEntry> csv = new CsvToBeanBuilder<CsvDataEntry>(new FileReader(sourceFile))
                .withType(CsvDataEntry.class)
                .build();
        // load all data to memory, just for this demo
        List<CsvDataEntry> data = csv.parse();

        log.info("Create data generator source from csv file");
        DataGeneratorSource<CsvDataEntry> source = new DataGeneratorSource<>(
                idx -> data.get(idx.intValue()),
                data.size(),
                RateLimiterStrategy.perSecond(1000),
                TypeInformation.of(CsvDataEntry.class));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<CsvDataEntry> inputStream = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "data-source");

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

//        result.insertInto("CsvSinkTable").execute();

// sink to file from DataStream using custom encoder

		FileSink<Row> sink = FileSink.forRowFormat(new Path(sourceFile.getParent()), (Encoder<Row>) (rowData, stream) -> {
            for (int index = 0; index < rowData.getArity(); index++) {
                stream.write(
                        Optional.ofNullable(rowData.getField(index))
                                .map(Object::toString)
                                .orElse("NULL")
                                .getBytes(StandardCharsets.UTF_8));
                if (index != rowData.getArity() - 1) {
                    stream.write(',');
                }
            }
            stream.write("\n".getBytes(StandardCharsets.UTF_8));
        }).withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(MemorySize.ofMebiBytes(100))
                                .withRolloverInterval(Duration.ofMinutes(10))
                                .build()
                )
        .build();
// Convert table env to DataStream and sink to file
//        tableEnv.toDataStream(result).sinkTo(sink);
        tableEnv.toChangelogStream(result, schema, ChangelogMode.insertOnly()).sinkTo(sink);

//         execute data processing pipeline
        env.execute("Real Estate aggregator");
    }
}
