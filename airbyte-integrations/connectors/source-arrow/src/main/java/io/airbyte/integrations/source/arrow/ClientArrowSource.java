package io.airbyte.integrations.source.arrow;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.client.json.Json;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

public class ClientArrowSource {


    private URLFile readerClass;
    private String datasetName;
    private Map provider;
    private String url;
    private String readerFormat;
    public ClientArrowSource(String datasetName,String url,Map provider,String format ) {
        this.datasetName = datasetName;
        this.url = url;
        this.provider = provider;
        if (format != null) {
            this.readerFormat = format;
        } else {
            this.readerFormat = "csv";
        }

    }


    public String streamName(){
        if (this.datasetName!= null && !this.datasetName.equals("")){
            return this.datasetName;
        }
        return "";
    }

    private Iterable<Map> read(Iterable fields){
        //TODO

    }

    private Iterable loadDataframes( File fp){
        //TODO

    }

    public static void readArrow(File file){
        try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)){

            try (FileInputStream fileInputStream = new FileInputStream(file);
                 ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
            ){
                System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
                for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                    reader.loadRecordBatch(arrowBlock);
                    VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                    System.out.print(vectorSchemaRootRecover.contentToTSVString());

                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



}
