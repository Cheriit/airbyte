package io.airbyte.integrations.source.arrow;

import java.io.File;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(URLFile.class);


    private String url;
    private Map provider;
    private File file;

    public URLFile(String url,Map provider){
        this.provider=provider;
        this.url=url;
        this.file=null;

    }


    public String storageScheme(){
        //Convert Storage Names to the proper URL Prefix:return: the corresponding URL prefix / scheme
         String storageName =this.provider.get("storage").toString().toUpperCase();

        if (storageName.equals("LOCAL")){
            return "file://";
        }else{
            LOGGER.error("Unknown Storage provider");
            return "";
        }
    }

    public String getFullUrl(){
        return storageScheme()+this.url;
    }
}
