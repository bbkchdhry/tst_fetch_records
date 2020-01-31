package org.bigmart.sink;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

public class Launcher {
    public static Config config = ConfigFactory.parseFile(new File("/etc/bigmart_fetchdata/fetch.conf"));

    public static void main(String[] args) throws InterruptedException {
        new HttpRequestBigmart().request();
    }
}
