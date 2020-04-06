package org.apache.rocketmq.logging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.rocketmq.logging.inner.Level;
import org.apache.rocketmq.logging.inner.Logger;
import org.apache.rocketmq.logging.inner.LoggingEvent;
import org.junit.After;
import org.junit.Before;

public class BasicLoggerTest {

    protected Logger logger = Logger.getLogger("test");

    protected LoggingEvent loggingEvent;

    protected String loggingDir = System.getProperty("user.home") + "/logs/rocketmq-test";

    @Before
    public void createLoggingEvent() {
        loggingEvent = new LoggingEvent(Logger.class.getName(), logger, Level.INFO, "junit test error", new RuntimeException("createLogging error"));
    }

    public String readFile(String file) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        FileInputStream fileInputStream = new FileInputStream(file);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        String line = bufferedReader.readLine();
        while (line != null) {
            stringBuilder.append(line);
            stringBuilder.append("\r\n");
            line = bufferedReader.readLine();
        }
        bufferedReader.close();
        return stringBuilder.toString();
    }

    @After
    public void clean() {
        File file = new File(loggingDir);
        if (file.exists()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                file1.delete();
            }
        }
    }
}
