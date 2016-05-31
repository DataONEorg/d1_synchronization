/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.log;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.OptionHandler;

/**
 * For any log entry, return an array representing just the message
 * 
 * So a simple message such as log.info("Hello") will return
 * {"Hello World"} when the getOutputArray() is called
 * 
 * 
 * @author waltz
 */
public class MockArrayWriterAppender extends WriterAppender implements Appender,OptionHandler{
    private static ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    
    // XXX We may wish to move this class to the test resources component
    // if it is needed by other projects
    
    public MockArrayWriterAppender() {
        Writer writerOut = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);

        this.layout = new PatternLayout("%m%n");
        this.setWriter(writerOut);
    }
    
    public MockArrayWriterAppender(Layout layout, OutputStream os) {
        super(layout, os);
    }
    public MockArrayWriterAppender(Layout layout, Writer writer) {
        super(layout, writer);
    }

    /**
     * return a String array representation of all Logged events.
     * 
     * @return 
     */
    public static String[] getOutputArray() {
        // System.lineSeparator should work for all cases, but if it does not
        // for some reason, define the PatternLayout to end with some
        // string that would generally not occur in a log message like "\x1E"
        
        return outputStream.toString().split(System.lineSeparator());
    }
    public static void logReset() {
        outputStream.reset();
    }
}
