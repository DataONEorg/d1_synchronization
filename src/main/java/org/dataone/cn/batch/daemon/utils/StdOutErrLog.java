/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.daemon.utils;

import java.io.PrintStream;
import org.apache.log4j.Logger;

/**
 *
 * @author rwaltz
 */
public class StdOutErrLog {
private static final Logger logger = Logger.getLogger(StdOutErrLog.class);
     public  void init() {
        System.setOut(createLoggingProxy(System.out));
        System.setErr(createLoggingProxy(System.err));
    }
    public PrintStream createLoggingProxy(final PrintStream realPrintStream) {
        return new PrintStream(realPrintStream) {
            public void print(final String string) {
                realPrintStream.print(string);
                logger.info(string);
            }
        };
    }


}
