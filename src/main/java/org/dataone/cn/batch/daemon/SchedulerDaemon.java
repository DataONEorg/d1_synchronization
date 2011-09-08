package org.dataone.cn.batch.daemon;

/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/* @version $Id: ServiceDaemon.java 897078 2010-01-08 01:52:47Z sebb $ */
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * Implements Apache Commons Daemon interface.
 * The Class will start up a Spring Application context configured in a set location
 * The Class assumes that it will be running a Quartz scheduler  that will be shutdown
 * when the process ends.
 * 
 * @author rwaltz
 */
public class SchedulerDaemon implements Daemon {

    private ApplicationContext context;

    public SchedulerDaemon() {
        super();
        System.out.println("ServiceDaemon: instance " + this.hashCode()
                + " created");
    }

    @Override
    protected void finalize() {
        System.out.println("ServiceDaemon: instance " + this.hashCode()
                + " garbage collected");
    }

    /**
     * init and destroy were added in jakarta-tomcat-daemon.
     */
    @Override
    public void init(DaemonContext context)
            throws Exception {
        /* Set the err */
        System.out.println("ServiceDaemon: instance " + this.hashCode()
                + " init");

        System.out.println("ServiceDaemon: init done ");

    }

    @Override
    public void start() {
        /* Dump a message */
        System.out.println("ServiceDaemon: starting");

          context = new ClassPathXmlApplicationContext(new String[]{"/org/dataone/configuration/applicationContext.xml"});
          System.out.println("ServiceDaemon: started");
    }

    @Override
    public void stop() {
        /* Dump a message */
        System.out.println("ServiceDaemon: stopping");
        try {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            Collection<Scheduler> quartzSchedulers = schedulerFactory.getAllSchedulers();
            for (Scheduler scheduler : quartzSchedulers) {
                scheduler.shutdown(true);
                System.out.println("Quartz scheduler " + scheduler.getSchedulerName() + " stopped");
            }

            System.out.println("ServiceDaemon: stopped");
        } catch (SchedulerException ex) {
            System.err.println("Shutting down Quartz scheduler failed: " + ex.getMessage());
        }

    }

    @Override
    public void destroy() {
        System.out.println("ServiceDaemon: instance " + this.hashCode()
                + " destroy");
    }
}
