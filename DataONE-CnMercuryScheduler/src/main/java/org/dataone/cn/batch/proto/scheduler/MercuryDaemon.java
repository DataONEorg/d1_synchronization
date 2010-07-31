package org.dataone.cn.batch.proto.scheduler;

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
import java.io.*;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

/**
 *
 * @author rwaltz
 */

public class MercuryDaemon  implements Daemon {
    private String appContextLocation = "file:/etc/dataone/mercury/applicationContext.xml";
    private ApplicationContext context;
    public MercuryDaemon() {
        super();
        System.out.println("ServiceDaemon: instance "+this.hashCode()+
                           " created");
    }

    @Override
    protected void finalize() {
        System.out.println("ServiceDaemon: instance "+this.hashCode()+
                           " garbage collected");
    }

    /**
     * init and destroy were added in jakarta-tomcat-daemon.
     */
    @Override
    public void init(DaemonContext context)
    throws Exception {
        /* Set the err */
        System.out.println("ServiceDaemon: instance "+this.hashCode()+
                           " init");

        System.out.println("ServiceDaemon: init done ");

    }

    @Override
    public void start() {
        /* Dump a message */
        System.out.println("ServiceDaemon: starting");

        context =new FileSystemXmlApplicationContext(appContextLocation);
//                 new ClassPathXmlApplicationContext(new String[]{"/org/dataone/cn/batch/proto/scheduler/applicationContext.xml"});
        }
    
    @Override
    public void stop() {
        /* Dump a message */
        System.out.println("ServiceDaemon: stopping");

//        SchedulerFactoryBean schedulerFactory = (SchedulerFactoryBean) context.getBean("scheduler", SchedulerFactoryBean.class);
//        Scheduler scheduler = schedulerFactory.getScheduler();
        Scheduler scheduler = (Scheduler) context.getBean("scheduler", org.quartz.impl.StdScheduler.class);
        try {
            scheduler.shutdown(true);
        } catch (SchedulerException ex) {
            System.err.println(ex.getMessage());
        }
 //       schedulerFactory.stop();
        System.out.println("ServiceDaemon: stopped");
    }

    @Override
    public void destroy() {
        System.out.println("ServiceDaemon: instance "+this.hashCode()+
                           " destroy");
    }

}
