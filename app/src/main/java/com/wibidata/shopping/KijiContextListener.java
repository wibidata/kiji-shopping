/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wibidata.shopping;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * A servlet context listener that creates a Kiji connection for use during
 * the web application's lifetime.
 */
public class KijiContextListener implements ServletContextListener {
  private static final Logger LOG = LoggerFactory.getLogger(KijiContextListener.class);

  /** The attribute in the servlet context that stores the Wibi connection. */
  public static final String KIJI_ATTRIBUTE = "kiji";

  @Override
  public void contextInitialized(ServletContextEvent event) {
    final ServletContext servletContext = event.getServletContext();

    try {
      LOG.info("Opening a kiji connection...");
      final Kiji kiji = Kiji.Factory.open(
          KijiURI.newBuilder().withInstanceName("shopping").build(),
          HBaseConfiguration.create());
      LOG.info("Opened.");
      servletContext.setAttribute(KIJI_ATTRIBUTE, kiji);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
    final ServletContext servletContext = event.getServletContext();

    final Kiji kiji = (Kiji) servletContext.getAttribute(KIJI_ATTRIBUTE);
    servletContext.removeAttribute(KIJI_ATTRIBUTE);

    LOG.info("Closing the wibi connection...");
    ResourceUtils.releaseOrLog(kiji);
    LOG.info("Closed.");
  }
}
