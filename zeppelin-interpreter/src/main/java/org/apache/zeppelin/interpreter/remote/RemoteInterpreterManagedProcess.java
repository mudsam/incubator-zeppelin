/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.interpreter.remote;

import org.apache.commons.exec.*;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.zeppelin.helium.ApplicationEventListener;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStream;
import java.util.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;

/**
 * This class manages start / stop of remote interpreter process
 */
public class RemoteInterpreterManagedProcess extends RemoteInterpreterProcess
    implements ExecuteResultHandler {
  private static final Logger logger = LoggerFactory.getLogger(
      RemoteInterpreterManagedProcess.class);
  private final String interpreterRunner;

  private DefaultExecutor executor;
  private ExecuteWatchdog watchdog;
  boolean running = false;
  private int port = -1;
  private String host = "localhost";
  private final String interpreterDir;
  private final String localRepoDir;

  private Map<String, String> env;
  private Properties property;

  public RemoteInterpreterManagedProcess(
      Properties property,
      String intpRunner,
      String intpDir,
      String localRepoDir,
      Map<String, String> env,
      int connectTimeout,
      RemoteInterpreterProcessListener listener,
      ApplicationEventListener appListener) {
    super(new RemoteInterpreterEventPoller(listener, appListener),
        connectTimeout);
    this.interpreterRunner = intpRunner;
    this.env = env;
    this.property = property;
    this.interpreterDir = intpDir;
    this.localRepoDir = localRepoDir;

  }

  RemoteInterpreterManagedProcess(Properties property,
                                  String intpRunner,
                                  String intpDir,
                                  String localRepoDir,
                                  Map<String, String> env,
                                  RemoteInterpreterEventPoller remoteInterpreterEventPoller,
                                  int connectTimeout) {
    super(remoteInterpreterEventPoller,
        connectTimeout);
    this.interpreterRunner = intpRunner;
    this.env = env;
    this.property = property;
    this.interpreterDir = intpDir;
    this.localRepoDir = localRepoDir;
  }

  @Override
  public String getHost() {
    return this.host;
  }

  @Override
  public int getPort() {
    return this.port;
  }

  @Override
  public void start() {
    // Create interpreter registration socket
    ServerSocket serverSocket = null;
    String localHost = "";
    int localPort = -1;
    try {
      serverSocket = new ServerSocket(0);
      localPort = serverSocket.getLocalPort();
      localHost = InetAddress.getLocalHost().getHostName();
      logger.info("Created interpreter registration listener at {}:{}",
        localHost,
        localPort);
    } catch (IOException e1) {
      throw new InterpreterException(e1);
    }

    CommandLine cmdLine = CommandLine.parse(interpreterRunner);
    String master = property.getProperty("master");
    logger.info("Spark Master: {}", master);
    if (master != null &&
        (master.equals("yarn-cluster") || master.equals("cluster"))) {
      cmdLine.addArgument("-m", false);
      cmdLine.addArgument(master, false);
    }
    cmdLine.addArgument("-d", false);
    cmdLine.addArgument(interpreterDir, false);
    cmdLine.addArgument("-h", false);
    cmdLine.addArgument(localHost);
    cmdLine.addArgument("-p", false);
    cmdLine.addArgument(Integer.toString(localPort), false);
    cmdLine.addArgument("-l", false);
    cmdLine.addArgument(localRepoDir, false);

    executor = new DefaultExecutor();
    executor.setStreamHandler(new PumpStreamHandler(new ProcessLogOutputStream(logger)));
    watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);

    try {
      Map procEnv = EnvironmentUtils.getProcEnvironment();
      procEnv.putAll(env);

      logger.info("Run interpreter process {}", cmdLine);
      executor.execute(cmdLine, procEnv, this);
      running = true;
    } catch (IOException e) {
      running = false;
      throw new InterpreterException(e);
    }


    // Capture callback from remote interpreter
    // TODO(JohanMuedsam): How to handle restarted remote interpreters?
    try {
      logger.info("Waiting for interpreter connection...");
      Socket socket = serverSocket.accept();
      serverSocket.close();
      BufferedReader input =
        new BufferedReader(new InputStreamReader(socket.getInputStream()));
      this.host = input.readLine();
      this.port = Integer.parseInt(input.readLine());
      socket.close();
      logger.info("Remote interpreter available at {}:{}", host, port);
    } catch (Exception e3) {
      throw new InterpreterException(e3);
    }

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < getConnectTimeout()) {
      if (RemoteInterpreterUtils.checkIfRemoteEndpointAccessible(this.host, this.port)) {
        break;
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          logger.error("Exception in RemoteInterpreterProcess while synchronized reference " +
              "Thread.sleep", e);
        }
      }
    }
  }

  public void stop() {
    if (isRunning()) {
      logger.info("kill interpreter process");
      watchdog.destroyProcess();
    }

    executor = null;
    watchdog = null;
    running = false;
    logger.info("Remote process terminated");
  }

  @Override
  public void onProcessComplete(int exitValue) {
    logger.info("Interpreter process exited {}", exitValue);
    running = false;

  }

  @Override
  public void onProcessFailed(ExecuteException e) {
    logger.info("Interpreter process failed {}", e);
    running = false;
  }

  public boolean isRunning() {
    return running;
  }

  private static class ProcessLogOutputStream extends LogOutputStream {

    private Logger logger;

    public ProcessLogOutputStream(Logger logger) {
      this.logger = logger;
    }

    @Override
    protected void processLine(String s, int i) {
      this.logger.debug(s);
    }
  }
}
