/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.io.IOException;

/**
 * An embedded, in-memory cassandra storage service.
 * This kind of service is useful when running unit tests of
 * services using cassandra for example.
 *
 * See {@link org.apache.cassandra.service.EmbeddedCassandraServiceTest} for usage.
 * <p>
 * This is the implementation of https://issues.apache.org/jira/browse/CASSANDRA-740
 * <p>
 * How to use:
 * In the client code simply create a new EmbeddedCassandraService and start it.
 * Example:
 * <pre>

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

 * </pre>
 */
public class EmbeddedCassandraService
{

    CassandraDaemon cassandraDaemon;

    public void start() throws IOException
    {
        cassandraDaemon = CassandraDaemon.instance;
        /**
         * 1.加载cassandra.yarm配置文件，获取配置信息
         * 2.为启动Cassandra节点计算Partitioner，获取哈希值
         * 3.获取监听节点，获取rpc(远程节点，如不同数据中心的节点)，获取广播节点，获取广播的远程节点
         * 4.申请本地数据中节点间的告密者(Snitch)
         * 5.初始化节点的令牌，即（Token）
         * 6.对Cassandra集群中的种子节点（Seeds）进行配置
         * 7.获取EncryptionContext
         */
        cassandraDaemon.applyConfig();
        /**
         * 初始化守护进程自身
         * 1.初始化jmx（Java 管理拓展，用于监控程序的性能）
         * 2.如果加载mx4j-tools.jar，将开启jmx，监听地址127.0.0.1，默认端口是8081
         * 3.开启线程安全管理
         * 4.日志的系统输出，内容JVM（Java虚拟机）的版本和参数，(Heap size)堆内存的大小，主机名等等
         * 5.不同操作系统连接本地库
         * 6.开启检查
         * 7.初始化本地表local table，初始化cluster_name，key，data_center，native_protocol_version等等元数据
         * 8.从系统表加载TokenMetadata
         * 9.从磁盘中加载Schema
         * 10.初始化键空间
         * 11.加载行键内存
         * 12.jmx注册GC（垃圾回收机制）监控，观察性能和内存占用情况
         * 13.CommitLog删除磁盘碎片
         * 14.启动键空间，启动ActiveRepairService服务，预加载PreparedStatement
         * 15.加载metrics-reporter-config配置
         * 16.调度线程的一些工作
         */
        cassandraDaemon.init(null);
        /**
         * 2.开启本地节点传输,配置ServerBootstrap
         * 3.Gossiper（通信协议）本地节点的状态初始化
         */
        cassandraDaemon.start();
    }

    public void stop()
    {
        cassandraDaemon.stop();
    }
}
