package com.rabbitmq.client.impl;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.SaslConfig;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

public class ConnectionParams {

    private ExecutorService consumerWorkServiceExecutor;
    private ConnectionFactory.ConnectionFactoryConfig connectionFactoryConfig;

    public ConnectionParams() {
    }

    public ExecutorService getConsumerWorkServiceExecutor() {
        return consumerWorkServiceExecutor;
    }
    
    public void setConsumerWorkServiceExecutor(ExecutorService consumerWorkServiceExecutor) {
        this.consumerWorkServiceExecutor = consumerWorkServiceExecutor;
    }

    public void setConnectionFactoryConfig(ConnectionFactory.ConnectionFactoryConfig connectionFactoryConfig) {
        this.connectionFactoryConfig = connectionFactoryConfig;
    }
    
    public String getUsername() {
        return connectionFactoryConfig.username;
    }

    public String getPassword() {
        return connectionFactoryConfig.password;
    }

    public String getVirtualHost() {
        return connectionFactoryConfig.virtualHost;
    }

    public Map<String, Object> getClientProperties() {
        return connectionFactoryConfig._clientProperties;
    }

    public int getRequestedFrameMax() {
        return connectionFactoryConfig.requestedFrameMax;
    }

    public int getRequestedChannelMax() {
        return connectionFactoryConfig.requestedChannelMax;
    }

    public int getRequestedHeartbeat() {
        return connectionFactoryConfig.requestedHeartbeat;
    }

    public int getHandshakeTimeout() {
        return connectionFactoryConfig.handshakeTimeout;
    }

    public int getShutdownTimeout() {
        return connectionFactoryConfig.shutdownTimeout;
    }

    public SaslConfig getSaslConfig() {
        return connectionFactoryConfig.saslConfig;
    }

    public ExceptionHandler getExceptionHandler() {
        return connectionFactoryConfig.exceptionHandler;
    }

    public long getNetworkRecoveryInterval() {
        return connectionFactoryConfig.networkRecoveryInterval;
    }

    public boolean isTopologyRecoveryEnabled() {
        return connectionFactoryConfig.topologyRecovery;
    }

    public ThreadFactory getThreadFactory() {
        return connectionFactoryConfig.threadFactory;
    }

    public ExecutorService getShutdownExecutor() {
        return connectionFactoryConfig.shutdownExecutor;
    }

    public FrameHandlerFactory getFrameHandlerFactory() {
        return connectionFactoryConfig.frameHandlerFactory;
    }

}
