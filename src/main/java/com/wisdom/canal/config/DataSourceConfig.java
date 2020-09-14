package com.wisdom.canal.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @date: 2020/9/4 15:22
 * @author: LJP
 */
@Configuration
public class DataSourceConfig {

    @Bean(name = "towerDataSource")
    @Qualifier("towerDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.tower")
    public DataSource towerDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "elevatorDataSource")
    @Qualifier("elevatorDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.elevator")
    public DataSource elevatorDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "dustDataSource")
    @Qualifier("dustDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.dust")
    public DataSource dustDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "cloudDataSource")
    @Qualifier("cloudDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.cloud")
    public DataSource cloudDataSource() {
        return DataSourceBuilder.create().build();
    }
}