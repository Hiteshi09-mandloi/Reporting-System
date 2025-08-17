package com.adtech; // Ensure this package matches your actual main application package

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@ComponentScan(basePackages = "com.adtech")
@EnableJpaRepositories(basePackages = "com.adtech.reportingsystem.repository")
public class AdtechReportingSystemApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdtechReportingSystemApplication.class, args);
    }

}
