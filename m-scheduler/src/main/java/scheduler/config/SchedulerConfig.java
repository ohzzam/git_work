package scheduler.config;

import org.quartz.Scheduler;
import org.quartz.ee.servlet.QuartzInitializerListener;
import org.quartz.spi.JobFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import scheduler.service.ApplicationContextHolder;

import java.io.IOException;
import java.util.Properties;

@Configuration
public class SchedulerConfig {
	
	@Autowired
	private ApplicationContext applicationContext;

    /*@Bean(name="SchedulerFactory")
    public SchedulerFactoryBean schedulerFactoryBean() throws IOException {
    	SchedulerFactoryBean factory = new SchedulerFactoryBean();
    	factory.setOverwriteExistingJobs(true);
        factory.setQuartzProperties(quartzProperties());
        return factory;
		//
		SchedulerJobFactory jobFactory = new SchedulerJobFactory();
		jobFactory.setApplicationContext(applicationContext);

		// set properties
		SchedulerFactoryBean factoryBean = new SchedulerFactoryBean();
		factoryBean.setOverwriteExistingJobs(true);
		factoryBean.setQuartzProperties(quartzProperties());
		factoryBean.setJobFactory(jobFactory);

		return factoryBean;//
    }*/
    
    @Bean
    public Properties quartzProperties() throws IOException {
        PropertiesFactoryBean propertiesFactoryBean = new PropertiesFactoryBean();
        propertiesFactoryBean.setLocation(new ClassPathResource("/quartz.properties"));
        propertiesFactoryBean.afterPropertiesSet();
        return propertiesFactoryBean.getObject();
    }

    @Bean
    public QuartzInitializerListener executorListener() {
        return new QuartzInitializerListener();
    }

    @Bean(name="Scheduler")
    public Scheduler scheduler() throws IOException {
        return schedulerFactoryBean().getScheduler();
    }
    
    /**
     * Create the job factory bean
     * @return Job factory bean
     */
    @Bean
    public JobFactory jobFactory() {
      ApplicationContextHolder jobFactory = new ApplicationContextHolder();
      jobFactory.setApplicationContext(applicationContext);
      return jobFactory;
    }

    /**
     * Create the Scheduler Factory bean
     * @return scheduler factory object
     * @throws IOException 
     */
    @Bean(name="SchedulerFactory")
    public SchedulerFactoryBean schedulerFactoryBean() throws IOException {
      SchedulerFactoryBean factory = new SchedulerFactoryBean();
      factory.setAutoStartup(true);
      //factory.setSchedulerName("My Scheduler");
      factory.setOverwriteExistingJobs(true);
      factory.setQuartzProperties(quartzProperties());
      factory.setJobFactory(jobFactory());
      return factory;
    }

}
