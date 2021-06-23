package com.wugui.datax.admin.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;


/**
 * Spring的ApplicationContext的持有者,可以用静态方法的方式获取spring容器中的bean
 * @author chen.ruihong
 * @date 2020/06/18
 */
@Component
@Lazy(false)
public class SpringContextHolder implements ApplicationContextAware {
 
	/** 上下文对象*/
	private static ApplicationContext applicationContext;

	/** 注入上下文对象*/
	@Override
	public void setApplicationContext(ApplicationContext applicationContext)  {
		SpringContextHolder.applicationContext = applicationContext;
	}

	/**
	 * 获取上下文对象
	 * @return
	 */
	public static ApplicationContext getApplicationContext() {
		assertApplicationContext();
		return applicationContext;
	}

	
	/**
	 * 获取bean
	 * @param beanName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getBean(String beanName) {
		assertApplicationContext();
		return (T) applicationContext.getBean(beanName);
	}

	
	/**
	 * 获取bean
	 * @param requiredType
	 * @return
	 */
	public static <T> T getBean(Class<T> requiredType) {
		assertApplicationContext();
		return applicationContext.getBean(requiredType);
	}

	
	/**
	 * 上下文对象判空
	 */
	private static void assertApplicationContext() {
		if (SpringContextHolder.applicationContext == null) {
			throw new RuntimeException("applicaitonContext属性为null,请检查是否注入了SpringContextHolder!");
		}
	}
}
