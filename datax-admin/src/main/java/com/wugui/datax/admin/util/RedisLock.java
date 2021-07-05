package com.wugui.datax.admin.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;

/**
 * redis锁
 * @author Doctor
 * @date 2019/11/12
 */
@Component
@Slf4j
public class RedisLock {
	
	/** 缓存服务 */
	@Resource
	private RedisTemplate<String, Object> redisTemplate;
	
	/**
     * 加锁
     * @param lock
     * @param timeOut
     * @return
     */
    public boolean lock(String lock, long timeOut){
    	while (true) {
			if(tryLock(lock, timeOut)){
        		return true;
        	}
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				log.warn("InterruptedException:{}", e.getMessage());
				Thread.currentThread().interrupt();
			}
		}
    }
    
    /**
     * 尝试加锁
     * @param lock
     * @param timeOut
     * @return
     */
    public boolean tryLock(String lock, long timeOut){
        return redisTemplate.execute((RedisConnection connection) -> {
					RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
					RedisSerializer keySerializer = redisTemplate.getKeySerializer();
					Object obj = connection.execute("set", keySerializer.serialize(lock),
							valueSerializer.serialize(1),
							"NX".getBytes(StandardCharsets.UTF_8),
							"EX".getBytes(StandardCharsets.UTF_8),
							String.valueOf(timeOut).getBytes(StandardCharsets.UTF_8));
					return obj != null;
				}

		);
    }
    
    /**
     * 解锁
     * @param lock
     */
    public void unlock(String lock) {
		redisTemplate.delete(lock);
	}

}
