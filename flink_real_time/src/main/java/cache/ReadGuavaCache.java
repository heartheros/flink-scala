package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author luckychacha
 * @date 2020-10-20 07:44
 */
public class ReadGuavaCache {
    public static void main(String[] args) {
        Cache<String, String> cache =  CacheBuilder.newBuilder()
                .maximumSize(2)
                .expireAfterAccess(3, TimeUnit.MINUTES)
                .build();
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(cache.getIfPresent("foo"));
        }
    }
}
