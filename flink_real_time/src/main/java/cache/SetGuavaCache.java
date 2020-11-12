package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author luckychacha
 * @date 2020-10-20 07:43
 */
public class SetGuavaCache {
    public static void main(String[] args) {
        Cache<String, String> cache =  CacheBuilder.newBuilder()
                .maximumSize(2)
                .expireAfterWrite(10, TimeUnit.SECONDS)
                .build();

        cache.put("foo", "bar");

//        while(true) {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.out.println(cache.getIfPresent("foo"));
//        }
    }
}
