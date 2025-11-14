package com.hmdp.utils;

import io.lettuce.core.ScriptOutputType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Resource
public class RedisIdWork {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final long BEGIN_TIMESTAMMP = 1640995200L;
    //序列号的位数
    private static final int COUNT_BITS = 32;
    public long nextId(String keyPrefix){
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        long second = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = second - BEGIN_TIMESTAMMP;

        //2.生成序列号
        //2.1获取当前日期,精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2自增长
        long count = stringRedisTemplate.opsForValue().increment("icr"+keyPrefix+":"+date);
        //3.拼接
        return timestamp << COUNT_BITS | count;
    }

}
