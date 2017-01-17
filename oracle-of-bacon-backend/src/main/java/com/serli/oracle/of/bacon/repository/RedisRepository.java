package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {

    Jedis jedis;

    public RedisRepository () {
        jedis = new Jedis("localhost", 6379);
    }

    public void addSearch(String search) {
        jedis.lpush("searches", search);
        if(jedis.llen("searches") > 10) jedis.rpop("searches");
    }

    public List<String> getLastTenSearches() {
        return jedis.lrange("searches", 0, 9);
    }
}
