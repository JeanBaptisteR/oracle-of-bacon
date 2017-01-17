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
    }

    public List<String> getLastTenSearches() {
        jedis.ltrim("searches", 0, 9);
        return jedis.lrange("searches", 0, 9);
    }
}
