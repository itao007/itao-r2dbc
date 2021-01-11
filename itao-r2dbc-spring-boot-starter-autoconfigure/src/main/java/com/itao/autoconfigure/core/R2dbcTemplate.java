package com.itao.autoconfigure.core;

import com.alibaba.fastjson.JSON;
import com.itao.bean.Count;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import static com.itao.bean.Count.ONE;
import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;

public class R2dbcTemplate {
    private R2dbcEntityTemplate template;


    public R2dbcTemplate(R2dbcEntityTemplate r2dbcEntityTemplate) {
        this.template = r2dbcEntityTemplate;
    }

    /**
     * 查询一条数据
     *
     * @param clazz
     * @return
     */
    public <T> Mono<T> selectOne(Class<T> clazz) {
        return template.select(clazz).one();

    }

    /**
     * 查询所有条数
     *
     * @param clazz
     * @return
     */
    public <T> Flux<T> select(Class<T> clazz) {
        return template.select(clazz).all();
    }

    /**
     * 新增
     *
     * @param t
     * @return
     */
    public <T> Mono<T> save(T t) {
        return template.insert(t);
    }

    /**
     * 修改
     *
     * @param t
     * @return
     */
    public <T> Mono<T> update(T t) {
        return template.update(t);
    }

    /**
     * 根据sql查询多条数据
     *
     * @param sql
     * @return
     */
    public Flux<Map<String, Object>> selectSql(String sql) {
        return template.getDatabaseClient()
                .execute(sql)
                .fetch()
                .all();
    }

    /**
     * 根据sql查询多条数据
     *
     * @param sql
     * @return
     */
    public <T> Flux<T> selectSql(String sql, Class<T> clazz) {
        return template.getDatabaseClient()
                .execute(sql)
                .fetch()
                .all()
                .map(t -> {
                    String json = JSON.toJSONString(t);
                    return JSON.parseObject(json, clazz);
                });
    }

    /**
     * 根据带命名参数的sql查询多条数据
     *
     * @param sql
     * @param params
     * @return
     */
    public Flux<Map<String, Object>> selectNamedSql(String sql, Map<String, Object> params) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.size() < 1) {
            return spec.fetch().all();
        }
        return bindNamedParams(spec, params).fetch().all();
    }

    /**
     * 根据带命名参数的sql查询多条数据
     *
     * @param sql
     * @param params
     * @return
     */
    public <T> Flux<T> selectNamedSql(String sql, Map<String, Object> params, Class<T> clazz) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.size() < 1) {
            return spec
                    .fetch()
                    .all()
                    .map(t -> {
                        String json = JSON.toJSONString(t);
                        return JSON.parseObject(json, clazz);
                    });
        }
        return bindNamedParams(spec, params)
                .fetch()
                .all()
                .map(t -> {
                    String json = JSON.toJSONString(t);
                    return JSON.parseObject(json, clazz);
                });
    }

    /**
     * 根据带位置参数的sql查询多条数据
     *
     * @param sql
     * @param params
     * @return
     */
    public Flux<Map<String, Object>> selectPositionSql(String sql, Object[] params) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.length < 1) {
            return spec.fetch().all();
        }
        return bindPositionParams(spec, params).fetch().all();
    }

    /**
     * 根据带位置参数的sql查询多条数据
     *
     * @param sql
     * @param params
     * @return
     */
    public <T> Flux<T> selectPositionSql(String sql, Object[] params, Class<T> clazz) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.length < 1) {
            return spec
                    .fetch()
                    .all()
                    .map(t -> {
                        String json = JSON.toJSONString(t);
                        return JSON.parseObject(json, clazz);
                    });
        }
        return bindPositionParams(spec, params)
                .fetch()
                .all()
                .map(t -> {
                    String json = JSON.toJSONString(t);
                    return JSON.parseObject(json, clazz);
                });
    }

    /**
     * 根据sql查询单条数据
     *
     * @param sql
     * @param count ONE 查询结果只能有一条数据，FIRST 去查询结果的第一条
     * @return
     */
    public Mono<Map<String, Object>> selectSql(String sql, Count count) {
        return count == ONE ?
                template.getDatabaseClient().execute(sql).fetch().one() :
                template.getDatabaseClient().execute(sql).fetch().first();
    }

    /**
     * 根据sql查询单条数据
     *
     * @param sql
     * @param count ONE 查询结果只能有一条数据，FIRST 去查询结果的第一条
     * @return
     */
    public <T>Mono<T> selectSql(String sql, Count count,Class<T> clazz) {
        return count == ONE ?
                template.getDatabaseClient()
                        .execute(sql)
                        .fetch()
                        .one()
                        .map(t -> {
                            String json = JSON.toJSONString(t);
                            return JSON.parseObject(json, clazz);
                        }) :
                template.getDatabaseClient()
                        .execute(sql)
                        .fetch()
                        .first()
                        .map(t -> {
                            String json = JSON.toJSONString(t);
                            return JSON.parseObject(json, clazz);
                        });
    }

    /**
     * 根据带命名参数的sql查询单条数据
     *
     * @param sql
     * @param params
     * @param count
     * @return
     */
    public Mono<Map<String, Object>> selectNamedSql(String sql, Map<String, Object> params, Count count) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.size() < 1) {
            if (count == ONE) {
                return spec.fetch().one();
            }
            return spec.fetch().first();
        }
        if (count == ONE) {
            return bindNamedParams(spec, params).fetch().one();
        }
        return bindNamedParams(spec, params).fetch().first();
    }

    /**
     * 根据带位置参数的sql查询单条数据
     *
     * @param sql
     * @param params
     * @param count
     * @return
     */
    public Mono<Map<String, Object>> selectPositionSql(String sql, Object[] params, Count count) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.length < 1) {
            if (count == ONE) {
                return spec.fetch().one();
            }
            return spec.fetch().first();
        }
        if (count == ONE) {
            return bindPositionParams(spec, params).fetch().one();
        }
        return bindPositionParams(spec, params).fetch().first();
    }


    /**
     * 执行sql语句（修改、删除、创建表）
     *
     * @param sql
     * @return
     */
    public Mono<Integer> sql(String sql) {
        return template.getDatabaseClient()
                .execute(sql)
                .fetch()
                .rowsUpdated();
    }

    /**
     * 执行带命名参数的sql语句（修改、删除）
     *
     * @param sql
     * @param params
     * @return
     */
    public Mono<Integer> namedSql(String sql, Map<String, Object> params) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.size() < 1) {
            return spec.fetch().rowsUpdated();
        }
        return bindNamedParams(spec, params).fetch().rowsUpdated();
    }

    /**
     * 执行带位置参数的sql语句（修改、删除）
     *
     * @param sql
     * @param params
     * @return
     */
    public Mono<Integer> positionSql(String sql, Object[] params) {
        DatabaseClient.GenericExecuteSpec spec = template.getDatabaseClient().execute(sql);
        if (params == null || params.length < 1) {
            return spec.fetch().rowsUpdated();
        }
        return bindPositionParams(spec, params).fetch().rowsUpdated();
    }

    /**
     * 删除数据
     *
     * @param t
     * @return
     */
    public <T> Mono<T> delete(T t) {
        return template.delete(t);
    }

    /**
     * 绑定命名参数
     *
     * @param spec
     * @param params
     * @return
     */
    private DatabaseClient.GenericExecuteSpec bindNamedParams(DatabaseClient.GenericExecuteSpec spec, Map<String, Object> params) {
        if (spec == null) {
            throw new RuntimeException("DatabaseClient.GenericExecuteSpec is null");
        }
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            spec.bind(entry.getKey(), entry.getValue());
        }
        return spec;
    }

    /**
     * 绑定位置参数
     *
     * @param spec
     * @param params
     * @return
     */
    private DatabaseClient.GenericExecuteSpec bindPositionParams(DatabaseClient.GenericExecuteSpec spec, Object[] params) {
        if (spec == null) {
            throw new RuntimeException("DatabaseClient.GenericExecuteSpec is null");
        }
        for (int i = 0; i < params.length; i++) {
            spec.bind(i, params[i]);
        }
        return spec;
    }

}
