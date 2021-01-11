package com.itao.autoconfigure;

import com.itao.autoconfigure.core.R2dbcTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;

@ConditionalOnBean(R2dbcEntityTemplate.class)
//@EnableConfigurationProperties(ItaoR2dbcProperties.class)
public class ItaoR2dbcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(R2dbcTemplate.class)
    public R2dbcTemplate r2dbcTemplate(R2dbcEntityTemplate r2dbcEntityTemplate){
        return new R2dbcTemplate(r2dbcEntityTemplate);
    }
}
