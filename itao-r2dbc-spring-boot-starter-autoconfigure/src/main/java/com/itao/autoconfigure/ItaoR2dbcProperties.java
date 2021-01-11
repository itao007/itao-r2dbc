package com.itao.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

//@ConfigurationProperties(prefix = "itao.r2dbc")
public class ItaoR2dbcProperties {
    private boolean namedParameters;

    public boolean isNamedParameters() {
        return namedParameters;
    }

    public void setNamedParameters(boolean namedParameters) {
        this.namedParameters = namedParameters;
    }
}
