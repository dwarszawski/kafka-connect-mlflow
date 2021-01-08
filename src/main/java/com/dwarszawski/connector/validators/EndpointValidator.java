package com.dwarszawski.connector.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.commons.validator.routines.UrlValidator;

public class EndpointValidator implements ConfigDef.Validator {
    private static final String[] SCHEMES = {"https", "http"};

    public void ensureValid(String name, Object value) {
        UrlValidator urlValidator = new UrlValidator(SCHEMES);
        if (!urlValidator.isValid((String) value)) {
            throw new ConfigException(name, value, "Not valid url");
        }
    }
}
