package com.crusbee.model;

public enum UpdateValue {
    INIT_VALUE(0),NOT_UPDATE_VALUE(1), UPDATE_VALUE(2);


    private final int forUpdateValue;

    UpdateValue(int forUpdateValue) {
        this.forUpdateValue = forUpdateValue;
    }

    public int getForUpdateValue() {
        return forUpdateValue;
    }
    public static UpdateValue getByIntValue(int intValue){
        return UpdateValue.values()[intValue];
    }
}
