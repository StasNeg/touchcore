package com.crusbee.model.dto;

import com.crusbee.model.UpdateValue;

import java.io.Serializable;

public class DrillExtractionDto implements Serializable {
    private final String externalId;
    private final String name;
    private final String description;
    private UpdateValue updateValue;

    public DrillExtractionDto(String externalId, String name, String description, UpdateValue updateValue) {
        this.externalId = externalId;
        this.name = name;
        this.description = description;
        this.updateValue = updateValue;
    }

    public DrillExtractionDto(String externalId, String name, String description) {
        this(externalId, name, description, UpdateValue.getByIntValue(0));
    }

    public String getExternalId() {
        return externalId;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public UpdateValue getUpdateValue() {
        return updateValue;
    }

    public void setUpdateValue(UpdateValue updateValue) {
        this.updateValue = updateValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DrillExtractionDto)) return false;

        DrillExtractionDto that = (DrillExtractionDto) o;

        return getExternalId().equals(that.getExternalId());
    }

    @Override
    public int hashCode() {
        return getExternalId().hashCode();
    }

    @Override
    public String toString() {
        return "DrillExtractionDto{" +
                "externalId='" + externalId + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", updateValue=" + updateValue +
                '}';
    }
}
