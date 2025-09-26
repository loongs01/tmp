package model;

import java.util.Objects;

public class MappingResult {
    private String icd11Name;
    private Double distanceSimilarity;
    private Double containSimilarity;
    private Double infoSimilarity;
    private String mappingWord;

    // 构造函数
    public MappingResult() {}

    public MappingResult(String icd11Name, Double distanceSimilarity, Double containSimilarity, 
                        Double infoSimilarity, String mappingWord) {
        this.icd11Name = icd11Name;
        this.distanceSimilarity = distanceSimilarity;
        this.containSimilarity = containSimilarity;
        this.infoSimilarity = infoSimilarity;
        this.mappingWord = mappingWord;
    }

    // Getters and Setters
    public String getIcd11Name() {
        return icd11Name;
    }

    public void setIcd11Name(String icd11Name) {
        this.icd11Name = icd11Name;
    }

    public Double getDistanceSimilarity() {
        return distanceSimilarity;
    }

    public void setDistanceSimilarity(Double distanceSimilarity) {
        this.distanceSimilarity = distanceSimilarity;
    }

    public Double getContainSimilarity() {
        return containSimilarity;
    }

    public void setContainSimilarity(Double containSimilarity) {
        this.containSimilarity = containSimilarity;
    }

    public Double getInfoSimilarity() {
        return infoSimilarity;
    }

    public void setInfoSimilarity(Double infoSimilarity) {
        this.infoSimilarity = infoSimilarity;
    }

    public String getMappingWord() {
        return mappingWord;
    }

    public void setMappingWord(String mappingWord) {
        this.mappingWord = mappingWord;
    }

    @Override
    public String toString() {
        return "MappingResult{" +
                "icd11Name='" + icd11Name + '\'' +
                ", distanceSimilarity=" + distanceSimilarity +
                ", containSimilarity=" + containSimilarity +
                ", infoSimilarity=" + infoSimilarity +
                ", mappingWord='" + mappingWord + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MappingResult that = (MappingResult) o;
        return Objects.equals(mappingWord, that.mappingWord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappingWord);
    }
}