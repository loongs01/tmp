package model;

public class WordInfo {
    private String textMd5;
    private Integer sentNo;
    private String sentText;
    private String sentMd5;
    private String sentSplitParam;
    private Integer wordNo;
    private String wordText;
    private String partOfSpeech;
    private String loadTime;

    // getters and setters

    public String getTextMd5() {
        return textMd5;
    }

    public void setTextMd5(String textMd5) {
        this.textMd5 = textMd5;
    }

    public Integer getSentNo() {
        return sentNo;
    }

    public void setSentNo(Integer sentNo) {
        this.sentNo = sentNo;
    }

    public String getSentText() {
        return sentText;
    }

    public void setSentText(String sentText) {
        this.sentText = sentText;
    }

    public String getSentMd5() {
        return sentMd5;
    }

    public void setSentMd5(String sentMd5) {
        this.sentMd5 = sentMd5;
    }

    public String getSentSplitParam() {
        return sentSplitParam;
    }

    public void setSentSplitParam(String sentSplitParam) {
        this.sentSplitParam = sentSplitParam;
    }

    public Integer getWordNo() {
        return wordNo;
    }

    public void setWordNo(Integer wordNo) {
        this.wordNo = wordNo;
    }

    public String getWordText() {
        return wordText;
    }

    public void setWordText(String wordText) {
        this.wordText = wordText;
    }

    public String getPartOfSpeech() {
        return partOfSpeech;
    }

    public void setPartOfSpeech(String partOfSpeech) {
        this.partOfSpeech = partOfSpeech;
    }

    public String getLoadTime() {
        return loadTime;
    }

    public void setLoadTime(String loadTime) {
        this.loadTime = loadTime;
    }
}
