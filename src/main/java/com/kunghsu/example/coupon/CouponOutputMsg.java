package com.kunghsu.example.coupon;

/**
 * kafka输出对象
 * author:xuyaokun_kzx
 * date:2022/2/11
 * desc:
*/
public class CouponOutputMsg {

    private String COUPON_ID;
    private String MESSAGE_TYPE;
    private String COUPON_SEND_NUM;
    private String SERIAL_NO;
    private String ID_TYPE;
    private String ID_NUMBER;
    private String STORE_ID;
    private String STORE_RANGE;

    public String getCOUPON_ID() {
        return COUPON_ID;
    }

    public void setCOUPON_ID(String COUPON_ID) {
        this.COUPON_ID = COUPON_ID;
    }

    public String getMESSAGE_TYPE() {
        return MESSAGE_TYPE;
    }

    public void setMESSAGE_TYPE(String MESSAGE_TYPE) {
        this.MESSAGE_TYPE = MESSAGE_TYPE;
    }

    public String getCOUPON_SEND_NUM() {
        return COUPON_SEND_NUM;
    }

    public void setCOUPON_SEND_NUM(String COUPON_SEND_NUM) {
        this.COUPON_SEND_NUM = COUPON_SEND_NUM;
    }

    public String getSERIAL_NO() {
        return SERIAL_NO;
    }

    public void setSERIAL_NO(String SERIAL_NO) {
        this.SERIAL_NO = SERIAL_NO;
    }

    public String getID_TYPE() {
        return ID_TYPE;
    }

    public void setID_TYPE(String ID_TYPE) {
        this.ID_TYPE = ID_TYPE;
    }

    public String getID_NUMBER() {
        return ID_NUMBER;
    }

    public void setID_NUMBER(String ID_NUMBER) {
        this.ID_NUMBER = ID_NUMBER;
    }

    public String getSTORE_ID() {
        return STORE_ID;
    }

    public void setSTORE_ID(String STORE_ID) {
        this.STORE_ID = STORE_ID;
    }

    public String getSTORE_RANGE() {
        return STORE_RANGE;
    }

    public void setSTORE_RANGE(String STORE_RANGE) {
        this.STORE_RANGE = STORE_RANGE;
    }

    @Override
    public String toString() {
        return "CouponOutputMsg{" +
                "COUPON_ID='" + COUPON_ID + '\'' +
                ", MESSAGE_TYPE='" + MESSAGE_TYPE + '\'' +
                ", COUPON_SEND_NUM='" + COUPON_SEND_NUM + '\'' +
                ", SERIAL_NO='" + SERIAL_NO + '\'' +
                ", ID_TYPE='" + ID_TYPE + '\'' +
                ", ID_NUMBER='" + ID_NUMBER + '\'' +
                ", STORE_ID='" + STORE_ID + '\'' +
                ", STORE_RANGE='" + STORE_RANGE + '\'' +
                '}';
    }
}
