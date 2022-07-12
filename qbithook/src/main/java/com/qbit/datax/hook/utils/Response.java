package com.qbit.datax.hook.utils;

import java.util.Objects;

public class Response {
    private String result;

    private int code;

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public boolean isSuccessful() {
        return code >= 200 && code < 300;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Response response = (Response) o;
        return code == response.code && Objects.equals(result, response.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result, code);
    }

    @Override
    public String toString() {
        return "Response{" +
                "result='" + result + '\'' +
                ", code=" + code +
                '}';
    }
}
