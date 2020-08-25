package com.atguigu.analysis.logger.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Response<T> implements Serializable {
    private int code;
    private String msg;
    private T data;

    public Response() {
    }

    public Response(int code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public static <T> Response<T> error() {
        return error((String) null);
    }

    public static <T> Response<T> error(String errorMsg) {
        return new Response<>(ErrorType.SYSTEM_ERROR.getCode(),
                errorMsg == null ? ErrorType.SYSTEM_ERROR.getMsg() : errorMsg,
                (T) null);
    }

    public static <T> Response<T> error(ErrorType errorType) {
        if (errorType == null) throw new IllegalArgumentException("errorType");
        int errorCode;
        if (errorType.getCode() == ErrorType.SUCCESS.getCode()) {
            throw new IllegalArgumentException("errorCode should != 200 when call error()!");
        } else {
            errorCode = errorType.getCode();
        }
        String errorMsg = errorType.getMsg();
        Object context = null;
        if (errorType instanceof ErrorType.Default) {
            context = ((ErrorType.Default) errorType).getContext();
        }
        return new Response<T>(errorCode, errorMsg, (T) context);
    }

    public static <T> Response<T> success(T data) {
        return new Response<>(ErrorType.SUCCESS.getCode(), ErrorType.SUCCESS.getMsg(), data);
    }

    public static <T> Response<T> success() {
        return new Response<>(ErrorType.SUCCESS.getCode(), ErrorType.SUCCESS.getMsg(), null);
    }

    @JsonIgnore
    public boolean isSuccess() {
        return this.code == ErrorType.SUCCESS.getCode();
    }

}
