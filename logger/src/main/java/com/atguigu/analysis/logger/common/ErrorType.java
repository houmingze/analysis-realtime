package com.atguigu.analysis.logger.common;

import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 * 系统级错误码范围：1 - 1000
 * 应用级错误码范围：1001 - 9999
 **/
public interface ErrorType {

    /**
     * Standard Errors
     */
    ErrorType SUCCESS = new Default(200, "响应成功");
    ErrorType UNAUTHORIZED = new Default(401, "请求未授权");
    ErrorType SYSTEM_ERROR = new Default(500, "系统内部错误");
    ErrorType PARAM_REQUIRED = new Default(600, "缺少必要参数[%s]");
    ErrorType PARAM_VALIDATE_FAILED = new Default(600, "参数错误[%s]");
    ErrorType ILLEGAL_PAGE_SIZE = new Default(601, "分页参数非法");


    int getCode();

    String getMsg();

    @Getter
    @ToString
    class Default implements ErrorType {

        private final int code;
        private final String msg;
        private Object context;

        public Default(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }

        public Default(int code, String msg, Object context) {
            this.code = code;
            this.msg = msg;
            this.context = context;
        }

        public Default setContext(Object context) {
            this.context = context;
            return this;
        }

    }

    default ErrorType params(Object... params) {
        return new Default(getCode(), String.format(getMsg(), params), null);
    }

    default ErrorType context(Object context) {
        return new Default(getCode(), getMsg(), context);
    }

}
