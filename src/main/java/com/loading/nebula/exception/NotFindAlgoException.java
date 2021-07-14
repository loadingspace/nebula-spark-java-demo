package com.loading.nebula.exception;

/**
 * desc:
 *
 * @author Lo_ading
 * @version 1.0.0
 * @date 2021/5/20
 */
public class NotFindAlgoException extends Exception {

  public NotFindAlgoException(String message) {
    super(message);
  }

  public NotFindAlgoException(String message, Exception e) {
    super(message, e);
  }

}
