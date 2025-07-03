package com.kinnarastudio.obclient.exceptions;

/**
 * @author aristo
 *
 * Rest Client Exception
 */
public class OpenbravoClientException extends Exception {

    public OpenbravoClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpenbravoClientException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

    public OpenbravoClientException(String message) {
        super(message);
    }
}
