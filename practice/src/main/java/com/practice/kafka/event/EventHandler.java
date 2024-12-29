package com.practice.kafka.event;

import java.util.concurrent.ExecutionException;

public interface EventHandler {

    void onMessage(MessageEvent massageEvent) throws InterruptedException, ExecutionException;

}
