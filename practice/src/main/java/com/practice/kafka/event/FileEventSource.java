package com.practice.kafka.event;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileEventSource는 지정된 파일을 모니터링하여
 * 파일에 새로운 라인이 추가될 때마다 이를 읽고 이벤트 핸들러를 통해 처리합니다.
 */
public class FileEventSource implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class.getName());
    private EventHandler eventHandler;

    boolean keepRunning = true;
    private File file;
    private int updateInterval;
    private long filePointer = 0;

    /**
     * FileEventSource 생성자
     *
     * @param updateInterval 파일 업데이트를 확인할 간격 (밀리초 단위)
     * @param file           모니터링할 파일
     * @param eventHandler   메시지를 처리할 이벤트 핸들러
     */
    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    /**
     * 스레드 실행 메서드
     * 주기적으로 파일을 확인하고, 새로운 라인이 추가되면 이를 읽어 이벤트 핸들러로 전달합니다.
     */
    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);

                // 파일의 현재 크기 확인
                long len = this.file.length();

                if (len < this.filePointer) {
                    // 파일이 리셋되었을 경우 (크기가 줄어든 경우)
                    logger.info("file was reset ad filePointer is longer than file length");
                    filePointer = len;
                } else if (len > this.filePointer) {
                    // 파일에 새로운 내용이 추가된 경우
                    readAppendAndSend();
                } else {
                    // 파일에 변화가 없는 경우
                    continue;
                }
            }
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        } catch (ExecutionException e) {
            logger.info(e.getMessage());
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * 파일의 새로운 내용을 읽고 이벤트 핸들러로 전달하는 메서드
     *
     * @throws InterruptedException 비동기 작업 중 인터럽트 발생 시
     * @throws ExecutionException   비동기 작업 중 예외 발생 시
     * @throws IOException          파일 읽기 중 예외 발생 시
     */
    private void readAppendAndSend() throws InterruptedException, ExecutionException, IOException {

        RandomAccessFile raf = new RandomAccessFile(this.file, "r");
        raf.seek(this.filePointer);
        String line = null;

        // 새로운 라인을 읽어 처리
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }

        // 현재 파일 포인터를 업데이트하여 다음 읽기 위치로 설정
        this.filePointer = raf.getFilePointer();
    }

    /**
     * 읽은 라인을 파싱하여 메시지 이벤트로 변환하고 이벤트 핸들러에 전달하는 메서드
     *
     * @param line 파일에서 읽은 한 줄의 텍스트
     * @throws ExecutionException   비동기 작업 중 예외 발생 시
     * @throws InterruptedException 비동기 작업 중 인터럽트 발생 시
     */
    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String delimiter = ",";

        String[] tokens = line.split(delimiter);
        String key = tokens[0];
        StringBuffer value = new StringBuffer();

        for (int i = 1; i < tokens.length; i++) {
            if (i != tokens.length - 1) {
                value.append(tokens[i] + delimiter);
            } else {
                value.append(tokens[i]);
            }
        }

        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        this.eventHandler.onMessage(messageEvent);
    }
}
