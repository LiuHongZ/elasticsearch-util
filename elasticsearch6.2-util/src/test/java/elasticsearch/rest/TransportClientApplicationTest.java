package elasticsearch.rest;

import lombok.extern.slf4j.Slf4j;

import static pers.wilson.elasticsearch.transport.TransportClientUtil.*;

@Slf4j
public class TransportClientApplicationTest {

    public static void main(String[] args) {

        log.info("Application start ...");
        String instruction = args.length > 0 ? args[0] : "";
        log.info("输入指令：{}", instruction);

        String INDEX_NAME = "security_log_2020.02.06";
        String INDEX_TYPE = "log";
        switch (instruction) {
            case "0":
                insert(INDEX_NAME, INDEX_TYPE);
                break;
            case "1":
                delete(INDEX_NAME, INDEX_TYPE, args[1]);
                break;
            case "2":
                update(INDEX_NAME, INDEX_TYPE, args[1]);
                break;
            case "3":
                query(INDEX_NAME, INDEX_TYPE);
                break;
            default:
                break;
        }
    }
}
