*** Settings ***
Documentation     Integrationstest: Kafka Produce → Consume → Verify gegen echten Broker.
...               Broker wird per OKW-Docker-Keywords provisioniert (KRaft-Mode).
Library           okw_env_docker.library.OkwEnvDockerLibrary
...               components_dir=${CURDIR}${/}components
Library           okw_kafka.library.OkwKafkaLibrary
...               config_dir=${CURDIR}${/}configs    backend=confluent
Suite Setup       Starte Kafka Broker
Suite Teardown    Run Keywords    Beende Kafka    ENV_Stop

*** Keywords ***
Starte Kafka Broker
    ENV_Start          KafkaTestBroker
    ENV_BuildAndRun
    ENV_WaitForReady   KafkaTestBroker
    KafkaStart         kafka-local

Beende Kafka
    KafkaStop

*** Test Cases ***
Produce und Consume Roundtrip
    [Documentation]    Nachricht senden und empfangen — der einfachste Integrationstest.
    KafkaSetTopic       okw-test-roundtrip
    KafkaSetValue       $.action    create
    KafkaSetValue       $.item      Widget-42
    KafkaSetKey         order-001
    KafkaSetHeader      correlation-id    test-001
    KafkaProduce

    KafkaConsume        okw-test-roundtrip    TIMEOUT=30
    KafkaVerifyValue    $.action    create
    KafkaVerifyValue    $.item      Widget-42
    KafkaVerifyKey      order-001
    KafkaVerifyHeader   correlation-id    test-001

Produce und Consume mit Filter
    [Documentation]    Zwei Nachrichten senden, per Filter die richtige konsumieren.
    KafkaSetTopic       okw-test-filter
    KafkaSetValue       $.item      A
    KafkaProduce

    KafkaSetValue       $.item      B
    KafkaProduce

    KafkaConsume        okw-test-filter    FILTER=$.item=B    TIMEOUT=30
    KafkaVerifyValue    $.item      B

Verify mit Wildcard und Regex
    [Documentation]    Match-Modes WCM und REGX gegen echte Kafka-Nachrichten.
    KafkaSetTopic       okw-test-matchmodes
    KafkaSetValue       $.message    Order created successfully
    KafkaSetValue       $.code       12345
    KafkaProduce

    KafkaConsume             okw-test-matchmodes    TIMEOUT=30
    KafkaVerifyValueWCM      $.message    *successfully
    KafkaVerifyValueREGX     $.code       \\d{5}

Memorize und Expand
    [Documentation]    Wert merken und in nächster Nachricht verwenden.
    KafkaSetTopic       okw-test-memorize
    KafkaSetValue       $.token     secret-abc-123
    KafkaProduce

    KafkaConsume             okw-test-memorize    TIMEOUT=30
    KafkaMemorizeValue       $.token    AUTH_TOKEN

    KafkaSetTopic       okw-test-memorize-verify
    KafkaSetValue       $.auth      $MEM{AUTH_TOKEN}
    KafkaProduce

    KafkaConsume        okw-test-memorize-verify    TIMEOUT=30
    KafkaVerifyValue    $.auth      secret-abc-123

Topic Existenz pruefen
    [Documentation]    Admin-Keyword: prüft ob ein Topic existiert.
    KafkaSetTopic       okw-test-admin
    KafkaSetValue       $.x         1
    KafkaProduce

    KafkaVerifyTopicExists    okw-test-admin    YES
    KafkaVerifyTopicExists    nonexistent-topic-xyz    NO

IGNORE Token ueberspringt Keyword
    [Documentation]    $IGNORE macht jedes Keyword zum No-Op.
    KafkaSetTopic       $IGNORE
    KafkaSetValue       $.x    $IGNORE
    KafkaVerifyValue    $.x    $IGNORE
