.PHONY: test

test:
	_JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED" ./gradlew test
