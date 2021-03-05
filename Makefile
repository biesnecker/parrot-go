BINARY_NAME=parrot

.DEFAULT_GOAL := build

SRCS= \
	parrot.go


run:
	go run $(SRCS)


build:
	go build -o $(BINARY_NAME) $(SRCS)

.PHONY: clean
clean:
	rm -f $(BINARY_NAME)
