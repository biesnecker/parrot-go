BINARY_NAME=parrot

.DEFAULT_GOAL := build

SRCS= \
	parrot.go \
	reader.go \
	seen.go \
	writer.go

run:
	go run parrot.go


build:
	go build -o $(BINARY_NAME) $(SRCS)

.PHONY: clean
clean:
	rm -f $(BINARY_NAME)
