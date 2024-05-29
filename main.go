package main

import (
	"fmt"
	"io"
	"os"
)

const inputFile = "./measurements.txt"
const numWorkers = 4

func main() {
	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	segments := getFileSegments(f)

	c := make(chan map[string]Stats, numWorkers)

	for worker := 0; worker < numWorkers; worker++ {
		go ProcessChunk(segments[worker], segments[worker+1], c)
	}
	for worker := 0; worker < numWorkers; worker++ {
		result := <-c
		fmt.Println(result)
	}
}

func getFileSegments(f *os.File) []int64 {
	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := fi.Size()
	fmt.Printf("Input file has size %v\n", fileSize)

	chunkSize := fileSize / numWorkers

	segments := []int64{0}

	buf := make([]byte, 500)
	for i := 0; i < numWorkers; i++ {
		nextStop := segments[i] + chunkSize
		if i == numWorkers-1 {
			nextStop = fileSize
		} else {

			_, err = f.Seek(nextStop, 0)
			if err != nil {
				panic(err)
			}

			_, err = f.Read(buf)
			if err != nil {
				if err != io.EOF {
					panic(err)
				}
			} else {
				for i, v := range buf {
					if v == '\n' {
						nextStop += int64(i) + 1
					}
				}
			}
		}
		segments = append(segments, nextStop)
	}
	fmt.Printf("Segments to be processed by %v workers: %v\n", numWorkers, segments)
	return segments
}

type Stats struct {
	max   int64
	min   int64
	sum   int64
	count int64
}

func ProcessChunk(start, end int64, c chan map[string]Stats) {
	result := make(map[string]Stats)

	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	_, err = f.Seek(start, 0)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 10_000)
	currentLocation := start
	carryOverBytes := []byte{};

	for currentLocation < end {
		bytesRead, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
		}
		byteData := buf[:bytesRead]
		currentLocation += int64(bytesRead)
		if currentLocation > end {
			byteData = byteData[:int64(bytesRead)-(currentLocation-end)]
		}

		lineStart := 0;
		for i, b := range byteData {
			if b == '\n' {
				_ = ProcessLine(append(carryOverBytes, byteData[lineStart:i]...));
				lineStart = i + 1;
				carryOverBytes = []byte{};
			}
		}
		fmt.Println(string(byteData));
		carryOverBytes = byteData[lineStart:];
	}
	c <- result
}

func ProcessLine(b []byte) string {
	s := string(b);
	fmt.Println(s);
	return s;
}
