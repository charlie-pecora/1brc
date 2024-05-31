package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

const inputFile = "./measurements.txt"
const numWorkers = 12

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
	finalResult := make(map[string]Stats)
	for worker := 0; worker < numWorkers; worker++ {
		result := <-c
		CombineMaps(&finalResult, result)
	}
	printMap(finalResult)
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
						break
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

func (s *Stats) AddLine(new int64) {
	s.count += 1
	s.sum += new
	s.max = max(s.max, new)
	s.min = min(s.min, new)
}

func (s *Stats) Combine(other Stats) {
	s.count += other.count
	s.sum += other.sum
	s.max = max(s.max, other.max)
	s.min = min(s.min, other.min)
}

func (s Stats) Mean() float64 {
	return math.Round(float64(s.sum)/float64(s.count)) / 10.0
}

func (s Stats) Print() string {
	return fmt.Sprintf("%v/%v/%v", s.min, s.Mean(), s.max)
}

func makeStats(new int64) Stats {
	return Stats{
		max:   new,
		min:   new,
		sum:   new,
		count: new,
	}

}

func AddToMap(sm *map[string]Stats, location string, temp int64) {
	locationStat, ok := (*sm)[location]
	if !ok {
		locationStat = makeStats(temp)
	} else {
		locationStat.AddLine(temp)
	}
	(*sm)[location] = locationStat
}

func CombineMaps(sm *map[string]Stats, other map[string]Stats) {
	for location, otherStat := range other {
		locationStat, ok := (*sm)[location]
		if !ok {
			locationStat = otherStat
		} else {
			locationStat.Combine(otherStat)
		}
		(*sm)[location] = locationStat
	}
}

func printMap(sm map[string]Stats) {
	for k, v := range sm {
		fmt.Printf("%v;%v\n", k, v.Print())
	}
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

	buf := make([]byte, 1_000)
	currentLocation := start
	carryOverBytes := []byte{}

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

		lineStart := 0
		for i, b := range byteData {
			if b == '\n' {
				location, temp, err := ProcessLine(append(carryOverBytes, byteData[lineStart:i]...))
				if err != nil {
					fmt.Println(err)
				} else {
					AddToMap(&result, location, temp)
				}
				lineStart = i + 1
				carryOverBytes = []byte{}
			}
		}
		carryOverBytes = make([]byte, len(byteData[lineStart:]))
		copy(carryOverBytes, byteData[lineStart:])
	}
	location, temp, err := ProcessLine(carryOverBytes)
	if err != nil {
		fmt.Println(err)
	} else {
		AddToMap(&result, location, temp)
	}
	c <- result
}

func ProcessLine(b []byte) (string, int64, error) {
	// fmt.Println(string(b));
	defer func() {
		// recover from panic if one occurred. Set err to nil otherwise.
		if recover() != nil {
			fmt.Println(string(b))
			panic("t")
		}
	}()
	values := strings.Split(string(b), ";")
	if len(values) != 2 {
		return "", 0, fmt.Errorf("Empty row %v", values)
	}
	location := values[0]

	fTemp, err := strconv.ParseFloat(values[1], 64)
	if err != nil {
		panic(err)
	}
	temp := int64(fTemp * 10)
	return location, temp, nil
}
