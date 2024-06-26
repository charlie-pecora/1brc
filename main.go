package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

const inputFile = "./measurements.txt"
const numWorkers = 12

func main() {
	Run();
}

func Run() {
	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	segments := getFileSegments(f)

	c := make(chan StatsMap, numWorkers)

	for worker := 0; worker < numWorkers; worker++ {
		go ProcessChunk(segments[worker], segments[worker+1], c)
	}
	finalResult := make(StatsMap, 1000)
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

func (s Stats) Mean() string {
	return fmt.Sprintf("%.1f", float64(s.sum)/float64(s.count) / 10.0)
}

func (s Stats) Min() string {
	return fmt.Sprintf("%.1f", float64(s.min) / 10.0)
}

func (s Stats) Max() string {
	return fmt.Sprintf("%.1f", float64(s.max) / 10.0)
}

func (s Stats) Print() string {
	return fmt.Sprintf("%v/%v/%v", s.Min(), s.Mean(), s.Max())
}

type Location = string;
type StatsMap = map[string]Stats;

func makeStats(new int64) Stats {
	return Stats{
		max:   new,
		min:   new,
		sum:   new,
		count: new,
	}

}

func AddToMap(sm *StatsMap, location string, temp int64) {
	locationStat, ok := (*sm)[location]
	if !ok {
		locationStat = makeStats(temp)
	} else {
		locationStat.AddLine(temp)
	}
	(*sm)[location] = locationStat
}

func CombineMaps(sm *StatsMap, other StatsMap) {
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

func printMap(sm StatsMap) {
	for k, v := range sm {
		fmt.Printf("%v;%v\n", k, v.Print())
	}
}

func ProcessChunk(start, end int64, c chan StatsMap) {
	result := make(StatsMap)

	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	_, err = f.Seek(start, 0)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 1024 * 1024 * 512)
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
				if len(carryOverBytes) > 0 {
					location, temp, err := ProcessLine(append(carryOverBytes, byteData[lineStart:i]...))
					if err != nil {
						log.Println(err)
					} else {
						AddToMap(&result, location, temp)
					}
				} else {
					location, temp, err := ProcessLine(byteData[lineStart:i])
					if err != nil {
						log.Println(err)
					} else {
						AddToMap(&result, location, temp)
					}
				}
				lineStart = i + 1
				carryOverBytes = carryOverBytes[:0]
			}
		}
		carryOverBytes = append(carryOverBytes, byteData[lineStart:]...)
	}
	location, temp, err := ProcessLine(carryOverBytes)
	if err != nil {
		log.Println(err)
	} else {
		AddToMap(&result, location, temp)
	}
	c <- result
}

func ProcessLine(b []byte) (Location, int64, error) {
	// fmt.Println(string(b));
	values := bytes.Split(b, []byte(";"))
	if len(values) != 2 {
		return Location([]byte{}), 0, fmt.Errorf("Empty row %v", values)
	}
	location := values[0]

	temp := ParseInt(values[1]);
	return Location(location), temp, nil
}

func ParseInt(v []byte) int64 {
	f, err := strconv.ParseFloat(string(v), 32);
	if err != nil {
		panic(err)
	}
	return int64(f * 10.0)
}
