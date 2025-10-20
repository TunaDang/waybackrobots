package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sort"
	"github.com/schollz/progressbar/v3"
)

// VersionContent holds the timestamp and the paths from a robots.txt version.
type VersionContent struct {
	Timestamp string
	Paths     map[string]bool
}

func main() {
	versionsLimit := flag.Int("limit", 50, "limit the number crawled snapshots. Use -1 for unlimited")
	recent := flag.Bool("recent", false, "use the most recent snapshots without evenly distributing them")
	timeline := flag.Bool("timeline", false, "show a timeline of changes in robots.txt")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		url, err := cleanURL(scanner.Text())
		if err != nil {
			continue
		}

		if !*timeline {
			// Original functionality
			processURL(url, *versionsLimit, *recent)
		} else {
			// New timeline functionality
			createTimeline(url, *versionsLimit, *recent)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading URLs from stdin: %v\n", err)
		os.Exit(1)
	}
}

func processURL(url string, limit int, recent bool) {
	versions, err := GetRobotsTxtVersions(url, limit, recent)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting versions: %v\n", err)
		return
	}

	numThreads := 10
	jobCh := make(chan string, numThreads)
	pathCh := make(chan []string)

	progressbarMessage := fmt.Sprintf("Enumerating %s/robots.txt versions...", url)
	bar := progressbar.Default(int64(len(versions)), progressbarMessage)

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func() {
			defer wg.Done()
			for version := range jobCh {
				GetRobotsTxtPaths(version, url, pathCh, bar)
			}
		}()
	}

	go func() {
		for _, version := range versions {
			jobCh <- version
		}
		close(jobCh)
	}()

	go func() {
		wg.Wait()
		close(pathCh)
	}()

	allPaths := make(map[string]bool)
	for pathsBatch := range pathCh {
		for _, path := range pathsBatch {
			allPaths[path] = true
		}
	}

	for path := range allPaths {
		fmt.Println(path)
	}
}

func createTimeline(url string, limit int, recent bool) {
	versions, err := GetRobotsTxtVersions(url, limit, recent)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting versions: %v\n", err)
		return
	}

	numThreads := 10
	jobCh := make(chan string, numThreads)
	resultCh := make(chan VersionContent, len(versions))

	progressbarMessage := fmt.Sprintf("Fetching %s/robots.txt versions for timeline...", url)
	bar := progressbar.Default(int64(len(versions)), progressbarMessage)

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func() {
			defer wg.Done()
			for version := range jobCh {
				paths := GetRobotsTxtPathsForTimeline(version, url, bar)
				resultCh <- VersionContent{Timestamp: version, Paths: paths}
			}
		}()
	}

	for _, version := range versions {
		jobCh <- version
	}
	close(jobCh)

	wg.Wait()
	close(resultCh)

	// Sort versions by timestamp
	versionContents := make([]VersionContent, 0, len(versions))
	for vc := range resultCh {
		versionContents = append(versionContents, vc)
	}
	sort.Slice(versionContents, func(i, j int) bool {
		return versionContents[i].Timestamp < versionContents[j].Timestamp
	})

	// Compare versions and print timeline
	var previousPaths map[string]bool
	for _, vc := range versionContents {
		fmt.Printf("\n--- Changes on %s ---\n", vc.Timestamp)
		if previousPaths == nil {
			fmt.Println("Initial version:")
			for path := range vc.Paths {
				fmt.Printf("+ %s\n", path)
			}
		} else {
			// Find added paths
			for path := range vc.Paths {
				if !previousPaths[path] {
					fmt.Printf("+ %s\n", path)
				}
			}
			// Find removed paths
			for path := range previousPaths {
				if !vc.Paths[path] {
					fmt.Printf("- %s\n", path)
				}
			}
		}
		previousPaths = vc.Paths
	}
}

func GetRobotsTxtVersions(url string, limit int, recent bool) ([]string, error) {
	requestURL := fmt.Sprintf("https://web.archive.org/cdx/search/cdx?url=%s/robots.txt&output=json&fl=timestamp&filter=statuscode:200&collapse=digest", url)
	if limit != -1 && recent {
		requestURL += "&limit=-" + strconv.Itoa(limit)
	}

	res, err := http.Get(requestURL)
	if err != nil {
		return nil, err
	}

	raw, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	var versions [][]string
	err = json.Unmarshal(raw, &versions)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return []string{}, nil
	}

	versions = versions[1:]

	selectedVersions := make([]string, 0)
	length := len(versions)

	if recent || limit == -1 || length <= limit {
		for _, version := range versions {
			selectedVersions = append(selectedVersions, version...)
		}
	} else {
		interval := length / (limit - 1)

		for i := 0; i < limit; i++ {
			index := i * interval
			if index >= length {
				index = length - (limit - i)
			}
			selectedVersions = append(selectedVersions, versions[index]...)
		}
	}
	return selectedVersions, nil
}

func GetRobotsTxtPaths(version string, url string, pathCh chan []string, bar *progressbar.ProgressBar) {
	requestURL := fmt.Sprintf("https://web.archive.org/web/%sif_/%s/robots.txt", version, url)
	res, err := http.Get(requestURL)
	bar.Add(1)
	if err != nil || res.StatusCode != 200 {
		return
	}

	outputURLs := make([]string, 0)

	scanner := bufio.NewScanner(res.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "Disallow:") || strings.HasPrefix(line, "Allow:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			path := strings.TrimSpace(fields[1])
			if path != "" {
				fullURL, err := mergeURLPath(url, path)
				if err != nil {
					continue
				}
				outputURLs = append(outputURLs, fullURL)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return
	}
	pathCh <- outputURLs
}

func GetRobotsTxtPathsForTimeline(version string, url string, bar *progressbar.ProgressBar) map[string]bool {
	requestURL := fmt.Sprintf("https://web.archive.org/web/%sif_/%s/robots.txt", version, url)
	res, err := http.Get(requestURL)
	bar.Add(1)
	if err != nil || res.StatusCode != 200 {
		return nil
	}
	defer res.Body.Close()

	paths := make(map[string]bool)

	scanner := bufio.NewScanner(res.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "Disallow:") || strings.HasPrefix(line, "Allow:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			path := strings.TrimSpace(fields[1])
			if path != "" {
				fullURL, err := mergeURLPath(url, path)
				if err != nil {
					continue
				}
				paths[fullURL] = true
			}
		}
	}
	return paths
}

func mergeURLPath(baseURL, path string) (string, error) {
	host, err := cleanURL(baseURL)
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	url := fmt.Sprintf(host + path)
	return url, nil
}

func cleanURL(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	if u.Scheme == "" {
		u.Scheme = "https"
		u.Host = baseURL
	}

	return fmt.Sprintf("%s://%s", u.Scheme, u.Host), nil
}
