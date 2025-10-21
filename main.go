package main

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/schollz/progressbar/v3"
)

// RuleSet holds the paths and their directive (allow/disallow) for a specific user-agent.
type RuleSet map[string]string // Key: path, Value: "allow" or "disallow"

// AgentRules holds the rules for all user-agents in a robots.txt file.
type AgentRules map[string]RuleSet // Key: user-agent

// VersionContent holds the timestamp, rules, and raw content from a robots.txt version.
type VersionContent struct {
	Timestamp  string
	Rules      AgentRules
	RawContent string // Store the raw text content
}

func main() {
	versionsLimit := flag.Int("limit", 100, "limit the number crawled snapshots. Use -1 for unlimited")
	recent := flag.Bool("recent", true, "use the most recent snapshots without evenly distributing them")
	timeline := flag.Bool("timeline", false, "show a timeline of changes in robots.txt")
	year := flag.Int("year", 0, "specify a year to fetch timeline changes for (e.g., 2023). Overrides -limit and -recent.")
	outputDir := flag.String("output", "", "directory to save JSON and raw .txt output")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		u, err := cleanURL(scanner.Text())
		if err != nil {
			continue
		}

		if !*timeline {
			// Original functionality
			processURL(u, *versionsLimit, *recent, *outputDir)
		} else {
			// New timeline functionality
			createTimeline(u, *versionsLimit, *recent, *year, *outputDir)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading URLs from stdin: %v\n", err)
		os.Exit(1)
	}
}

func processURL(u string, limit int, recent bool, outputDir string) {
	// Pass 0 for year to use default limit/recent logic
	versions, err := GetRobotsTxtVersions(u, limit, recent, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting versions: %v\n", err)
		return
	}

	numThreads := 10
	jobCh := make(chan string, numThreads)
	pathCh := make(chan []string)

	progressbarMessage := fmt.Sprintf("Enumerating %s/robots.txt versions...", u)
	bar := progressbar.Default(int64(len(versions)), progressbarMessage)

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func() {
			defer wg.Done()
			for version := range jobCh {
				GetRobotsTxtPaths(version, u, pathCh, bar)
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

	if outputDir != "" {
		writePathsJSON(u, allPaths, outputDir)
	} else {
		for path := range allPaths {
			fmt.Println(path)
		}
	}
}

func createTimeline(u string, limit int, recent bool, year int, outputDir string) {
	versions, err := GetRobotsTxtVersions(u, limit, recent, year)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting versions: %v\n", err)
		return
	}
	if len(versions) == 0 {
		fmt.Fprintf(os.Stderr, "No versions found for %s (Year: %d)\n", u, year)
		return
	}

	numThreads := 10
	jobCh := make(chan string, numThreads)
	resultCh := make(chan VersionContent, len(versions))

	progressbarMessage := fmt.Sprintf("Fetching %s/robots.txt versions for timeline...", u)
	bar := progressbar.Default(int64(len(versions)), progressbarMessage)

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func() {
			defer wg.Done()
			for version := range jobCh {
				rules, rawContent := GetRobotsTxtPathsForTimeline(version, u, bar)
				resultCh <- VersionContent{Timestamp: version, Rules: rules, RawContent: rawContent}
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

	if outputDir != "" {
		writeTimelineOutput(u, versionContents, year, outputDir)
		return
	}

	// Compare versions and print timeline to STDOUT
	var previousRules AgentRules
	for _, vc := range versionContents {
		addedAgents := []string{}
		removedAgents := []string{}
		ruleChanges := false

		// Find added/changed agents
		for agent, currentRules := range vc.Rules {
			prevAgentRules, exists := previousRules[agent]
			if !exists {
				addedAgents = append(addedAgents, agent)
				ruleChanges = true
				continue
			}

			// Check for path changes within the agent
			addedAllows, removedAllows, addedDisallows, removedDisallows := diffRuleSets(currentRules, prevAgentRules)
			if len(addedAllows) > 0 || len(removedAllows) > 0 || len(addedDisallows) > 0 || len(removedDisallows) > 0 {
				ruleChanges = true
			}
		}

		// Find removed agents
		for agent := range previousRules {
			if _, exists := vc.Rules[agent]; !exists {
				removedAgents = append(removedAgents, agent)
				ruleChanges = true
			}
		}

		if !ruleChanges && len(addedAgents) == 0 && len(removedAgents) == 0 && previousRules != nil {
			continue // Skip if no changes *and* it's not the first version
		}

		fmt.Printf("\n--- Changes on %s ---\n", vc.Timestamp)

		if previousRules == nil {
			fmt.Println("Initial version:")
			for agent, rules := range vc.Rules {
				fmt.Printf("  User-agent: %s\n", agent)
				allows := []string{}
				disallows := []string{}
				for path, directive := range rules {
					if directive == "allow" {
						allows = append(allows, path)
					} else {
						disallows = append(disallows, path)
					}
				}
				sort.Strings(allows)
				sort.Strings(disallows)

				if len(allows) > 0 {
					fmt.Println("    Allow:")
					for _, path := range allows {
						fmt.Printf("      + %s\n", path)
					}
				}
				if len(disallows) > 0 {
					fmt.Println("    Disallow:")
					for _, path := range disallows {
						fmt.Printf("      + %s\n", path)
					}
				}
			}
		} else {
			for _, agent := range addedAgents {
				fmt.Printf("  [+] New User-agent: %s\n", agent)
				// Similar logic as initial version to print all rules for the new agent
				rules := vc.Rules[agent]
				allows := []string{}
				disallows := []string{}
				for path, directive := range rules {
					if directive == "allow" {
						allows = append(allows, path)
					} else {
						disallows = append(disallows, path)
					}
				}
				sort.Strings(allows)
				sort.Strings(disallows)
				if len(allows) > 0 {
					fmt.Println("    Allow:")
					for _, path := range allows {
						fmt.Printf("      + %s\n", path)
					}
				}
				if len(disallows) > 0 {
					fmt.Println("    Disallow:")
					for _, path := range disallows {
						fmt.Printf("      + %s\n", path)
					}
				}
			}
			for _, agent := range removedAgents {
				fmt.Printf("  [-] Removed User-agent: %s\n", agent)
			}

			for agent, currentRules := range vc.Rules {
				if prevAgentRules, exists := previousRules[agent]; exists {
					addedAllows, removedAllows, addedDisallows, removedDisallows := diffRuleSets(currentRules, prevAgentRules)

					if len(addedAllows) > 0 || len(removedAllows) > 0 || len(addedDisallows) > 0 || len(removedDisallows) > 0 {
						fmt.Printf("  [~] Changed User-agent: %s\n", agent)
						if len(addedAllows) > 0 || len(removedAllows) > 0 {
							fmt.Println("    Allow:")
							for _, path := range addedAllows {
								fmt.Printf("      + %s\n", path)
							}
							for _, path := range removedAllows {
								fmt.Printf("      - %s\n", path)
							}
						}
						if len(addedDisallows) > 0 || len(removedDisallows) > 0 {
							fmt.Println("    Disallow:")
							for _, path := range addedDisallows {
								fmt.Printf("      + %s\n", path)
							}
							for _, path := range removedDisallows {
								fmt.Printf("      - %s\n", path)
							}
						}
					}
				}
			}
		}
		previousRules = vc.Rules
	}
}

func diffRuleSets(current, previous RuleSet) (addedAllows, removedAllows, addedDisallows, removedDisallows []string) {
	for path, directive := range current {
		prevDirective, exists := previous[path]
		if !exists { // Path is new
			if directive == "allow" {
				addedAllows = append(addedAllows, path)
			} else {
				addedDisallows = append(addedDisallows, path)
			}
		} else if directive != prevDirective { // Path changed directive
			if directive == "allow" { // Was disallow, now allow
				addedAllows = append(addedAllows, path)
				removedDisallows = append(removedDisallows, path)
			} else { // Was allow, now disallow
				addedDisallows = append(addedDisallows, path)
				removedAllows = append(removedAllows, path)
			}
		}
	}

	for path, prevDirective := range previous {
		if _, exists := current[path]; !exists { // Path was removed
			if prevDirective == "allow" {
				removedAllows = append(removedAllows, path)
			} else {
				removedDisallows = append(removedDisallows, path)
			}
		}
	}
	sort.Strings(addedAllows)
	sort.Strings(removedAllows)
	sort.Strings(addedDisallows)
	sort.Strings(removedDisallows)
	return
}

func writePathsJSON(u string, paths map[string]bool, outputDir string) {
	domain := getHost(u)
	dirPath := filepath.Join(outputDir, domain)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory %s: %v\n", dirPath, err)
		return
	}

	pathList := make([]string, 0, len(paths))
	for path := range paths {
		pathList = append(pathList, path)
	}
	sort.Strings(pathList)

	filePath := filepath.Join(dirPath, "paths.json")
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file %s: %v\n", filePath, err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(pathList); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing JSON to %s: %v\n", filePath, err)
	} else {
		fmt.Fprintf(os.Stderr, "Wrote paths to %s\n", filePath)
	}
}

// writeTimelineOutput handles writing both the JSON delta file and the raw
// robots.txt files for the specified year.
func writeTimelineOutput(u string, versionContents []VersionContent, year int, outputDir string) {
    if len(versionContents) == 0 {
        fmt.Fprintf(os.Stderr, "No versions to write for %s\n", u)
        return
    }

    domain := getHost(u)
    var dirPath string
    var jsonFileName string

    if year > 0 {
        dirPath = filepath.Join(outputDir, domain, strconv.Itoa(year))
        jsonFileName = fmt.Sprintf("timeline_%d.json", year)
    } else {
        dirPath = filepath.Join(outputDir, domain)
        jsonFileName = "timeline.json"
    }

    if err := os.MkdirAll(dirPath, 0755); err != nil {
        fmt.Fprintf(os.Stderr, "Error creating directory %s: %v\n", dirPath, err)
        return
    }

    // --- Structs for JSON output ---
    type changeSet struct {
        Added   []string `json:"added,omitempty"`
        Removed []string `json:"removed,omitempty"`
    }
    type ruleChange struct {
        UserAgent string    `json:"user_agent"`
        Allow     changeSet `json:"allow,omitempty"`
        Disallow  changeSet `json:"disallow,omitempty"`
    }
    type timelineEntry struct {
        Timestamp      string       `json:"timestamp"`
        AgentsAdded    []string     `json:"agents_added,omitempty"`
        AgentsRemoved  []string     `json:"agents_removed,omitempty"`
        RuleChanges    []ruleChange `json:"rule_changes,omitempty"`
        InitialContent []ruleChange `json:"initial_content,omitempty"`
    }

    var timeline []timelineEntry
    var previousRules AgentRules
    filesToZip := make(map[string]string) // K: filename, V: content

    // --- Process versions to find changes and collect files to zip ---
    for _, vc := range versionContents {
        entry := timelineEntry{Timestamp: vc.Timestamp}
        hasChanges := false

        if previousRules == nil {
            // --- Initial version (for JSON) ---
            hasChanges = true // The first entry is always included in the timeline
            for agent, rules := range vc.Rules {
                allows := []string{}
                disallows := []string{}
                for path, directive := range rules {
                    if directive == "allow" {
                        allows = append(allows, path)
                    } else {
                        disallows = append(disallows, path)
                    }
                }
                sort.Strings(allows)
                sort.Strings(disallows)
                change := ruleChange{UserAgent: agent}
                if len(allows) > 0 {
                    change.Allow.Added = allows
                }
                if len(disallows) > 0 {
                    change.Disallow.Added = disallows
                }
                entry.InitialContent = append(entry.InitialContent, change)
            }
        } else {
            // --- Compare with previous version (for JSON and raw file logic) ---
            // Find added agents
            for agent, rules := range vc.Rules {
                if _, exists := previousRules[agent]; !exists {
                    entry.AgentsAdded = append(entry.AgentsAdded, agent)
                    // also list the initial rules for the new agent
                    allows := []string{}
                    disallows := []string{}
                    for path, directive := range rules {
                        if directive == "allow" {
                            allows = append(allows, path)
                        } else {
                            disallows = append(disallows, path)
                        }
                    }
                    sort.Strings(allows)
                    sort.Strings(disallows)
                    change := ruleChange{UserAgent: agent}
                    if len(allows) > 0 {
                        change.Allow.Added = allows
                    }
                    if len(disallows) > 0 {
                        change.Disallow.Added = disallows
                    }
                    entry.RuleChanges = append(entry.RuleChanges, change)
                    hasChanges = true
                }
            }
            sort.Strings(entry.AgentsAdded)

            // Find removed agents
            for agent := range previousRules {
                if _, exists := vc.Rules[agent]; !exists {
                    entry.AgentsRemoved = append(entry.AgentsRemoved, agent)
                    hasChanges = true
                }
            }
            sort.Strings(entry.AgentsRemoved)

            // Find rule changes for existing agents
            for agent, currentRules := range vc.Rules {
                if prevAgentRules, exists := previousRules[agent]; exists {
                    addedAllows, removedAllows, addedDisallows, removedDisallows := diffRuleSets(currentRules, prevAgentRules)

                    if len(addedAllows) > 0 || len(removedAllows) > 0 || len(addedDisallows) > 0 || len(removedDisallows) > 0 {
                        change := ruleChange{UserAgent: agent}
                        change.Allow = changeSet{Added: addedAllows, Removed: removedAllows}
                        change.Disallow = changeSet{Added: addedDisallows, Removed: removedDisallows}
                        entry.RuleChanges = append(entry.RuleChanges, change)
                        hasChanges = true
                    }
                }
            }
        }

        // --- Collect raw .txt file content if this is the first one or if there are changes ---
        if (previousRules == nil || hasChanges) && vc.RawContent != "" {
            if year > 0 {
                // If year is specified, add to zip map instead of writing directly
                fileName := fmt.Sprintf("robots_%s.txt", vc.Timestamp)
                filesToZip[fileName] = vc.RawContent
            } else {
                // Original behavior: write individual files if not using -year
                rawFileName := fmt.Sprintf("robots_%s.txt", vc.Timestamp)
                rawFilePath := filepath.Join(dirPath, rawFileName)
                err := ioutil.WriteFile(rawFilePath, []byte(vc.RawContent), 0644)
                if err != nil {
                    fmt.Fprintf(os.Stderr, "Error writing raw file %s: %v\n", rawFilePath, err)
                }
            }
        }

        if hasChanges {
            timeline = append(timeline, entry)
        }
        previousRules = vc.Rules
    }

    // --- Write the collected .txt files to a zip archive if year is specified ---
    if year > 0 && len(filesToZip) > 0 {
        zipFileName := fmt.Sprintf("robots_txt_%d.zip", year)
        zipFilePath := filepath.Join(dirPath, zipFileName)
        zipFile, err := os.Create(zipFilePath)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error creating zip file %s: %v\n", zipFilePath, err)
            return
        }
        defer zipFile.Close()

        zipWriter := zip.NewWriter(zipFile)
        defer zipWriter.Close()

        for name, content := range filesToZip {
            f, err := zipWriter.Create(name)
            if err != nil {
                fmt.Fprintf(os.Stderr, "Error adding file %s to zip: %v\n", name, err)
                continue
            }
            _, err = f.Write([]byte(content))
            if err != nil {
                fmt.Fprintf(os.Stderr, "Error writing content for file %s to zip: %v\n", name, err)
                continue
            }
        }
        fmt.Fprintf(os.Stderr, "Wrote %d txt files to %s\n", len(filesToZip), zipFilePath)
    }

    // --- Write the JSON timeline.json file ---
    jsonFilePath := filepath.Join(dirPath, jsonFileName)
    file, err := os.Create(jsonFilePath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error creating file %s: %v\n", jsonFilePath, err)
        return
    }
    defer file.Close()

    encoder := json.NewEncoder(file)
    encoder.SetIndent("", "  ")
    if err := encoder.Encode(timeline); err != nil {
        fmt.Fprintf(os.Stderr, "Error writing JSON to %s: %v\n", jsonFilePath, err)
    } else {
        fmt.Fprintf(os.Stderr, "Wrote timeline to %s\n", jsonFilePath)
    }
}

func GetRobotsTxtVersions(url string, limit int, recent bool, year int) ([]string, error) {
	var requestURL string

	if year > 0 {
		// Year is specified, override limit/recent and use from/to
		from := fmt.Sprintf("%d0101000000", year)
		to := fmt.Sprintf("%d1231235959", year)
		requestURL = fmt.Sprintf("https://web.archive.org/cdx/search/cdx?url=%s/robots.txt&output=json&fl=timestamp&filter=statuscode:200&collapse=digest&from=%s&to=%s", url, from, to)
	} else {
		// No year, use original logic
		requestURL = fmt.Sprintf("https://web.archive.org/cdx/search/cdx?url=%s/robots.txt&output=json&fl=timestamp&filter=statuscode:200&collapse=digest", url)
		if limit != -1 && recent {
			requestURL += "&limit=-" + strconv.Itoa(limit)
		}
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

	versions = versions[1:] // Skip header row

	selectedVersions := make([]string, 0)
	length := len(versions)

	if year > 0 {
		// If year was specified, we want all versions returned
		for _, version := range versions {
			selectedVersions = append(selectedVersions, version...)
		}
	} else {
		// Use original limit/recent logic if no year was given
		if recent || limit == -1 || length <= limit {
			for _, version := range versions {
				selectedVersions = append(selectedVersions, version...)
			}
		} else {
			interval := float64(length) / float64(limit-1)
			for i := 0; i < limit; i++ {
				index := int(float64(i) * interval)
				if i == limit-1 {
					index = length - 1 // Ensure last index is always included
				}
				if index >= length {
					index = length - 1
				}
				selectedVersions = append(selectedVersions, versions[index]...)
			}
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
	defer res.Body.Close()

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

// GetRobotsTxtPathsForTimeline parses a robots.txt version and returns its rules and raw content.
func GetRobotsTxtPathsForTimeline(version string, u string, bar *progressbar.ProgressBar) (AgentRules, string) {
	requestURL := fmt.Sprintf("https://web.archive.org/web/%sif_/%s/robots.txt", version, u)
	res, err := http.Get(requestURL)
	bar.Add(1)
	if err != nil {
		return nil, ""
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, ""
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, ""
	}
	rawContent := string(body)
	allRules := make(AgentRules)

	var currentAgents []string
	lastDirectiveWasAgent := false

	scanner := bufio.NewScanner(strings.NewReader(rawContent))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		directive := strings.ToLower(strings.TrimSpace(parts[0]))
		value := strings.TrimSpace(parts[1])

		switch directive {
		case "user-agent":
			if !lastDirectiveWasAgent {
				// This is the start of a new agent group, clear the previous list.
				currentAgents = []string{}
			}
			currentAgents = append(currentAgents, value)
			lastDirectiveWasAgent = true
		case "allow", "disallow":
			if len(currentAgents) == 0 {
				continue // Rule without a user-agent
			}
			// Use the raw path from the file, but create a full URL for comparison
			// Note: The diff logic relies on paths being consistent.
			// Using the merged URL path ensures "path" and "/path" are treated same.
			fullPath, err := mergeURLPath(u, value)
			if err != nil {
				continue
			}
			for _, agent := range currentAgents {
				if _, ok := allRules[agent]; !ok {
					allRules[agent] = make(RuleSet)
				}
				// Store the full path for consistent diffing
				allRules[agent][fullPath] = directive
			}
			lastDirectiveWasAgent = false
		default:
			// Any other directive (like Sitemap) also breaks an agent group.
			lastDirectiveWasAgent = false
		}
	}
	return allRules, rawContent
}

func mergeURLPath(baseURL, path string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Use ResolveReference to correctly handle paths
	pathURL, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	
	resolvedURL := base.ResolveReference(pathURL)
	return resolvedURL.String(), nil
}

func getHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Host
}

func cleanURL(baseURL string) (string, error) {
	// Trim protocol if present for parsing
	cleanBase := strings.TrimPrefix(strings.TrimPrefix(baseURL, "https://"), "http://")

	u, err := url.Parse("https://" + cleanBase) // Default to https for parsing
	if err != nil {
		return "", err
	}

	// Re-parse with the original string to detect scheme
	originalURL, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	
	scheme := "https" // Default
	if originalURL.Scheme != "" {
		scheme = originalURL.Scheme
	}

	return fmt.Sprintf("%s://%s", scheme, u.Host), nil
}