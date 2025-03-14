package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "github.com/rclone/rclone/backend/b2" // Import desired backends
	_ "github.com/rclone/rclone/backend/s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/walk"
)

// Define Prometheus metrics for bucket size and file count.
var (
	bucketSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rclone_bucket_size_bytes",
			Help: "Total size in bytes for a bucket",
		},
		[]string{"remote", "bucket"},
	)
	bucketFileCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rclone_bucket_file_count",
			Help: "File count for a bucket",
		},
		[]string{"remote", "bucket"},
	)
)

func init() {
	prometheus.MustRegister(bucketSize)
	prometheus.MustRegister(bucketFileCount)
}

// ListDir lists the top-level directories (buckets) of the given Fs.
func ListDir(ctx context.Context, f fs.Fs) (fs.DirEntries, error) {
	dirs := fs.DirEntries{}
	err := walk.ListR(ctx, f, "", false, 1, walk.ListDirs, func(entries fs.DirEntries) error {
		entries.ForDir(func(dir fs.Directory) {
			if dir != nil {
				dirs = append(dirs, dir)
			}
		})
		return nil
	})
	return dirs, err
}

// updateRemoteBuckets lists the top-level directories (buckets) in the given remote using ListDir(),
// then for each bucket, it calls operations.Count() to get the file count and total size.
func updateRemoteBuckets(ctx context.Context, remote string) {
	// Create a new Fs for the remote.
	f, err := fs.NewFs(ctx, remote)
	if err != nil {
		log.Printf("Error creating Fs for remote %q: %v", remote, err)
		return
	}

	// List top-level directories (buckets). The empty string ("") lists the root.
	dirs, err := ListDir(ctx, f)
	if err != nil {
		log.Printf("Error listing directories for remote %q: %v", remote, err)
		return
	}

	for _, d := range dirs {
		// Get the bucket name from the directory entry.
		bucketName := d.Remote()
		// Construct the bucket remote. For example, "b2:" + "mybucket" becomes "b2:mybucket".
		bucketRemote := remote + bucketName

		// Create a new Fs for the bucket.
		bucketFs, err := fs.NewFs(ctx, bucketRemote)
		if err != nil {
			log.Printf("Error creating Fs for bucket %q: %v", bucketRemote, err)
			continue
		}

		// operations.Count returns file count, directory count, and total size in bytes.
		// We ignore the directory count.
		files, size, _, err := operations.Count(ctx, bucketFs)
		if err != nil {
			log.Printf("Error counting bucket %q: %v", bucketRemote, err)
			continue
		}

		// Update Prometheus metrics.
		bucketSize.WithLabelValues(remote, bucketName).Set(float64(size))
		bucketFileCount.WithLabelValues(remote, bucketName).Set(float64(files))
		log.Printf("Updated bucket %q: size=%d bytes, file count=%d", bucketRemote, size, files)
	}
}

// updateRemotes runs updateRemoteBuckets on each remote in a goroutine
func updateRemotes(ctx context.Context, remotes []string) {
	for _, remote := range remotes {
		go updateRemoteBuckets(ctx, remote)
	}
}

func main() {
	// Parse command-line arguments.
	remotesFlag := flag.String("remote", "", "Comma separated list of remotes to monitor (REQUIRED)")
	updatePeriodFlag := flag.Int("update-period", 60, "Update period in minutes")
	listenAddrFlag := flag.String("listen", ":8080", "Address to listen on for serving metrics")
	remoteTimeoutFlag := flag.Int("remote-timeout", 30, "Timeout in seconds for calls to the remotes")
	flag.Parse()

	if *remotesFlag == "" {
		log.Print("FATAL: atleast one remote must be configured with -remote")
		flag.Usage()
		os.Exit(1)
	}
	// Split the comma separated remotes into a slice.
	remotes := []string{}
	for _, remote := range strings.Split(*remotesFlag, ",") {
		remotes = append(remotes, strings.TrimSpace(remote))
	}
	timeout := time.Duration(*remoteTimeoutFlag) * time.Second

	ctx := context.Background()
	// Install config file (required by rclone).
	configfile.Install()

	// Start a goroutine to periodically update bucket metrics.
	go func() {
		ticker := time.NewTicker(time.Duration(*updatePeriodFlag) * time.Minute)
		defer ticker.Stop()
		// Run an update immediately.
		ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		updateRemotes(ctxTimeout, remotes)
		// Update periodically.
		for {
			select {
			case <-ticker.C:
				ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				updateRemotes(ctxTimeout, remotes)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Expose Prometheus metrics via HTTP.
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Serving Prometheus metrics on %s/metrics", *listenAddrFlag)
	if err := http.ListenAndServe(*listenAddrFlag, nil); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}
