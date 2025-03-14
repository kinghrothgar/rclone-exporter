package main

import (
	"context"
	"flag"
	"net/http"
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
	"github.com/sirupsen/logrus"
)

// Define Prometheus metrics for bucket size and file count
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

// ListDir lists the top-level directories (buckets) of the given Fs
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
// then for each bucket, it calls operations.Count() to get the file count and total size
func updateRemoteBuckets(ctx context.Context, remote string) {
	// Create a new Fs for the remote
	f, err := fs.NewFs(ctx, remote)
	if err != nil {
		logrus.WithField("remote", remote).WithError(err).Error("failed creating Fs for remote")
		return
	}

	// List top-level directories (buckets). The empty string ("") lists the root
	dirs, err := ListDir(ctx, f)
	if err != nil {
		logrus.WithField("remote", remote).WithError(err).Error("failed listing directories for remote")
		return
	}

	for _, d := range dirs {
		// Get the bucket name from the directory entry
		bucketName := d.Remote()
		// Construct the bucket remote. For example, "b2:" + "mybucket" becomes "b2:mybucket"
		bucketRemote := remote + bucketName
		contextLogger := logrus.WithField("bucket", bucketRemote)

		// Create a new Fs for the bucket
		bucketFs, err := fs.NewFs(ctx, bucketRemote)
		if err != nil {
			contextLogger.WithError(err).Error("failed creating Fs for bucket")
			continue
		}

		// operations.Count returns file count, directory count, and total size in bytes
		// We ignore the directory count
		files, size, _, err := operations.Count(ctx, bucketFs)
		if err != nil {
			contextLogger.WithError(err).Error("failed counting bucket")
			continue
		}

		// Update Prometheus metrics
		bucketSize.WithLabelValues(remote, bucketName).Set(float64(size))
		bucketFileCount.WithLabelValues(remote, bucketName).Set(float64(files))
		contextLogger.WithFields(logrus.Fields{
			"size":  size,
			"count": files,
		}).Info("updated bucket metrics")
	}
}

// updateRemotes runs updateRemoteBuckets on each remote in a goroutine
func updateRemotes(ctx context.Context, remotes []string) {
	for _, remote := range remotes {
		go updateRemoteBuckets(ctx, remote)
	}
}

func main() {
	// Parse command-line arguments
	remotesFlag := flag.String("remote", "", "comma separated list of remotes to monitor (REQUIRED)")
	updatePeriodFlag := flag.Int("update-period", 60, "update period in minutes")
	listenAddrFlag := flag.String("listen", ":8080", "address to listen on for serving metrics")
	remoteTimeoutFlag := flag.Int("remote-timeout", 30, "timeout in seconds for calls to the remotes")
	logJSONFlag := flag.Bool("log-json", false, "output logs in json")
	flag.Parse()

	if *logJSONFlag {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	if *remotesFlag == "" {
		if !*logJSONFlag {
			flag.Usage()
		}
		logrus.Fatal("at least one remote must be configured with -remote")
	}

	// Split the comma separated remotes into a slice
	remotes := []string{}
	for _, remote := range strings.Split(*remotesFlag, ",") {
		remotes = append(remotes, strings.TrimSpace(remote))
	}
	timeout := time.Duration(*remoteTimeoutFlag) * time.Second

	ctx := context.Background()
	// Install config file (required by rclone)
	configfile.Install()

	// Start a goroutine to periodically update bucket metrics
	go func() {
		ticker := time.NewTicker(time.Duration(*updatePeriodFlag) * time.Minute)
		defer ticker.Stop()
		// Run an update immediately
		ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
		updateRemotes(ctxTimeout, remotes)
		cancel()
		// Update periodically
		for {
			select {
			case <-ticker.C:
				ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
				updateRemotes(ctxTimeout, remotes)
				cancel()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Expose Prometheus metrics via HTTP
	http.Handle("/metrics", promhttp.Handler())
	logrus.WithField("address", *listenAddrFlag+"/metrics").Info("serving Prometheus metrics")
	if err := http.ListenAndServe(*listenAddrFlag, nil); err != nil {
		logrus.WithError(err).Fatal("failed to start HTTP server")
	}
}
