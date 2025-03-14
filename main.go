package main

import (
	"context"
	"log"
	"net/http"
	"time"

	_ "github.com/rclone/rclone/backend/b2" // Import desired backends
	_ "github.com/rclone/rclone/backend/s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

// updateRemoteBuckets lists the top-level directories (buckets) in the given remote using operations.ListDir(),
// then for each bucket, it calls operations.Count() to get the file count and total size.
func updateRemoteBuckets(ctx context.Context, remote string) {
	// Create a new Fs for the remote.
	f, err := fs.NewFs(remote)
	if err != nil {
		log.Printf("Error creating Fs for remote %q: %v", remote, err)
		return
	}

	// List top-level directories (buckets). The empty string ("") lists the root.
	dirs, err := operations.ListDir(ctx, f, "")
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
		bucketFs, err := fs.NewFs(bucketRemote)
		if err != nil {
			log.Printf("Error creating Fs for bucket %q: %v", bucketRemote, err)
			continue
		}

		// operations.Count returns file count, directory count, and total size in bytes.
		// We ignore the directory count.
		files, _, size, err := operations.Count(ctx, bucketFs, nil)
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

func main() {
	// List of remotes to monitor. Adjust these as needed.
	remotes := []string{"b2:", "s3:"}
	ctx := context.Background()

	// Start a goroutine to periodically update bucket metrics.
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		// Run an update immediately.
		for _, remote := range remotes {
			updateRemoteBuckets(ctx, remote)
		}
		// Update periodically.
		for {
			select {
			case <-ticker.C:
				for _, remote := range remotes {
					updateRemoteBuckets(ctx, remote)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Expose Prometheus metrics via HTTP.
	http.Handle("/metrics", promhttp.Handler())
	log.Println("Serving Prometheus metrics on :8080/metrics")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}
