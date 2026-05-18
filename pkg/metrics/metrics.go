package metrics

import (
	"time"

	"github.com/k3s-io/kine/pkg/query"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	ResultSuccess = "success"
	ResultError   = "error"
)

var (
	SQLTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_sql_total",
		Help: "Total number of SQL operations",
	}, []string{"name", "error_code"})

	SQLTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kine_sql_time_seconds",
		Help: "Length of time per SQL operation",
		// SQL request latency in seconds for each named query and error code.
		// Keep consistent with apiserver metric 'requestLatencies' in
		// staging/src/k8s.io/apiserver/pkg/endpoints/metrics/metrics.go
		Buckets: []float64{0.005, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3, 4, 5, 6, 8, 10, 15, 20, 30, 45, 60},
	}, []string{"name", "error_code"})

	CompactTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_compact_total",
		Help: "Total number of compactions",
	}, []string{"result"})

	InsertErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_insert_errors_total",
		Help: "Total number of insert retries due to unique constraint violations",
	}, []string{"retriable"})
)

var (
	// SlowSQLThreshold is a duration which SQL executed longer than will be logged.
	// This can be directly modified to override the default value when kine is used as a library.
	SlowSQLThreshold        = time.Second
	SlowSQLWarningThreshold = 5 * time.Second
)

func ObserveSQL(start time.Time, errCode string, retries int, sql *query.Filled) {
	if sql.Name != "" {
		SQLTotal.WithLabelValues(sql.Name, errCode).Inc()
		duration := time.Since(start)
		SQLTime.WithLabelValues(sql.Name, errCode).Observe(duration.Seconds())
		if SlowSQLThreshold > 0 && duration >= SlowSQLThreshold {
			instrumentedLogger := logrus.WithFields(logrus.Fields{
				"name":     sql.Name,
				"duration": duration.String(),
				"started":  start.Format(time.RFC3339Nano),
			})
			if retries > 0 {
				instrumentedLogger = instrumentedLogger.WithField("retries", retries)
			}
			if duration < SlowSQLWarningThreshold {
				instrumentedLogger.Infof("Slow SQL: %s", sql.QueryString())
			} else {
				instrumentedLogger.Warnf("Slow SQL: %s", sql.QueryString())
			}
		}
	}
}
