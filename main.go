package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var url = `https://air-quality-api.open-meteo.com/v1/air-quality?latitude=%f&longitude=%f&current=european_aqi,us_aqi,pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,aerosol_optical_depth,dust,ammonia,alder_pollen,birch_pollen,grass_pollen,mugwort_pollen,olive_pollen,ragweed_pollen&timezone=America%%2FLos_Angeles`

func main() {
	var pollInterval time.Duration
	var latitude float64
	var longitude float64
	var addr string
	flag.DurationVar(&pollInterval, "poll-interval", time.Minute, "poll frequency")
	flag.Float64Var(&latitude, "latitude", 0, "latitude")
	flag.Float64Var(&longitude, "longitude", 0, "longitude")
	flag.StringVar(&addr, "addr", "0.0.0.0:9771", "server addr")
	flag.Parse()

	if latitude == 0 && longitude == 0 {
		log.Fatalf("Please specify latitude and longitude")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go collect(ctx, pollInterval, latitude, longitude)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}

func collect(ctx context.Context, pollInterval time.Duration, latitude, longitude float64) {
	fullUrl := fmt.Sprintf(url, latitude, longitude)
	log.Printf("full url: %s", fullUrl)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullUrl, strings.NewReader(""))
		if err != nil {
			log.Fatalf("error constructing request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("error in http request: %v", err)
		}
		var response response
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(&response); err != nil {
			log.Fatalf("error decoding response body: %v", err)
		}

		metrics.elevation.Set(response.Elevation)

		//metrics.current.timeSinceUpdate.Set(time.Since(response.Current.Time).Seconds())
		metrics.current.europeanAQI.Set(response.Current.EuropeanAQI)
		metrics.current.usAQI.Set(response.Current.USAQI)
		metrics.current.pm10.Set(response.Current.PM10)
		metrics.current.pm25.Set(response.Current.PM25)
		metrics.current.carbonMonoxide.Set(response.Current.CarbonMonoxide)
		metrics.current.nitrogenDioxide.Set(response.Current.NitrogenDioxide)
		metrics.current.sulfurDioxide.Set(response.Current.SulfurDioxide)
		metrics.current.ozone.Set(response.Current.Ozone)
		metrics.current.aerosolOpticalDepth.Set(response.Current.AerosolOpticalDepth)
		metrics.current.dust.Set(response.Current.Dust)
		metrics.current.ammonia.Set(response.Current.Ammonia)
		metrics.current.alderPollen.Set(response.Current.AlderPollen)
		metrics.current.birchPollen.Set(response.Current.BirchPollen)
		metrics.current.grassPollen.Set(response.Current.GrassPollen)
		metrics.current.mugwortPollen.Set(response.Current.MugwortPollen)
		metrics.current.olivePollen.Set(response.Current.OlivePollen)
		metrics.current.ragweedPollen.Set(response.Current.RagweedPollen)

		select {
		case <-ctx.Done():
			log.Printf("%v", ctx.Err())
			return
		case <-time.After(pollInterval):
		}
	}
}

type response struct {
	Elevation float64
	Current   struct {
		//Time                time.Time `json:"time"`
		EuropeanAQI         float64 `json:"european_aqi"`
		USAQI               float64 `json:"us_aqi"`
		PM10                float64 `json:"pm10"`
		PM25                float64 `json:"pm2_5"`
		CarbonMonoxide      float64 `json:"carbon_monoxide"`
		NitrogenDioxide     float64 `json:"nitrogen_dioxide"`
		SulfurDioxide       float64 `json:"sulphur_dioxiode"`
		Ozone               float64 `json:"ozone"`
		AerosolOpticalDepth float64 `json:"aerosol_optical_depth"`
		Dust                float64 `json:"dust"`
		Ammonia             float64 `json:"ammonia"`
		AlderPollen         float64 `json:"alder_pollen"`
		BirchPollen         float64 `json:"birch_pollen"`
		GrassPollen         float64 `json:"grass_pollen"`
		MugwortPollen       float64 `json:"mugwort_pollen"`
		OlivePollen         float64 `json:"olive_pollen"`
		RagweedPollen       float64 `json:"ragweed_pollen"`
	} `json:"current"`
}

type currentMetrics struct {
	timeSinceUpdate     prometheus.Gauge
	europeanAQI         prometheus.Gauge
	usAQI               prometheus.Gauge
	pm10                prometheus.Gauge
	pm25                prometheus.Gauge
	carbonMonoxide      prometheus.Gauge
	nitrogenDioxide     prometheus.Gauge
	sulfurDioxide       prometheus.Gauge
	ozone               prometheus.Gauge
	aerosolOpticalDepth prometheus.Gauge
	dust                prometheus.Gauge
	ammonia             prometheus.Gauge
	alderPollen         prometheus.Gauge
	birchPollen         prometheus.Gauge
	grassPollen         prometheus.Gauge
	mugwortPollen       prometheus.Gauge
	olivePollen         prometheus.Gauge
	ragweedPollen       prometheus.Gauge
}

var metrics = struct {
	elevation prometheus.Gauge
	current   currentMetrics
}{
	elevation: prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "openmeteo",
		Name:      "elevation",
	}),
	current: currentMetrics{
		timeSinceUpdate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo",
			Name:      "current_time_since_last_update",
		}),
		europeanAQI: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_european_aqi",
		}),
		usAQI: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_us_aqi",
		}),
		pm10: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_pm10",
		}),
		pm25: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_pm2_5",
		}),
		carbonMonoxide: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_carbon_monoxide",
		}),
		nitrogenDioxide: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_nitrogen_dioxide",
		}),
		sulfurDioxide: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_sulphur_dioxiode",
		}),
		ozone: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_ozone",
		}),
		aerosolOpticalDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_aerosol_optical_depth",
		}),
		dust: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_dust",
		}),
		ammonia: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_ammonia",
		}),
		alderPollen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_alder_pollen",
		}),
		birchPollen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_birch_pollen",
		}),
		grassPollen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_grass_pollen",
		}),
		mugwortPollen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_mugwort_pollen",
		}),
		olivePollen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_olive_pollen",
		}),
		ragweedPollen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "openmeteo_airquality",
			Name:      "current_ragweed_pollen",
		}),
	},
}

func init() {
	prometheus.Register(metrics.elevation)
	prometheus.Register(metrics.current.timeSinceUpdate)
	prometheus.Register(metrics.current.europeanAQI)
	prometheus.Register(metrics.current.usAQI)
	prometheus.Register(metrics.current.pm10)
	prometheus.Register(metrics.current.pm25)
	prometheus.Register(metrics.current.carbonMonoxide)
	prometheus.Register(metrics.current.nitrogenDioxide)
	prometheus.Register(metrics.current.sulfurDioxide)
	prometheus.Register(metrics.current.ozone)
	prometheus.Register(metrics.current.aerosolOpticalDepth)
	prometheus.Register(metrics.current.dust)
	prometheus.Register(metrics.current.ammonia)
	prometheus.Register(metrics.current.alderPollen)
	prometheus.Register(metrics.current.birchPollen)
	prometheus.Register(metrics.current.grassPollen)
	prometheus.Register(metrics.current.mugwortPollen)
	prometheus.Register(metrics.current.olivePollen)
	prometheus.Register(metrics.current.ragweedPollen)
}
