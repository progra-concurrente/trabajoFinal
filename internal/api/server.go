package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"powersight/internal/auth"
	"powersight/internal/cache"
	"powersight/internal/cluster"
	"powersight/internal/realtime"
	"powersight/internal/store"
	"powersight/pkg/ml"
)

const (
	performanceTargetMS = 100.0
	highThresholdKW     = 1.528
	requiredReadings    = 15
	forecastHorizon     = 30
)

type Server struct {
	store                   *store.Store
	cache                   *cache.Cache
	cluster                 *cluster.Coordinator
	auth                    *auth.Service
	hub                     *realtime.Hub
	reportPath, openAPIPath string
	location                *time.Location
	historical              historicalRates
	modelMu                 sync.RWMutex
	activeModel             ml.Model
	training                atomic.Bool
	persistQueue            chan store.ForecastRecord
}

type readingRequest struct {
	ObservedAt          time.Time `json:"observed_at"`
	GlobalActivePower   float64   `json:"global_active_power"`
	GlobalReactivePower float64   `json:"global_reactive_power"`
	Voltage             float64   `json:"voltage"`
	GlobalIntensity     float64   `json:"global_intensity"`
	SubMetering1        float64   `json:"sub_metering_1"`
	SubMetering2        float64   `json:"sub_metering_2"`
	SubMetering3        float64   `json:"sub_metering_3"`
}

type forecastRequest struct {
	Readings []readingRequest `json:"readings"`
}
type cachedInference struct {
	Probability float64 `json:"probability"`
	Class       int     `json:"class"`
	NodeID      string  `json:"node_id"`
}
type historicalRates struct {
	Hour    map[int]float64
	DayHour map[string]float64
}
type reportAggregate struct {
	Group               string  `json:"group"`
	PrimaryGroup        string  `json:"primary_group"`
	SecondaryGroup      string  `json:"secondary_group"`
	HighConsumptionRate float64 `json:"high_consumption_rate"`
}
type reportDocument struct {
	Hourly  []reportAggregate `json:"hourly_patterns"`
	DayHour []reportAggregate `json:"day_hour_patterns"`
}

func New(db *store.Store, redis *cache.Cache, coordinator *cluster.Coordinator, authService *auth.Service,
	hub *realtime.Hub, model ml.Model, reportPath, openAPIPath, timezone string, _ int) *Server {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.FixedZone("America/Lima", -5*60*60)
	}
	server := &Server{
		store: db, cache: redis, cluster: coordinator, auth: authService, hub: hub,
		activeModel: model, reportPath: reportPath, openAPIPath: openAPIPath, location: location,
		persistQueue: make(chan store.ForecastRecord, 2048),
	}
	server.historical = loadHistoricalRates(reportPath)
	go server.persistenceWorker()
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /api/v1/auth/login", s.login)
	mux.HandleFunc("GET /openapi.yaml", s.openAPI)
	mux.HandleFunc("GET /swagger/", s.swagger)
	protected := http.NewServeMux()
	protected.HandleFunc("POST /api/v1/forecasts", s.createForecast)
	protected.HandleFunc("GET /api/v1/forecasts", s.listForecasts)
	protected.HandleFunc("GET /api/v1/forecasts/{id}", s.getForecast)
	protected.HandleFunc("POST /api/v1/trainings", s.createTraining)
	protected.HandleFunc("GET /api/v1/trainings", s.listTrainings)
	protected.HandleFunc("GET /api/v1/trainings/{id}", s.getTraining)
	protected.HandleFunc("GET /api/v1/models/active", s.getActiveModel)
	protected.HandleFunc("GET /api/v1/cluster/nodes", s.getNodes)
	protected.HandleFunc("GET /api/v1/reports/sustainability", s.getSustainabilityReport)
	mux.Handle("/api/v1/", s.auth.Middleware(protected))
	mux.HandleFunc("GET /ws", s.websocket)
	return recoveryMiddleware(loggingMiddleware(corsMiddleware(mux)))
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	token, err := s.auth.Login(request.Username, request.Password)
	if err != nil {
		writeError(w, 401, err)
		return
	}
	writeJSON(w, 200, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in_seconds": 28800})
}

func (s *Server) createForecast(w http.ResponseWriter, r *http.Request) {
	var request forecastRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	record, err := s.forecast(r.Context(), request)
	if err != nil {
		writeError(w, 400, err)
		return
	}
	writeJSON(w, 200, record)
}

func (s *Server) forecast(ctx context.Context, request forecastRequest) (store.ForecastRecord, error) {
	started := time.Now()
	features, latest, current, contextInfo, err := s.buildForecastFeatures(request)
	if err != nil {
		return store.ForecastRecord{}, err
	}
	s.modelMu.RLock()
	model := s.activeModel
	s.modelMu.RUnlock()
	key := forecastCacheKey(model.Version, features)
	var inference cachedInference
	cached, cacheErr := s.cache.GetJSON(ctx, key, &inference)
	if cacheErr != nil {
		log.Printf("redis forecast read: %v", cacheErr)
	}
	clusterLatency := 0.0
	if !cached {
		result, err := s.cluster.Predict(ctx, model, features)
		if err != nil {
			return store.ForecastRecord{}, err
		}
		inference = cachedInference{result.Probability, result.Class, result.NodeID}
		clusterLatency = result.LatencyMS
		_ = s.cache.SetJSON(context.Background(), key, inference, 5*time.Minute)
	}
	risk := riskLevel(inference.Probability, model.DecisionThreshold)
	recommendation := recommendationFor(current, risk, features.RecentActivePowerTrend, contextInfo)
	elapsed := float64(time.Since(started)) / float64(time.Millisecond)
	record := store.ForecastRecord{
		ID: newRequestID(), UserID: auth.User(ctx), ObservedAt: latest.UTC(), ObservedAtLocal: latest.Format(time.RFC3339),
		CurrentStatus: current, Features: features, HorizonMinutes: forecastHorizon,
		ExpectedWindowStart: latest.Add(time.Minute).UTC(), ExpectedWindowEnd: latest.Add(forecastHorizon * time.Minute).UTC(),
		Probability: inference.Probability, Class: inference.Class, RiskLevel: risk, Context: contextInfo,
		Recommendation: recommendation, NodeID: inference.NodeID, ModelID: model.ID, ModelVersion: model.Version,
		ClusterLatencyMS: clusterLatency, ProcessingTimeMS: elapsed, PerformanceTargetMS: performanceTargetMS,
		TargetMet: elapsed < performanceTargetMS, Cached: cached, CreatedAt: time.Now().UTC(),
	}
	s.enqueuePersistence(record)
	go s.hub.Broadcast("forecast.created", record)
	if risk == "alto" {
		go s.hub.Broadcast("forecast.alert", record)
	}
	return record, nil
}

func (s *Server) buildForecastFeatures(request forecastRequest) (ml.ForecastFeatures, time.Time, store.CurrentStatus, store.TimeContext, error) {
	if len(request.Readings) != requiredReadings {
		return ml.ForecastFeatures{}, time.Time{}, store.CurrentStatus{}, store.TimeContext{}, fmt.Errorf("readings must contain exactly %d consecutive one-minute readings", requiredReadings)
	}
	readings := request.Readings
	for index := range readings {
		if readings[index].ObservedAt.IsZero() {
			return ml.ForecastFeatures{}, time.Time{}, store.CurrentStatus{}, store.TimeContext{}, errors.New("every reading requires observed_at")
		}
		if readings[index].GlobalActivePower < 0 || readings[index].GlobalReactivePower < 0 || readings[index].Voltage <= 0 || readings[index].GlobalIntensity < 0 ||
			readings[index].SubMetering1 < 0 || readings[index].SubMetering2 < 0 || readings[index].SubMetering3 < 0 {
			return ml.ForecastFeatures{}, time.Time{}, store.CurrentStatus{}, store.TimeContext{}, errors.New("readings contain invalid electrical values")
		}
		if index > 0 && readings[index].ObservedAt.Sub(readings[index-1].ObservedAt) != time.Minute {
			return ml.ForecastFeatures{}, time.Time{}, store.CurrentStatus{}, store.TimeContext{}, errors.New("readings must be ordered and exactly one minute apart")
		}
	}
	var sum, maxValue, variance float64
	maxValue = readings[0].GlobalActivePower
	for _, reading := range readings {
		sum += reading.GlobalActivePower
		if reading.GlobalActivePower > maxValue {
			maxValue = reading.GlobalActivePower
		}
	}
	average := sum / requiredReadings
	for _, reading := range readings {
		delta := reading.GlobalActivePower - average
		variance += delta * delta
	}
	latestReading := readings[len(readings)-1]
	latest := latestReading.ObservedAt.In(s.location)
	total := latestReading.SubMetering1 + latestReading.SubMetering2 + latestReading.SubMetering3
	other := latestReading.GlobalActivePower*1000/60 - total
	hourRate := s.historical.Hour[latest.Hour()]
	dayHourRate := s.historical.DayHour[dayHourKey(latest)]
	features := ml.ForecastFeatures{
		CurrentActivePower: latestReading.GlobalActivePower, RecentAverageActivePower: average,
		RecentMaximumActivePower: maxValue, RecentStdDevActivePower: math.Sqrt(variance / requiredReadings),
		RecentActivePowerTrend: (latestReading.GlobalActivePower - readings[0].GlobalActivePower) / (requiredReadings - 1),
		CurrentReactivePower:   latestReading.GlobalReactivePower, CurrentVoltage: latestReading.Voltage,
		CurrentIntensity: latestReading.GlobalIntensity, CurrentSubMeteringTotal: total, CurrentOtherConsumption: other,
		Hour: latest.Hour(), DayOfWeek: int(latest.Weekday()), Month: int(latest.Month()),
		HistoricalHourHighRate: hourRate, HistoricalDayHourHighRate: dayHourRate,
	}
	if err := ml.ValidateFeatures(features); err != nil {
		return ml.ForecastFeatures{}, time.Time{}, store.CurrentStatus{}, store.TimeContext{}, err
	}
	level := "normal"
	if latestReading.GlobalActivePower >= highThresholdKW {
		level = "alto"
	} else if latestReading.GlobalActivePower >= highThresholdKW*.75 {
		level = "elevado"
	}
	current := store.CurrentStatus{ActivePowerKW: latestReading.GlobalActivePower, ThresholdKW: highThresholdKW, Level: level, CurrentlyHigh: latestReading.GlobalActivePower >= highThresholdKW, UnmeteredEnergyWh: other}
	contextInfo := temporalContext(latest, hourRate, dayHourRate)
	return features, latest, current, contextInfo, nil
}

func temporalContext(value time.Time, hourRate, dayHourRate float64) store.TimeContext {
	timeBand := "madrugada"
	if value.Hour() >= 6 && value.Hour() < 12 {
		timeBand = "mañana"
	} else if value.Hour() >= 12 && value.Hour() < 18 {
		timeBand = "tarde"
	} else if value.Hour() >= 18 {
		timeBand = "noche"
	}
	days := []string{"domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"}
	months := []string{"", "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"}
	return store.TimeContext{TimeBand: timeBand, DayName: days[value.Weekday()], MonthName: months[value.Month()], Timezone: value.Location().String(),
		IsPeak: dayHourRate >= .45 || hourRate >= .45, HourHighRate: hourRate, DayHourHighRate: dayHourRate}
}

func riskLevel(probability, threshold float64) string {
	if threshold <= 0 {
		threshold = .5
	}
	if probability >= threshold {
		return "alto"
	}
	if probability >= .35 {
		return "moderado"
	}
	return "bajo"
}

func recommendationFor(current store.CurrentStatus, risk string, trend float64, context store.TimeContext) store.Recommendation {
	if !current.CurrentlyHigh && risk == "alto" {
		return store.Recommendation{Title: "Prevén un pico durante los próximos 30 minutos", Message: "El consumo aún no supera el umbral, pero la tendencia reciente y el contexto indican riesgo alto.", Actions: []string{"No inicies otra carga intensiva durante los próximos 30 minutos.", "Escalona lavadora, secadora o terma.", "Activa una nueva revisión en 5 minutos."}}
	}
	if current.CurrentlyHigh {
		return store.Recommendation{Title: "Reduce el consumo actual y evita que se prolongue", Message: "El hogar ya supera el umbral y el pronóstico estima el riesgo de que continúe durante los próximos 30 minutos.", Actions: []string{"Apaga o posterga una carga no esencial.", "Evita encender otro equipo intensivo.", "Revisa el consumo no identificado."}}
	}
	if risk == "moderado" || trend > 0 || context.IsPeak {
		return store.Recommendation{Title: "Vigila la tendencia antes de añadir nuevas cargas", Message: "Existe una señal moderada de crecimiento o un riesgo histórico relevante.", Actions: []string{"Escalona electrodomésticos intensivos.", "Repite el pronóstico cuando lleguen nuevas lecturas."}}
	}
	return store.Recommendation{Title: "Condición favorable para mantener hábitos eficientes", Message: "El consumo actual y el pronóstico futuro presentan riesgo bajo.", Actions: []string{"Mantén las cargas intensivas separadas.", "Conserva este horario cuando sea posible."}}
}

func loadHistoricalRates(path string) historicalRates {
	rates := historicalRates{Hour: map[int]float64{}, DayHour: map[string]float64{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return rates
	}
	var report reportDocument
	if json.Unmarshal(data, &report) != nil {
		return rates
	}
	for _, item := range report.Hourly {
		var hour int
		if _, err := fmt.Sscanf(item.Group, "%d:00", &hour); err == nil {
			rates.Hour[hour] = item.HighConsumptionRate
		}
	}
	dayIndexes := map[string]int{"Sunday": 0, "Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6}
	for _, item := range report.DayHour {
		var hour int
		if _, err := fmt.Sscanf(item.SecondaryGroup, "%d:00", &hour); err == nil {
			rates.DayHour[fmt.Sprintf("%d-%d", dayIndexes[item.PrimaryGroup], hour)] = item.HighConsumptionRate
		}
	}
	return rates
}
func dayHourKey(value time.Time) string {
	return fmt.Sprintf("%d-%d", int(value.Weekday()), value.Hour())
}
func forecastCacheKey(version string, features ml.ForecastFeatures) string {
	data, _ := json.Marshal(struct {
		Version  string
		Features ml.ForecastFeatures
	}{version, features})
	sum := sha256.Sum256(data)
	return "forecast:" + hex.EncodeToString(sum[:])
}
func (s *Server) enqueuePersistence(record store.ForecastRecord) {
	select {
	case s.persistQueue <- record:
	default:
		go func() { _ = s.store.SaveForecast(context.Background(), record) }()
	}
}
func (s *Server) persistenceWorker() {
	for record := range s.persistQueue {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.store.SaveForecast(ctx, record); err != nil {
			log.Printf("persist forecast: %v", err)
		}
		cancel()
	}
}

func (s *Server) listForecasts(w http.ResponseWriter, r *http.Request) {
	limit, offset := pagination(r, 50, 200)
	var class *int
	if value := r.URL.Query().Get("class"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || (parsed != 0 && parsed != 1) {
			writeError(w, 400, errors.New("class must be 0 or 1"))
			return
		}
		class = &parsed
	}
	from, err := optionalTime(r.URL.Query().Get("from"))
	if err != nil {
		writeError(w, 400, err)
		return
	}
	to, err := optionalTime(r.URL.Query().Get("to"))
	if err != nil {
		writeError(w, 400, err)
		return
	}
	records, err := s.store.ListForecasts(r.Context(), auth.User(r.Context()), limit, offset, class, from, to)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": records, "limit": limit, "offset": offset})
}
func (s *Server) getForecast(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.Forecast(r.Context(), r.PathValue("id"), auth.User(r.Context()))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, 404, err)
		return
	}
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, 200, record)
}

func (s *Server) createTraining(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Epochs       int     `json:"epochs"`
		LearningRate float64 `json:"learning_rate"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.Epochs == 0 {
		request.Epochs = 80
	}
	if request.LearningRate == 0 {
		request.LearningRate = .25
	}
	if request.Epochs < 1 || request.Epochs > 500 || request.LearningRate <= 0 || request.LearningRate > 10 {
		writeError(w, 400, errors.New("invalid training parameters"))
		return
	}
	if !s.training.CompareAndSwap(false, true) {
		writeError(w, 409, errors.New("another training is running"))
		return
	}
	run, err := s.store.CreateTraining(r.Context(), request.Epochs, request.LearningRate, s.cluster.HealthyCount())
	if err != nil {
		s.training.Store(false)
		writeError(w, 500, err)
		return
	}
	go s.runTraining(run)
	writeJSON(w, 202, run)
}
func (s *Server) runTraining(run store.TrainingRun) {
	defer s.training.Store(false)
	ctx := context.Background()
	if err := s.store.StartTraining(ctx, run.ID, s.cluster.HealthyCount()); err != nil {
		_ = s.store.FailTraining(ctx, run.ID, err)
		return
	}
	s.hub.Broadcast("training.started", run)
	s.modelMu.RLock()
	initial := s.activeModel
	s.modelMu.RUnlock()
	model, err := s.cluster.Train(ctx, initial, cluster.TrainOptions{Epochs: run.Epochs, LearningRate: run.LearningRate}, func(metric cluster.EpochMetric) {
		_ = s.store.SaveEpoch(ctx, run.ID, metric)
		s.hub.Broadcast("training.epoch", map[string]any{"training_id": run.ID, "metric": metric})
	})
	if err != nil {
		_ = s.store.FailTraining(ctx, run.ID, err)
		s.hub.Broadcast("training.failed", map[string]string{"id": run.ID, "error": err.Error()})
		return
	}
	model.HistoryMinutes = requiredReadings
	model.HorizonMinutes = forecastHorizon
	model.SustainedMinutes = 10
	model.Metrics, _ = json.Marshal(map[string]any{"training_run_id": run.ID})
	model, err = s.store.CompleteTraining(ctx, run.ID, model)
	if err != nil {
		_ = s.store.FailTraining(ctx, run.ID, err)
		return
	}
	s.modelMu.Lock()
	s.activeModel = model
	s.modelMu.Unlock()
	s.hub.Broadcast("training.completed", map[string]any{"id": run.ID, "model": model})
}
func (s *Server) listTrainings(w http.ResponseWriter, r *http.Request) {
	limit, offset := pagination(r, 50, 200)
	runs, err := s.store.ListTrainings(r.Context(), limit, offset)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": runs, "limit": limit, "offset": offset})
}
func (s *Server) getTraining(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.Training(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, 404, err)
		return
	}
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, 200, run)
}
func (s *Server) getActiveModel(w http.ResponseWriter, _ *http.Request) {
	s.modelMu.RLock()
	defer s.modelMu.RUnlock()
	writeJSON(w, 200, s.activeModel)
}
func (s *Server) getNodes(w http.ResponseWriter, r *http.Request) {
	nodes := s.cluster.Nodes()
	_ = s.cache.SetJSON(r.Context(), "cluster:nodes", nodes, 15*time.Second)
	writeJSON(w, 200, map[string]any{"items": nodes})
}
func (s *Server) getSustainabilityReport(w http.ResponseWriter, r *http.Request) {
	var report any
	if found, _ := s.cache.GetJSON(r.Context(), "report:sustainability:v2", &report); found {
		writeJSON(w, 200, report)
		return
	}
	data, err := os.ReadFile(s.reportPath)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	if json.Unmarshal(data, &report) != nil {
		writeError(w, 500, errors.New("invalid report"))
		return
	}
	_ = s.cache.SetJSON(r.Context(), "report:sustainability:v2", report, 24*time.Hour)
	writeJSON(w, 200, report)
}
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	mongoStatus, redisStatus, clusterStatus := "up", "up", "up"
	if s.store.Ping(r.Context()) != nil {
		mongoStatus = "down"
	}
	if s.cache.Ping(r.Context()) != nil {
		redisStatus = "down"
	}
	if s.cluster.HealthyCount() == 0 {
		clusterStatus = "down"
	}
	status := 200
	if mongoStatus == "down" || redisStatus == "down" || clusterStatus == "down" {
		status = 503
	}
	writeJSON(w, status, map[string]any{"status": map[bool]string{true: "ok", false: "degraded"}[status == 200], "mongodb": mongoStatus, "redis": redisStatus, "cluster": clusterStatus, "healthy_nodes": s.cluster.HealthyCount()})
}
func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
	username, err := s.auth.Parse(r.URL.Query().Get("token"))
	if err != nil || username == "" {
		writeError(w, 401, errors.New("valid token required"))
		return
	}
	s.hub.ServeHTTP(w, r)
}
func (s *Server) openAPI(w http.ResponseWriter, _ *http.Request) {
	data, err := os.ReadFile(s.openAPIPath)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(data)
}
func (s *Server) swagger(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html><html><head><title>PowerSight API</title><link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:"/openapi.yaml",dom_id:"#swagger-ui",persistAuthorization:true})</script></body></html>`))
}
func newRequestID() string { return fmt.Sprintf("forecast-%d", time.Now().UnixNano()) }
func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, 400, err)
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
func pagination(r *http.Request, defaultLimit, maxLimit int) (int, int) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
func optionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, errors.New("date filters must use RFC3339")
	}
	return &parsed, nil
}
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("panic: %v", recovered)
				writeError(w, 500, errors.New("internal server error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
