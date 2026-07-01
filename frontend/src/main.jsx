import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import "bootstrap/dist/css/bootstrap.min.css";
import {
  FiActivity,
  FiAlertTriangle,
  FiBarChart2,
  FiClock,
  FiCpu,
  FiDatabase,
  FiLogOut,
  FiPlay,
  FiRefreshCw,
  FiSettings,
  FiShield,
  FiSquare,
  FiUser,
  FiZap,
} from "react-icons/fi";
import { api, wsUrl } from "./api";
import { buildWindow, initialIndex, makeReading, scenarioDescription, scenarioLabels } from "./simulator";
import "./styles.css";

const tabs = [
  { id: "home", label: "Inicio", icon: FiZap },
  { id: "predict", label: "Predicción", icon: FiActivity },
  { id: "visual", label: "Visualización", icon: FiBarChart2 },
  { id: "history", label: "Historial", icon: FiClock },
  { id: "settings", label: "Configuración", icon: FiSettings },
];

const intervalOptions = [
  { value: 5000, label: "5 s" },
  { value: 15000, label: "15 s" },
  { value: 60000, label: "60 s" },
];

function App() {
  const [session, setSession] = useState(() => {
    const stored = localStorage.getItem("powersight-session");
    return stored ? JSON.parse(stored) : null;
  });
  const [activeTab, setActiveTab] = useState("home");
  const [scenario, setScenario] = useState("incoming");
  const [intervalMs, setIntervalMs] = useState(5000);
  const [readings, setReadings] = useState([]);
  const [forecast, setForecast] = useState(null);
  const [history, setHistory] = useState([]);
  const [report, setReport] = useState(null);
  const [adminMetrics, setAdminMetrics] = useState(null);
  const [trainings, setTrainings] = useState([]);
  const [events, setEvents] = useState([]);
  const [running, setRunning] = useState(false);
  const [loading, setLoading] = useState(false);
  const [manualOutput, setManualOutput] = useState(null);
  const [error, setError] = useState("");
  const indexRef = useRef(0);

  const isAdmin = session?.user?.role === "admin";
  const navigation = isAdmin ? [...tabs, { id: "admin", label: "Admin", icon: FiShield }] : tabs;

  useEffect(() => {
    if (!session) return;
    localStorage.setItem("powersight-session", JSON.stringify(session));
    refreshData(session);
    const socket = new WebSocket(wsUrl(session.access_token));
    socket.onmessage = (event) => {
      const message = JSON.parse(event.data);
      setEvents((current) => [message, ...current].slice(0, 8));
      if (message.type === "forecast.created" || message.type === "forecast.alert") {
        setForecast(message.payload);
        setHistory((current) => mergeHistory(message.payload, current));
      }
      if (message.type?.startsWith("training.")) {
        refreshAdmin(session);
      }
    };
    socket.onerror = () => setEvents((current) => [{ type: "ws.error", at: new Date().toISOString() }, ...current].slice(0, 8));
    return () => socket.close();
  }, [session?.access_token]);

  useEffect(() => {
    if (!running || !session) return;
    const timer = setInterval(() => {
      advanceSimulation();
    }, intervalMs);
    return () => clearInterval(timer);
  }, [running, intervalMs, scenario, session, readings]);

  async function refreshData(activeSession = session) {
    if (!activeSession) return;
    setError("");
    try {
      const [forecastList, sustainability] = await Promise.all([
        api.forecasts(activeSession.access_token),
        api.sustainability(activeSession.access_token),
      ]);
      setHistory(forecastList.items || []);
      setReport(sustainability);
      if (activeSession.user?.role === "admin") {
        await refreshAdmin(activeSession);
      }
    } catch (err) {
      setError(err.message);
    }
  }

  async function refreshAdmin(activeSession = session) {
    if (!activeSession?.access_token || activeSession.user?.role !== "admin") return;
    try {
      const [metrics, trainingList] = await Promise.all([
        api.adminMetrics(activeSession.access_token),
        api.trainings(activeSession.access_token),
      ]);
      setAdminMetrics(metrics);
      setTrainings(trainingList.items || []);
    } catch (err) {
      setEvents((current) => [{ type: "admin.error", payload: err.message, at: new Date().toISOString() }, ...current].slice(0, 8));
    }
  }

  async function signIn(payload) {
    setSession(payload);
    setActiveTab("home");
  }

  function signOut() {
    localStorage.removeItem("powersight-session");
    setSession(null);
    setForecast(null);
    setHistory([]);
    setEvents([]);
    setRunning(false);
  }

  async function runForecast(windowReadings) {
    if (!session || windowReadings.length !== 15) return;
    setLoading(true);
    setError("");
    try {
      const result = await api.forecast(windowReadings, session.access_token);
      setForecast(result);
      setHistory((current) => mergeHistory(result, current));
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function startSimulation() {
    const startIndex = initialIndex(session?.user?.username) + 18;
    indexRef.current = startIndex;
    const windowReadings = buildWindow(scenario, session?.user?.username, startIndex);
    setReadings(windowReadings);
    setRunning(true);
    await runForecast(windowReadings);
  }

  async function advanceSimulation() {
    indexRef.current += 1;
    const nextReading = makeReading(scenario, session?.user?.username, indexRef.current);
    const nextWindow = [...readings.slice(-14), nextReading];
    setReadings(nextWindow);
    await runForecast(nextWindow);
  }

  async function startTraining() {
    if (!session) return;
    setLoading(true);
    setError("");
    try {
      await api.startTraining({ epochs: 30, learning_rate: 0.25 }, session.access_token);
      await refreshAdmin(session);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function runManualPrediction(manualReadings) {
    if (!session) return;
    setLoading(true);
    setError("");
    try {
      const result = await api.forecast(manualReadings, session.access_token);
      setManualOutput(result);
      setForecast(result);
      setHistory((current) => mergeHistory(result, current));
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  if (!session) {
    return <AuthScreen onAuthenticated={signIn} />;
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark"><FiZap /></span>
          <div>
            <strong>PowerSight</strong>
            <span>Prevención energética</span>
          </div>
        </div>
        <nav>
          {navigation.map((item) => {
            const Icon = item.icon;
            return (
              <button key={item.id} className={activeTab === item.id ? "active" : ""} onClick={() => setActiveTab(item.id)}>
                <Icon /> {item.label}
              </button>
            );
          })}
        </nav>
        <div className="sidebar-user">
          <FiUser />
          <div>
            <strong>{session.user.username}</strong>
            <span>{session.user.role}</span>
          </div>
          <button className="icon-button" onClick={signOut} title="Cerrar sesión"><FiLogOut /></button>
        </div>
      </aside>

      <main className="content">
        <TopBar
          scenario={scenario}
          running={running}
          forecast={forecast}
          onRefresh={() => refreshData()}
          loading={loading}
        />
        {error && <div className="alert alert-danger py-2">{error}</div>}

        {activeTab === "home" && (
          <HomeView
            forecast={forecast}
            readings={readings}
            report={report}
            running={running}
            loading={loading}
            onStart={startSimulation}
            onStop={() => setRunning(false)}
            onStep={advanceSimulation}
          />
        )}
        {activeTab === "predict" && (
          <PredictionView forecast={forecast} readings={readings} events={events} loading={loading} />
        )}
        {activeTab === "visual" && (
          <VisualView readings={readings} history={history} report={report} />
        )}
        {activeTab === "history" && <HistoryView history={history} />}
        {activeTab === "settings" && (
          <SettingsView
            scenario={scenario}
            setScenario={setScenario}
            intervalMs={intervalMs}
            setIntervalMs={setIntervalMs}
            running={running}
            onStart={startSimulation}
            onStop={() => setRunning(false)}
          />
        )}
        {activeTab === "admin" && isAdmin && (
          <AdminView
            metrics={adminMetrics}
            trainings={trainings}
          events={events}
          onRefresh={() => refreshAdmin()}
          onTrain={startTraining}
          onManualPredict={runManualPrediction}
          manualOutput={manualOutput}
          username={session.user.username}
          loading={loading}
        />
      )}
      </main>
    </div>
  );
}

function AuthScreen({ onAuthenticated }) {
  const [mode, setMode] = useState("login");
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("powersight");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(event) {
    event.preventDefault();
    setLoading(true);
    setError("");
    try {
      const result = mode === "login" ? await api.login(username, password) : await api.register(username, password);
      onAuthenticated(result);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="auth-page">
      <section className="auth-panel">
        <div className="brand large">
          <span className="brand-mark"><FiZap /></span>
          <div>
            <strong>PowerSight</strong>
            <span>Sistema preventivo de consumo eléctrico</span>
          </div>
        </div>
        <div className="auth-copy">
          <h1>Decide antes de conectar otra carga</h1>
          <p>Predicción distribuida de riesgo alto para los próximos 30 minutos, con historial y métricas del cluster.</p>
        </div>
        <form onSubmit={submit} className="auth-form">
          <div className="segmented">
            <button type="button" className={mode === "login" ? "selected" : ""} onClick={() => setMode("login")}>Ingresar</button>
            <button type="button" className={mode === "register" ? "selected" : ""} onClick={() => setMode("register")}>Registrar</button>
          </div>
          <label>
            Usuario
            <input className="form-control" value={username} onChange={(event) => setUsername(event.target.value)} minLength={3} required />
          </label>
          <label>
            Contraseña
            <input className="form-control" type="password" value={password} onChange={(event) => setPassword(event.target.value)} minLength={6} required />
          </label>
          {error && <div className="alert alert-danger py-2">{error}</div>}
          <button className="btn btn-primary w-100" disabled={loading}>{loading ? "Validando..." : mode === "login" ? "Entrar" : "Crear usuario"}</button>
        </form>
      </section>
    </main>
  );
}

function TopBar({ scenario, running, forecast, onRefresh, loading }) {
  return (
    <header className="topbar">
      <div>
        <span className="eyebrow">{scenarioLabels[scenario]}</span>
        <h2>{forecast ? riskHeadline(forecast) : "Monitoreo preventivo listo"}</h2>
      </div>
      <div className="topbar-actions">
        <StatusBadge risk={forecast?.risk_level} running={running} />
        <button className="btn btn-outline-secondary btn-sm" onClick={onRefresh} disabled={loading}>
          <FiRefreshCw /> Actualizar
        </button>
      </div>
    </header>
  );
}

function HomeView({ forecast, readings, report, running, loading, onStart, onStop, onStep }) {
  const latest = readings[readings.length - 1];
  const impact = buildImpact(report, forecast);
  return (
    <section className="stack">
      <div className={`decision-band ${forecast?.risk_level || "none"}`}>
        <div>
          <span className="eyebrow">Decisión del hogar</span>
          <h1>{forecast ? decisionText(forecast) : "Inicia una simulación para evaluar el consumo actual"}</h1>
          <p>{forecast?.recommendation?.message || "PowerSight evaluará los últimos 15 minutos y estimará el riesgo de alto consumo sostenido."}</p>
        </div>
        <div className="decision-metrics">
          <Metric label="Potencia actual" value={latest ? `${latest.global_active_power.toFixed(2)} kW` : "--"} />
          <Metric label="Probabilidad" value={forecast ? formatProbability(forecast.probability) : "--"} />
          <Metric label="Tiempo" value={forecast ? `${forecast.processing_time_ms.toFixed(1)} ms` : "--"} />
        </div>
      </div>

      <div className="toolbar-band">
        <button className="btn btn-primary" onClick={onStart} disabled={loading}>
          <FiPlay /> {running ? "Reiniciar demo" : "Iniciar demo"}
        </button>
        <button className="btn btn-outline-secondary" onClick={onStep} disabled={loading || readings.length < 15}>
          <FiRefreshCw /> Nuevo minuto
        </button>
        <button className="btn btn-outline-danger" onClick={onStop} disabled={!running}>
          <FiSquare /> Detener
        </button>
      </div>

      <div className="grid-3">
        <section className="panel">
          <h3>Ventana de 15 minutos</h3>
          <Sparkline data={readings.map((item) => item.global_active_power)} threshold={forecast?.current_status?.threshold_kw || 1.528} />
        </section>
        <section className="panel">
          <h3>Recomendación</h3>
          <p className="strong-copy">{forecast?.recommendation?.title || "Sin predicción activa"}</p>
          <ul className="action-list">
            {(forecast?.recommendation?.actions || ["Ejecuta la simulación para obtener acciones preventivas."]).map((item) => <li key={item}>{item}</li>)}
          </ul>
        </section>
        <section className="panel">
          <h3>Impacto social</h3>
          <p className="muted">
            PowerSight busca ayudar a hogares a postergar cargas intensivas antes de que ocurra un pico sostenido.
            La alerta no promete ahorro automático: entrega una recomendación preventiva basada en 15 minutos recientes,
            patrones históricos y una predicción de los próximos 30 minutos.
          </p>
          <p className="strong-copy">Umbral técnico usado: {impact.threshold} kW.</p>
        </section>
      </div>
    </section>
  );
}

function PredictionView({ forecast, readings, events }) {
  return (
    <section className="stack">
      <div className="grid-4">
        <MetricPanel icon={FiZap} label="Riesgo" value={forecast?.risk_level || "--"} />
        <MetricPanel icon={FiBarChart2} label="Probabilidad 30 min" value={forecast ? formatProbability(forecast.probability) : "--"} />
        <MetricPanel icon={FiCpu} label="Nodo" value={forecast?.node_id || "--"} />
        <MetricPanel icon={FiClock} label="Latencia total" value={forecast ? `${forecast.processing_time_ms.toFixed(1)} ms` : "--"} />
      </div>
      {forecast && (
        <section className="panel wide">
          <h3>Interpretación</h3>
          <p className="muted">
            La probabilidad estima si habrá consumo alto sostenido durante los próximos 30 minutos.
            El umbral de decisión del modelo es {formatProbability(0.3)}; valores redondeados pueden verse iguales aunque uno esté apenas por encima y otro apenas por debajo.
          </p>
        </section>
      )}
      <section className="panel wide">
        <h3>Lecturas enviadas al algoritmo</h3>
        <ReadingsTable readings={readings} />
      </section>
      <section className="panel wide">
        <h3>Eventos en tiempo real</h3>
        <EventList events={events} />
      </section>
    </section>
  );
}

function VisualView({ readings, history, report }) {
  const probabilities = history.slice(0, 12).reverse().map((item) => item.probability);
  const peakHours = report?.peak_hours || report?.hourly_patterns?.slice(0, 5) || [];
  return (
    <section className="stack">
      <div className="grid-2">
        <section className="panel">
          <h3>Consumo reciente</h3>
          <Sparkline data={readings.map((item) => item.global_active_power)} threshold={1.528} />
        </section>
        <section className="panel">
          <h3>Probabilidad histórica de la sesión</h3>
          <Sparkline data={probabilities} max={1} />
        </section>
      </div>
      <section className="panel wide">
        <h3>Horas pico reales del dataset</h3>
        <div className="bars">
          {peakHours.slice(0, 6).map((item) => (
            <div className="bar-row" key={item.group}>
              <span>{item.group}</span>
              <div><i style={{ width: `${Math.round((item.high_consumption_rate || 0) * 100)}%` }} /></div>
              <strong>{Math.round((item.high_consumption_rate || 0) * 100)}%</strong>
            </div>
          ))}
        </div>
      </section>
    </section>
  );
}

function HistoryView({ history }) {
  return (
    <section className="panel wide">
      <h3>Historial de predicciones</h3>
      <div className="table-responsive">
        <table className="table align-middle">
          <thead>
            <tr>
              <th>Fecha</th>
              <th>Riesgo</th>
              <th>Probabilidad</th>
              <th>Umbral</th>
              <th>Actual</th>
              <th>Nodo</th>
              <th>Tiempo</th>
            </tr>
          </thead>
          <tbody>
            {history.map((item) => (
              <tr key={item.id}>
                <td>{formatDate(item.observed_at_local || item.observed_at)}</td>
                <td><RiskPill risk={item.risk_level} /></td>
                <td>{formatProbability(item.probability)}</td>
                <td>{formatProbability(0.3)}</td>
                <td>{item.current_status?.active_power_kw?.toFixed(2)} kW</td>
                <td>{item.node_id}</td>
                <td>{item.processing_time_ms?.toFixed(1)} ms</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SettingsView({ scenario, setScenario, intervalMs, setIntervalMs, running, onStart, onStop }) {
  return (
    <section className="settings-layout">
      <div className="panel">
        <h3>Escenario de consumo</h3>
        <div className="option-grid">
          {Object.keys(scenarioLabels).map((key) => (
            <button key={key} className={scenario === key ? "option selected" : "option"} onClick={() => setScenario(key)}>
              <strong>{scenarioLabels[key]}</strong>
              <span>{scenarioDescription(key)}</span>
            </button>
          ))}
        </div>
      </div>
      <div className="panel">
        <h3>Ritmo de demo</h3>
        <div className="segmented">
          {intervalOptions.map((option) => (
            <button key={option.value} className={intervalMs === option.value ? "selected" : ""} onClick={() => setIntervalMs(option.value)}>
              {option.label}
            </button>
          ))}
        </div>
        <div className="toolbar-band compact">
          <button className="btn btn-primary" onClick={onStart}><FiPlay /> {running ? "Reiniciar" : "Iniciar"}</button>
          <button className="btn btn-outline-danger" onClick={onStop} disabled={!running}><FiSquare /> Detener</button>
        </div>
      </div>
    </section>
  );
}

function AdminView({ metrics, trainings, events, onRefresh, onTrain, onManualPredict, manualOutput, username, loading }) {
  const nodes = metrics?.cluster?.nodes || [];
  return (
    <section className="stack">
      <div className="toolbar-band">
        <button className="btn btn-outline-secondary" onClick={onRefresh}><FiRefreshCw /> Actualizar métricas</button>
        <button className="btn btn-primary" onClick={onTrain} disabled={loading}><FiCpu /> Entrenar modelo demo</button>
      </div>
      <div className="grid-4">
        <MetricPanel icon={FiDatabase} label="Filas modelo" value={formatNumber(metrics?.processing?.rows)} />
        <MetricPanel icon={FiBarChart2} label="Recall prueba" value={formatPercent(metrics?.processing?.recall)} />
        <MetricPanel icon={FiCpu} label="Nodos sanos" value={`${metrics?.health?.healthy_nodes ?? "--"}`} />
        <MetricPanel icon={FiClock} label="Workers dataset" value={`${metrics?.processing?.workers ?? "--"}`} />
      </div>
      <InitialTrainingMetrics metrics={metrics?.etl_initial_training} />
      <section className="panel wide">
        <h3>Nodos del cluster ML</h3>
        <div className="table-responsive">
          <table className="table align-middle">
            <thead>
              <tr>
                <th>Nodo</th>
                <th>Estado</th>
                <th>Capacidad</th>
                <th>CPU estimado</th>
                <th>Jobs</th>
                <th>Errores</th>
                <th>Latencia media</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((node) => (
                <tr key={node.id}>
                  <td>{node.id}</td>
                  <td>{node.healthy ? <span className="text-success">activo</span> : <span className="text-danger">inactivo</span>}</td>
                  <td>{node.capacity}</td>
                  <td>{formatPercent((node.cpu_usage_percent || 0) / 100)}</td>
                  <td>{node.jobs_completed}</td>
                  <td>{node.errors}</td>
                  <td>{node.average_latency_ms?.toFixed(1) || "0.0"} ms</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      <ManualPredictionPanel onPredict={onManualPredict} output={manualOutput} username={username} loading={loading} />
      <div className="grid-2">
        <section className="panel">
          <h3>Entrenamientos</h3>
          <EventList events={trainings.map((run) => ({ type: run.status, at: run.created_at, payload: `${run.current_epoch || 0}/${run.epochs} épocas` }))} />
        </section>
        <section className="panel">
          <h3>Eventos del sistema</h3>
          <EventList events={events} />
        </section>
      </div>
    </section>
  );
}

function InitialTrainingMetrics({ metrics }) {
  if (!metrics) {
    return null;
  }
  if (!metrics.available) {
    return (
      <section className="panel wide">
        <h3>ETL y entrenamiento inicial</h3>
        <p className="muted">{metrics.message}</p>
        <code>{metrics.command}</code>
      </section>
    );
  }
  return (
    <section className="panel wide">
      <div className="panel-heading">
        <h3>ETL y entrenamiento inicial</h3>
        <span className="muted">Generado: {formatDate(metrics.generated_at)}</span>
      </div>
      <div className="initial-metrics">
        <Metric label="Tiempo total" value={formatDuration(metrics.total_duration_ms)} />
        <Metric label="Filas limpias" value={formatNumber(metrics.rows_clean)} />
        <Metric label="Ventanas forecast" value={formatNumber(metrics.forecast_rows)} />
        <Metric label="Workers ETL" value={`${metrics.workers}`} />
      </div>
      <div className="table-responsive mt-3">
        <table className="table table-sm align-middle">
          <thead>
            <tr>
              <th>Etapa</th>
              <th>Duración</th>
            </tr>
          </thead>
          <tbody>
            {(metrics.stages || []).map((stage) => (
              <tr key={stage.name}>
                <td>{stage.name}</td>
                <td>{formatDuration(stage.duration_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ManualPredictionPanel({ onPredict, output, username, loading }) {
  const [open, setOpen] = useState(false);
  const [readings, setReadings] = useState(() => buildWindow("incoming", username, initialIndex(username) + 18));

  function updateValue(index, field, value) {
    setReadings((current) => current.map((item, row) => row === index ? { ...item, [field]: Number(value) } : item));
  }

  function resetScenario(scenario) {
    setReadings(buildWindow(scenario, username, initialIndex(username) + 18));
  }

  return (
    <section className="panel wide">
      <div className="panel-heading">
        <h3>Predicción manual de administrador</h3>
        <button className="btn btn-outline-secondary btn-sm" onClick={() => setOpen((value) => !value)}>
          {open ? "Ocultar" : "Abrir prueba manual"}
        </button>
      </div>
      {open && (
        <div className="manual-grid">
          <div>
            <div className="toolbar-band compact">
              <button className="btn btn-outline-secondary btn-sm" onClick={() => resetScenario("low")}>Bajo</button>
              <button className="btn btn-outline-secondary btn-sm" onClick={() => resetScenario("stable")}>Estable</button>
              <button className="btn btn-outline-secondary btn-sm" onClick={() => resetScenario("incoming")}>Carga próxima</button>
              <button className="btn btn-primary btn-sm" disabled={loading} onClick={() => onPredict(readings)}>
                <FiActivity /> Ejecutar predicción
              </button>
            </div>
            <div className="table-responsive mt-3">
              <table className="table table-sm align-middle manual-table">
                <thead>
                  <tr>
                    <th>Minuto</th>
                    <th>kW</th>
                    <th>kVAr</th>
                    <th>Voltaje</th>
                    <th>Intensidad</th>
                  </tr>
                </thead>
                <tbody>
                  {readings.map((item, index) => (
                    <tr key={item.observed_at}>
                      <td>{formatTime(item.observed_at)}</td>
                      <td><input value={item.global_active_power} onChange={(event) => updateValue(index, "global_active_power", event.target.value)} /></td>
                      <td><input value={item.global_reactive_power} onChange={(event) => updateValue(index, "global_reactive_power", event.target.value)} /></td>
                      <td><input value={item.voltage} onChange={(event) => updateValue(index, "voltage", event.target.value)} /></td>
                      <td><input value={item.global_intensity} onChange={(event) => updateValue(index, "global_intensity", event.target.value)} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          <PredictionOutput output={output} />
        </div>
      )}
    </section>
  );
}

function PredictionOutput({ output }) {
  const rows = output ? [
    ["Probabilidad estimada de riesgo", output.probability?.toFixed(4)],
    ["Umbral de decisión", "0.30"],
    ["Clase predicha", output.class],
    ["Nivel de riesgo", output.risk_level],
    ["Horizonte de predicción", `${output.horizon_minutes} minutos`],
    ["Nodo ML que respondió", output.node_id],
    ["Latencia del clúster", `${(output.cluster_latency_ms || 0).toFixed(3)} ms`],
    ["Tiempo total de procesamiento", `${output.processing_time_ms?.toFixed(3)} ms`],
    ["Objetivo de rendimiento", `${output.performance_target_ms?.toFixed(0)} ms`],
    ["Cumplimiento del objetivo", output.target_met ? "Sí" : "No"],
    ["Versión de modelo usada", output.model_version],
    ["Cache", output.cached ? "true" : "false"],
  ] : [];
  const pipeline = output?.pipeline_timings ? [
    ["Recepción y parseo HTTP", output.pipeline_timings.receive_parse_ms],
    ["Transformación a features", output.pipeline_timings.transform_features_ms],
    ["Inferencia cache/cluster", output.pipeline_timings.inference_ms],
    ["Generación de recomendación", output.pipeline_timings.recommendation_ms],
    ["Encolado de persistencia", output.pipeline_timings.persistence_enqueue_ms],
  ] : [];
  return (
    <div className="output-panel">
      <h3>Salida del modelo</h3>
      {output ? (
        <table className="table table-sm">
          <tbody>
            {rows.map(([label, value]) => (
              <tr key={label}>
                <td>{label}</td>
                <td><strong>{value}</strong></td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="muted">Ejecuta una predicción manual para ver el desglose técnico.</p>
      )}
      {pipeline.length > 0 && (
        <>
          <h3 className="mt-3">Tiempos del proceso</h3>
          <table className="table table-sm">
            <tbody>
              {pipeline.map(([label, value]) => (
                <tr key={label}>
                  <td>{label}</td>
                  <td><strong>{Number(value || 0).toFixed(3)} ms</strong></td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

function MetricPanel({ icon: Icon, label, value }) {
  return (
    <section className="metric-panel">
      <Icon />
      <span>{label}</span>
      <strong>{value ?? "--"}</strong>
    </section>
  );
}

function Metric({ label, value }) {
  return (
    <div className="metric-inline">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function Sparkline({ data, threshold, max }) {
  const values = data.length ? data : [0];
  const high = max || Math.max(...values, threshold || 0, 1);
  const points = values.map((value, index) => {
    const x = values.length === 1 ? 0 : (index / (values.length - 1)) * 100;
    const y = 100 - (value / high) * 88;
    return `${x},${Math.max(6, Math.min(94, y))}`;
  }).join(" ");
  const thresholdY = threshold ? 100 - (threshold / high) * 88 : null;
  return (
    <svg className="sparkline" viewBox="0 0 100 100" preserveAspectRatio="none">
      {thresholdY && <line x1="0" x2="100" y1={thresholdY} y2={thresholdY} className="threshold" />}
      <polyline points={points} />
    </svg>
  );
}

function ReadingsTable({ readings }) {
  return (
    <div className="table-responsive">
      <table className="table table-sm align-middle">
        <thead>
          <tr>
            <th>Hora</th>
            <th>kW</th>
            <th>Voltaje</th>
            <th>Intensidad</th>
            <th>Submedición</th>
          </tr>
        </thead>
        <tbody>
          {readings.map((item) => (
            <tr key={item.observed_at}>
              <td>{formatTime(item.observed_at)}</td>
              <td>{item.global_active_power.toFixed(2)}</td>
              <td>{item.voltage.toFixed(1)}</td>
              <td>{item.global_intensity.toFixed(1)}</td>
              <td>{(item.sub_metering_1 + item.sub_metering_2 + item.sub_metering_3).toFixed(1)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function EventList({ events }) {
  if (!events.length) {
    return <p className="muted">Sin eventos recientes.</p>;
  }
  return (
    <div className="event-list">
      {events.map((event, index) => (
        <div key={`${event.type}-${event.at}-${index}`}>
          <span>{event.type}</span>
          <strong>{typeof event.payload === "string" ? event.payload : formatDate(event.at)}</strong>
        </div>
      ))}
    </div>
  );
}

function StatusBadge({ risk, running }) {
  return <span className={`status-badge ${risk || "idle"}`}>{risk || (running ? "ejecutando" : "en espera")}</span>;
}

function RiskPill({ risk }) {
  return <span className={`risk-pill ${risk}`}>{risk}</span>;
}

function mergeHistory(item, current) {
  if (!item?.id) return current;
  return [item, ...current.filter((entry) => entry.id !== item.id)].slice(0, 30);
}

function riskHeadline(forecast) {
  if (forecast.risk_level === "alto") return "Riesgo alto detectado";
  if (forecast.risk_level === "moderado") return "Tendencia bajo vigilancia";
  return "Consumo dentro de zona segura";
}

function decisionText(forecast) {
  if (forecast.risk_level === "alto") return "No conviene conectar otro dispositivo intensivo";
  if (forecast.risk_level === "moderado") return "Conviene esperar o escalonar cargas";
  return "Puedes mantener el consumo actual";
}

function buildImpact(report, forecast) {
  const threshold = forecast?.current_status?.threshold_kw?.toFixed(3) || report?.threshold || "1.528";
  return { threshold };
}

function formatDate(value) {
  if (!value) return "--";
  return new Date(value).toLocaleString("es-PE", { dateStyle: "short", timeStyle: "short" });
}

function formatTime(value) {
  return new Date(value).toLocaleTimeString("es-PE", { hour: "2-digit", minute: "2-digit" });
}

function formatNumber(value) {
  if (value === undefined || value === null || Number.isNaN(value)) return "--";
  return Number(value).toLocaleString("es-PE");
}

function formatPercent(value) {
  if (value === undefined || value === null) return "--";
  return `${Math.round(value * 100)}%`;
}

function formatProbability(value) {
  if (value === undefined || value === null) return "--";
  return `${(value * 100).toFixed(2)}%`;
}

function formatDuration(ms) {
  if (ms === undefined || ms === null) return "--";
  if (ms >= 60000) return `${(ms / 60000).toFixed(2)} min`;
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  return `${Number(ms).toFixed(1)} ms`;
}

createRoot(document.getElementById("root")).render(<App />);
