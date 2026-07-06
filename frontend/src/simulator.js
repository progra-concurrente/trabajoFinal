const SCENARIO_START_HOURS = {
  stable: 16,
  peak: 20,
  incoming: 20,
  low: 2,
};

export const scenarioLabels = {
  stable: "Hogar estable",
  peak: "Hora pico",
  incoming: "Carga intensiva próxima",
  low: "Bajo consumo",
};

export function scenarioDescription(scenario) {
  return {
    stable: "Consumo cotidiano con pequeñas variaciones.",
    peak: "Franja nocturna con alto riesgo histórico.",
    incoming: "Tendencia creciente antes de conectar una carga fuerte.",
    low: "Madrugada o periodo de bajo uso.",
  }[scenario];
}

export function initialIndex(username) {
  return Array.from(username || "demo").reduce((acc, char) => acc + char.charCodeAt(0), 0) % 180;
}

export function buildWindow(scenario, username, endIndex) {
  const start = endIndex - 14;
  return Array.from({ length: 15 }, (_, offset) => makeReading(scenario, username, start + offset));
}

export function makeReading(scenario, username, index) {
  const baseDate = new Date("2026-06-25T00:00:00-05:00");
  baseDate.setHours(SCENARIO_START_HOURS[scenario] ?? 20, 0, 0, 0);
  baseDate.setMinutes(baseDate.getMinutes() + index);
  const seed = initialIndex(username);
  const wave = Math.sin((index + seed) / 3) * 0.06;
  const slow = Math.sin((index + seed) / 11) * 0.04;
  let active = 0.82 + wave + slow;

  if (scenario === "low") {
    active = 0.38 + Math.abs(wave) * 0.6;
  }
  if (scenario === "stable") {
    active = 0.86 + wave + Math.max(0, Math.sin(index / 7)) * 0.12;
  }
  if (scenario === "peak") {
    active = 1.16 + Math.max(0, Math.sin(index / 5)) * 0.42 + index * 0.006;
  }
  if (scenario === "incoming") {
    active = 0.76 + Math.max(0, index % 30) * 0.033 + wave;
  }

  active = clamp(active, 0.25, 2.45);
  const reactive = clamp(active * 0.12 + Math.abs(wave) * 0.08, 0.04, 0.38);
  const voltage = clamp(240.5 - active * 2.1 + slow * 4, 232, 243);
  const intensity = clamp((active * 1000) / voltage, 1, 11);
  const kitchen = scenario === "peak" || scenario === "incoming" ? active * 7.5 : active * 2.5;
  const laundry = scenario === "incoming" ? Math.max(0, active - 1.05) * 10 : active * 1.2;
  const water = scenario === "peak" ? active * 6 : active * 2;

  return {
    observed_at: toOffsetIso(baseDate),
    global_active_power: round(active, 3),
    global_reactive_power: round(reactive, 3),
    voltage: round(voltage, 2),
    global_intensity: round(intensity, 2),
    sub_metering_1: round(kitchen, 2),
    sub_metering_2: round(laundry, 2),
    sub_metering_3: round(water, 2),
  };
}

function toOffsetIso(date) {
  const pad = (value) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:00-05:00`;
}

function round(value, digits) {
  return Number(value.toFixed(digits));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}
