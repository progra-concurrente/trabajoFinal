param([int]$Requests = 100, [string]$BaseUrl = "http://localhost:8080")

$login = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/auth/login" `
  -ContentType "application/json" -Body '{"username":"admin","password":"powersight"}'
$headers = @{ Authorization = "Bearer $($login.access_token)" }

$start = [datetimeoffset]"2026-06-25T20:46:00-05:00"
$readings = for ($i = 0; $i -lt 15; $i++) {
  @{
    observed_at = $start.AddMinutes($i).ToString("o")
    global_active_power = 0.82 + 0.04 * $i
    global_reactive_power = 0.12 + 0.005 * $i
    voltage = 239.8 - 0.2 * $i
    global_intensity = 3.5 + 0.2 * $i
    sub_metering_1 = 0
    sub_metering_2 = 1
    sub_metering_3 = 2 + [math]::Floor($i / 4)
  }
}
$body = @{ readings = $readings } | ConvertTo-Json -Depth 5
$times = @()
for ($i = 0; $i -lt $Requests; $i++) {
  $watch = [Diagnostics.Stopwatch]::StartNew()
  Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/forecasts" `
    -Headers $headers -ContentType "application/json" -Body $body | Out-Null
  $watch.Stop()
  $times += $watch.Elapsed.TotalMilliseconds
}
$sorted = $times | Sort-Object
$under100 = ($times | Where-Object { $_ -lt 100 }).Count
[pscustomobject]@{
  Requests = $Requests
  MedianMs = [math]::Round($sorted[[math]::Floor($Requests * .50)], 2)
  P95Ms = [math]::Round($sorted[[math]::Min($Requests - 1, [math]::Floor($Requests * .95))], 2)
  CompliancePercent = [math]::Round($under100 * 100 / $Requests, 2)
}
