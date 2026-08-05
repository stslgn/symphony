defmodule SymphonyElixirWeb.DashboardLive do
  @moduledoc """
  Live observability dashboard for Symphony.
  """

  use Phoenix.LiveView, layout: {SymphonyElixirWeb.Layouts, :app}

  alias SymphonyElixir.RateLimitTelemetry
  alias SymphonyElixirWeb.{Endpoint, ObservabilityPubSub, Presenter}
  @runtime_tick_ms 1_000

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:payload, load_payload())
      |> assign(:now, DateTime.utc_now())

    if connected?(socket) do
      :ok = ObservabilityPubSub.subscribe()
      schedule_runtime_tick()
    end

    {:ok, socket}
  end

  @impl true
  def handle_info(:runtime_tick, socket) do
    schedule_runtime_tick()
    {:noreply, assign(socket, :now, DateTime.utc_now())}
  end

  @impl true
  def handle_info(:observability_updated, socket) do
    {:noreply,
     socket
     |> assign(:payload, load_payload())
     |> assign(:now, DateTime.utc_now())}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <section class="dashboard-shell">
      <header class="hero-card">
        <div class="hero-grid">
          <div>
            <p class="eyebrow">
              Symphony Observability
            </p>
            <h1 class="hero-title">
              Operations Dashboard
            </h1>
            <p class="hero-copy">
              Current state, retry pressure, token usage, and orchestration health for the active Symphony runtime.
            </p>
          </div>

          <div class="status-stack">
            <span class="status-badge status-badge-live">
              <span class="status-badge-dot"></span>
              Live
            </span>
            <span class="status-badge status-badge-offline">
              <span class="status-badge-dot"></span>
              Offline
            </span>
          </div>
        </div>
      </header>

      <%= if @payload[:error] do %>
        <section class="error-card">
          <h2 class="error-title">
            Snapshot unavailable
          </h2>
          <p class="error-copy">
            <strong><%= @payload.error.code %>:</strong> <%= @payload.error.message %>
          </p>
        </section>
      <% else %>
        <section class="metric-grid">
          <article class="metric-card">
            <p class="metric-label">Running</p>
            <p class="metric-value numeric"><%= @payload.counts.running %></p>
            <p class="metric-detail">Active issue sessions in the current runtime.</p>
          </article>

          <article class="metric-card">
            <p class="metric-label">Retrying</p>
            <p class="metric-value numeric"><%= @payload.counts.retrying %></p>
            <p class="metric-detail">Issues waiting for the next retry window.</p>
          </article>

          <article class="metric-card">
            <p class="metric-label">Parked</p>
            <p class="metric-value numeric"><%= @payload.counts.parked %></p>
            <p class="metric-detail">Unresolved waits that require an operator decision.</p>
          </article>

          <article class="metric-card">
            <p class="metric-label">Cleanup pending</p>
            <p class="metric-value numeric"><%= @payload.counts.cleanup_pending %></p>
            <p class="metric-detail">Workspace cleanup owners still holding durable claims.</p>
          </article>

          <article class="metric-card">
            <p class="metric-label">Total tokens</p>
            <p class="metric-value numeric"><%= format_int(@payload.codex_totals.total_tokens) %></p>
            <p class="metric-detail numeric">
              In <%= format_int(@payload.codex_totals.input_tokens) %> / Out <%= format_int(@payload.codex_totals.output_tokens) %>
            </p>
          </article>

          <article class="metric-card">
            <p class="metric-label">Runtime</p>
            <p class="metric-value numeric"><%= format_runtime_seconds(total_runtime_seconds(@payload, @now)) %></p>
            <p class="metric-detail">Total Codex runtime across completed and active sessions.</p>
          </article>
        </section>

        <section class="section-card" id="cleanup-pending">
          <div class="section-header">
            <div>
              <h2 class="section-title">Workspace cleanup pending</h2>
              <p class="section-copy">Durably owned cleanups that must finish before the issue claim is released.</p>
            </div>
          </div>

          <%= if @payload.cleanup_pending == [] do %>
            <p class="empty-state">No workspace cleanups are pending.</p>
          <% else %>
            <div class="table-wrap">
              <table class="data-table" style="min-width: 980px; table-layout: fixed;">
                <thead>
                  <tr>
                    <th>Issue</th>
                    <th>Stage</th>
                    <th>Run / attempt</th>
                    <th>Error code</th>
                    <th>Worker host</th>
                    <th>Canonical workspace path</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @payload.cleanup_pending}>
                    <td>
                      <div class="issue-stack">
                        <span class="issue-id"><%= entry.issue_identifier %></span>
                        <a class="issue-link" href={"/api/v1/#{entry.issue_identifier}"}>JSON details</a>
                      </div>
                    </td>
                    <td><span class="state-badge state-badge-warning"><%= entry.stage %></span></td>
                    <td>
                      <div class="detail-stack mono">
                        <span class="bounded-value" title={entry.run_id || "n/a"}><%= entry.run_id || "n/a" %></span>
                        <span class="muted">attempt <%= entry.attempt %></span>
                      </div>
                    </td>
                    <td><%= entry.error_code %></td>
                    <td class="mono"><%= entry.worker_host || "local" %></td>
                    <td>
                      <span class="mono bounded-value" title={entry.workspace_path || "missing"}>
                        <%= entry.workspace_path || "missing" %>
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>

        <section class="section-card">
          <div class="section-header">
            <div>
              <h2 class="section-title">Rate limits</h2>
              <p class="section-copy">Latest upstream rate-limit snapshot, when available.</p>
            </div>
          </div>

          <pre class="code-panel"><%= format_rate_limits(@payload.rate_limits) %></pre>
        </section>

        <section class="section-card">
          <div class="section-header">
            <div>
              <h2 class="section-title">Running sessions</h2>
              <p class="section-copy">Active issues, last known agent activity, and token usage.</p>
            </div>
          </div>

          <%= if @payload.running == [] do %>
            <p class="empty-state">
              No active sessions.<%= if @payload.counts.parked > 0 do %> Unresolved parked waits are listed separately below.<% end %>
            </p>
          <% else %>
            <div class="table-wrap">
              <table class="data-table data-table-running">
                <colgroup>
                  <col style="width: 12rem;" />
                  <col style="width: 8rem;" />
                  <col style="width: 7.5rem;" />
                  <col style="width: 8.5rem;" />
                  <col />
                  <col style="width: 10rem;" />
                </colgroup>
                <thead>
                  <tr>
                    <th>Issue</th>
                    <th>State</th>
                    <th>Session</th>
                    <th>Runtime / turns</th>
                    <th>Codex update</th>
                    <th>Tokens</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @payload.running}>
                    <td>
                      <div class="issue-stack">
                        <span class="issue-id"><%= entry.issue_identifier %></span>
                        <a class="issue-link" href={"/api/v1/#{entry.issue_identifier}"}>JSON details</a>
                      </div>
                    </td>
                    <td>
                      <span class={state_badge_class(entry.state)}>
                        <%= entry.state %>
                      </span>
                    </td>
                    <td>
                      <div class="session-stack">
                        <%= if entry.session_id do %>
                          <button
                            type="button"
                            class="subtle-button"
                            data-label="Copy ID"
                            data-copy={entry.session_id}
                            onclick="navigator.clipboard.writeText(this.dataset.copy); this.textContent = 'Copied'; clearTimeout(this._copyTimer); this._copyTimer = setTimeout(() => { this.textContent = this.dataset.label }, 1200);"
                          >
                            Copy ID
                          </button>
                        <% else %>
                          <span class="muted">n/a</span>
                        <% end %>
                      </div>
                    </td>
                    <td class="numeric"><%= format_runtime_and_turns(entry.started_at, entry.turn_count, @now) %></td>
                    <td>
                      <div class="detail-stack">
                        <span
                          class="event-text"
                          title={to_string(entry.last_event || "n/a")}
                        ><%= entry.last_event || "n/a" %></span>
                        <span class="muted event-meta">
                          <%= entry.last_event || "n/a" %>
                          <%= if entry.last_event_at do %>
                            · <span class="mono numeric"><%= entry.last_event_at %></span>
                          <% end %>
                        </span>
                      </div>
                    </td>
                    <td>
                      <div class="token-stack numeric">
                        <span>Total: <%= format_int(entry.tokens.total_tokens) %></span>
                        <span class="muted">In <%= format_int(entry.tokens.input_tokens) %> / Out <%= format_int(entry.tokens.output_tokens) %></span>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>

        <section class="section-card" id="parked-waits">
          <div class="section-header">
            <div>
              <h2 class="section-title">Parked waits</h2>
              <p class="section-copy">Unresolved issues waiting for an allowlisted operator action.</p>
            </div>
          </div>

          <%= if @payload.parked == [] do %>
            <p class="empty-state">No unresolved operator waits.</p>
          <% else %>
            <p :if={@payload.parked_meta.truncated} class="empty-state">
              Showing <%= @payload.parked_meta.returned_count %> of <%= @payload.parked_meta.total_count %> parked waits; <%= @payload.parked_meta.omitted_count %> omitted by the bounded projection.
            </p>
            <div class="table-wrap">
              <table class="data-table data-table-parked">
                <thead>
                  <tr>
                    <th>Issue / wait</th>
                    <th>Reason / actions</th>
                    <th>Run / attempt</th>
                    <th>Terminal reason</th>
                    <th>Worker host</th>
                    <th>Canonical workspace path</th>
                    <th>Parked at</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @payload.parked} id={"parked-wait-#{entry.wait_id}"}>
                    <td>
                      <div class="issue-stack">
                        <span class="issue-id"><%= entry.issue_identifier %></span>
                        <span class="mono bounded-value" title={entry.wait_id}><%= entry.wait_id %></span>
                        <a class="issue-link" href={"/api/v1/#{entry.issue_identifier}"}>JSON details</a>
                      </div>
                    </td>
                    <td>
                      <div class="detail-stack">
                        <span><%= entry.reason %></span>
                        <span class="muted"><%= Enum.join(entry.allowed_actions, ", ") %></span>
                      </div>
                    </td>
                    <td>
                      <div class="detail-stack mono">
                        <span class="bounded-value" title={entry.run_id}><%= entry.run_id %></span>
                        <span class="muted">attempt <%= entry.attempt %></span>
                      </div>
                    </td>
                    <td><%= entry.terminal_reason || "n/a" %></td>
                    <td class="mono"><%= entry.worker_host || "n/a" %></td>
                    <td>
                      <span class="mono bounded-value" title={entry.workspace_path || "n/a"}>
                        <%= entry.workspace_path || "n/a" %>
                      </span>
                      <span :if={"workspace_path" in entry.truncated_fields} class="muted">
                        display truncated
                      </span>
                    </td>
                    <td class="mono"><%= entry.parked_at || "n/a" %></td>
                  </tr>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>

        <section class="section-card">
          <div class="section-header">
            <div>
              <h2 class="section-title">Retry queue</h2>
              <p class="section-copy">Issues waiting for the next retry window.</p>
            </div>
          </div>

          <%= if @payload.retrying == [] do %>
            <p class="empty-state">No issues are currently backing off.</p>
          <% else %>
            <div class="table-wrap">
              <table class="data-table" style="min-width: 680px;">
                <thead>
                  <tr>
                    <th>Issue</th>
                    <th>Attempt</th>
                    <th>Due at</th>
                    <th>Error code</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @payload.retrying}>
                    <td>
                      <div class="issue-stack">
                        <span class="issue-id"><%= entry.issue_identifier %></span>
                        <a class="issue-link" href={"/api/v1/#{entry.issue_identifier}"}>JSON details</a>
                      </div>
                    </td>
                    <td><%= entry.attempt %></td>
                    <td class="mono"><%= entry.due_at || "n/a" %></td>
                    <td><%= entry.error_code || "n/a" %></td>
                  </tr>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>
      <% end %>
    </section>
    """
  end

  defp load_payload do
    Presenter.state_payload(orchestrator(), snapshot_timeout_ms())
  end

  defp orchestrator do
    Endpoint.config(:orchestrator) || SymphonyElixir.Orchestrator
  end

  defp snapshot_timeout_ms do
    Endpoint.config(:snapshot_timeout_ms) || 15_000
  end

  defp completed_runtime_seconds(payload) do
    payload.codex_totals.seconds_running || 0
  end

  defp total_runtime_seconds(payload, now) do
    completed_runtime_seconds(payload) +
      Enum.reduce(payload.running, 0, fn entry, total ->
        total + runtime_seconds_from_started_at(entry.started_at, now)
      end)
  end

  defp format_runtime_and_turns(started_at, turn_count, now) when is_integer(turn_count) and turn_count > 0 do
    "#{format_runtime_seconds(runtime_seconds_from_started_at(started_at, now))} / #{turn_count}"
  end

  defp format_runtime_and_turns(started_at, _turn_count, now),
    do: format_runtime_seconds(runtime_seconds_from_started_at(started_at, now))

  defp format_runtime_seconds(seconds) when is_number(seconds) do
    whole_seconds = max(trunc(seconds), 0)
    mins = div(whole_seconds, 60)
    secs = rem(whole_seconds, 60)
    "#{mins}m #{secs}s"
  end

  defp runtime_seconds_from_started_at(%DateTime{} = started_at, %DateTime{} = now) do
    DateTime.diff(now, started_at, :second)
  end

  defp runtime_seconds_from_started_at(started_at, %DateTime{} = now) when is_binary(started_at) do
    case DateTime.from_iso8601(started_at) do
      {:ok, parsed, _offset} -> runtime_seconds_from_started_at(parsed, now)
      _ -> 0
    end
  end

  defp runtime_seconds_from_started_at(_started_at, _now), do: 0

  defp format_int(value) when is_integer(value) do
    value
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/.{3}(?=.)/, "\\0,")
    |> String.reverse()
  end

  defp format_int(_value), do: "n/a"

  defp state_badge_class(state) do
    base = "state-badge"
    normalized = state |> to_string() |> String.downcase()

    cond do
      String.contains?(normalized, ["progress", "running", "active"]) -> "#{base} state-badge-active"
      String.contains?(normalized, ["blocked", "error", "failed"]) -> "#{base} state-badge-danger"
      String.contains?(normalized, ["todo", "queued", "pending", "retry"]) -> "#{base} state-badge-warning"
      true -> base
    end
  end

  defp schedule_runtime_tick do
    Process.send_after(self(), :runtime_tick, @runtime_tick_ms)
  end

  defp format_rate_limits(rate_limits) do
    case RateLimitTelemetry.project(rate_limits) do
      %{limit_id: limit_id} = projected ->
        [
          "limit_id: #{limit_id}",
          format_rate_limit_bucket("primary", Map.get(projected, :primary)),
          format_rate_limit_bucket("secondary", Map.get(projected, :secondary)),
          format_rate_limit_credits(Map.get(projected, :credits))
        ]
        |> Enum.reject(&is_nil/1)
        |> Enum.join("\n")

      nil ->
        "n/a"
    end
  end

  defp format_rate_limit_bucket(_label, nil), do: nil

  defp format_rate_limit_bucket(label, bucket) when is_map(bucket) do
    details =
      []
      |> append_rate_limit_integer("remaining", Map.get(bucket, :remaining))
      |> append_rate_limit_integer("limit", Map.get(bucket, :limit))
      |> append_rate_limit_number("used_percent", Map.get(bucket, :used_percent))
      |> append_rate_limit_integer("window_duration_mins", Map.get(bucket, :window_duration_mins))
      |> append_rate_limit_integer("reset_in_seconds", Map.get(bucket, :reset_in_seconds))
      |> append_rate_limit_reset_at(Map.get(bucket, :reset_at))

    "#{label}: #{Enum.join(details, ", ")}"
  end

  defp format_rate_limit_credits(nil), do: nil

  defp format_rate_limit_credits(credits) when is_map(credits) do
    details =
      []
      |> append_rate_limit_boolean("has_credits", Map.get(credits, :has_credits))
      |> append_rate_limit_boolean("unlimited", Map.get(credits, :unlimited))
      |> append_rate_limit_number("balance", Map.get(credits, :balance))

    "credits: #{Enum.join(details, ", ")}"
  end

  defp append_rate_limit_integer(parts, label, value) when is_integer(value),
    do: parts ++ ["#{label}=#{Integer.to_string(value)}"]

  defp append_rate_limit_integer(parts, _label, _value), do: parts

  defp append_rate_limit_number(parts, label, value) when is_integer(value),
    do: parts ++ ["#{label}=#{Integer.to_string(value)}"]

  defp append_rate_limit_number(parts, label, value) when is_float(value),
    do: parts ++ ["#{label}=#{:erlang.float_to_binary(value, [:compact, decimals: 2])}"]

  defp append_rate_limit_number(parts, _label, _value), do: parts

  defp append_rate_limit_boolean(parts, label, value) when is_boolean(value),
    do: parts ++ ["#{label}=#{if(value, do: "true", else: "false")}"]

  defp append_rate_limit_boolean(parts, _label, _value), do: parts

  defp append_rate_limit_reset_at(parts, value) when is_integer(value),
    do: parts ++ ["reset_at=#{Integer.to_string(value)}"]

  defp append_rate_limit_reset_at(parts, value) when is_binary(value),
    do: parts ++ ["reset_at=#{value}"]

  defp append_rate_limit_reset_at(parts, _value), do: parts
end
