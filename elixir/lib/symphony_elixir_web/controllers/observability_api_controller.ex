defmodule SymphonyElixirWeb.ObservabilityApiController do
  @moduledoc """
  JSON API for Symphony observability data.
  """

  use Phoenix.Controller, formats: [:json]

  alias Plug.Conn
  alias SymphonyElixir.Config
  alias SymphonyElixir.Linear.Webhook
  alias SymphonyElixirWeb.{Endpoint, Presenter}
  alias SymphonyElixirWeb.RawBodyReader

  @spec state(Conn.t(), map()) :: Conn.t()
  def state(conn, _params) do
    json(conn, Presenter.state_payload(orchestrator(), snapshot_timeout_ms()))
  end

  @spec issue(Conn.t(), map()) :: Conn.t()
  def issue(conn, %{"issue_identifier" => issue_identifier}) do
    case Presenter.issue_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, :issue_not_found} ->
        error_response(conn, 404, "issue_not_found", "Issue not found")
    end
  end

  @spec refresh(Conn.t(), map()) :: Conn.t()
  def refresh(conn, _params) do
    case Presenter.refresh_payload(orchestrator()) do
      {:ok, payload} ->
        conn
        |> put_status(202)
        |> json(payload)

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")
    end
  end

  @spec pause_state(Conn.t(), map()) :: Conn.t()
  def pause_state(conn, _params) do
    if local_operator_request?(conn) do
      case Presenter.pause_payload(orchestrator(), snapshot_timeout_ms()) do
        {:ok, payload} ->
          json(conn, payload)

        {:error, :timeout} ->
          error_response(conn, 503, "snapshot_timeout", "Snapshot timed out")

        {:error, :unavailable} ->
          error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")
      end
    else
      error_response(conn, 403, "operator_access_denied", "Operator controls require loopback access")
    end
  end

  @spec set_pause(Conn.t(), map()) :: Conn.t()
  def set_pause(conn, params) do
    if local_operator_request?(conn) do
      do_set_pause(conn, params)
    else
      error_response(conn, 403, "operator_access_denied", "Operator controls require loopback access")
    end
  end

  defp do_set_pause(conn, %{"paused" => paused}) when is_boolean(paused) do
    case Presenter.set_pause_payload(orchestrator(), paused) do
      {:ok, payload} ->
        conn
        |> put_status(202)
        |> json(payload)

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")

      {:error, _reason} ->
        error_response(conn, 503, "operator_control_unavailable", "Operator control is unavailable")
    end
  end

  defp do_set_pause(conn, _params) do
    error_response(conn, 422, "invalid_pause_request", "paused must be a boolean")
  end

  @spec linear_webhook(Conn.t(), map()) :: Conn.t()
  def linear_webhook(conn, params) do
    with {:ok, secret} <- configured_webhook_secret(),
         {:ok, raw_body} <- RawBodyReader.fetch_raw_body(conn) do
      verify_linear_webhook(conn, params, raw_body, secret)
    else
      {:error, :webhook_not_configured} ->
        error_response(conn, 503, "webhook_not_configured", "Webhook is not configured")

      {:error, :raw_body_unavailable} ->
        error_response(conn, 400, "invalid_webhook", "Invalid webhook")
    end
  end

  @spec method_not_allowed(Conn.t(), map()) :: Conn.t()
  def method_not_allowed(conn, _params) do
    error_response(conn, 405, "method_not_allowed", "Method not allowed")
  end

  @spec not_found(Conn.t(), map()) :: Conn.t()
  def not_found(conn, _params) do
    error_response(conn, 404, "not_found", "Route not found")
  end

  defp error_response(conn, status, code, message) do
    conn
    |> put_status(status)
    |> json(%{error: %{code: code, message: message}})
  end

  defp verify_linear_webhook(conn, params, raw_body, secret) do
    verification =
      Webhook.verify(
        raw_body,
        request_header(conn, "linear-signature"),
        request_header(conn, "linear-delivery"),
        request_header(conn, "linear-event"),
        params,
        secret,
        System.system_time(:millisecond)
      )

    case verification do
      {:ok, _event} ->
        wake_from_linear_webhook(conn)

      {:ignore, :unsupported_event} ->
        json(conn, %{accepted: true, ignored: true, reason: "unsupported_event"})

      {:error, _reason} ->
        error_response(conn, 401, "invalid_webhook", "Invalid webhook")
    end
  end

  defp wake_from_linear_webhook(conn) do
    case Presenter.refresh_payload(orchestrator()) do
      {:ok, payload} ->
        json(conn, Map.merge(payload, %{accepted: true, source: "linear_webhook"}))

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")
    end
  end

  defp configured_webhook_secret do
    case Config.settings() do
      {:ok, %{tracker: %{webhook_secret: secret}}} when is_binary(secret) and byte_size(secret) > 0 ->
        {:ok, secret}

      _other ->
        {:error, :webhook_not_configured}
    end
  end

  defp request_header(conn, name), do: conn |> Conn.get_req_header(name) |> List.first()

  defp local_operator_request?(%Conn{remote_ip: {127, _b, _c, _d}}), do: true
  defp local_operator_request?(%Conn{remote_ip: {0, 0, 0, 0, 0, 0, 0, 1}}), do: true
  defp local_operator_request?(_conn), do: false

  defp orchestrator do
    Endpoint.config(:orchestrator) || SymphonyElixir.Orchestrator
  end

  defp snapshot_timeout_ms do
    Endpoint.config(:snapshot_timeout_ms) || 15_000
  end
end
