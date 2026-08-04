defmodule SymphonyElixir.Codex.CapabilityPolicy do
  @moduledoc """
  Fail-closed capability checks for Symphony-mediated Codex tool interactions.
  """

  @type t :: %__MODULE__{}

  defstruct dynamic_tools: MapSet.new(),
            mcp_tools: MapSet.new(),
            mcp_elicitation_servers: MapSet.new()

  @spec new(map()) :: t()
  def new(settings) when is_map(settings) do
    %__MODULE__{
      dynamic_tools: normalized_set(Map.get(settings, :dynamic_tool_allowlist, [])),
      mcp_tools: normalized_set(Map.get(settings, :mcp_tool_auto_approve_allowlist, [])),
      mcp_elicitation_servers: normalized_set(Map.get(settings, :mcp_elicitation_auto_approve_allowlist, []))
    }
  end

  @spec dynamic_tool_allowed?(t(), term()) :: boolean()
  def dynamic_tool_allowed?(%__MODULE__{} = policy, tool) when is_binary(tool) do
    MapSet.member?(policy.dynamic_tools, normalize(tool))
  end

  def dynamic_tool_allowed?(_policy, _tool), do: false

  @spec mcp_tool_allowed?(t(), term(), term()) :: boolean()
  def mcp_tool_allowed?(%__MODULE__{} = policy, server, tool)
      when is_binary(server) and is_binary(tool) do
    MapSet.member?(policy.mcp_tools, normalize("#{server}/#{tool}"))
  end

  def mcp_tool_allowed?(_policy, _server, _tool), do: false

  @spec mcp_elicitation_allowed?(t(), term()) :: boolean()
  def mcp_elicitation_allowed?(%__MODULE__{} = policy, server) when is_binary(server) do
    MapSet.member?(policy.mcp_elicitation_servers, normalize(server))
  end

  def mcp_elicitation_allowed?(_policy, _server), do: false

  defp normalized_set(values) when is_list(values) do
    values
    |> Enum.filter(&is_binary/1)
    |> Enum.map(&normalize/1)
    |> MapSet.new()
  end

  defp normalized_set(_values), do: MapSet.new()

  defp normalize(value), do: value |> String.trim() |> String.downcase()
end
