defmodule SymphonyElixir.CapabilityPolicyTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Codex.CapabilityPolicy

  test "normalizes exact dynamic and MCP allowlist identities" do
    policy =
      CapabilityPolicy.new(%{
        dynamic_tool_allowlist: [" linear_graphql "],
        mcp_tool_auto_approve_allowlist: ["Linear/Save issue"],
        mcp_elicitation_auto_approve_allowlist: ["Linear"]
      })

    assert CapabilityPolicy.dynamic_tool_allowed?(policy, "LINEAR_GRAPHQL")
    refute CapabilityPolicy.dynamic_tool_allowed?(policy, "other_tool")
    refute CapabilityPolicy.dynamic_tool_allowed?(policy, nil)
    refute CapabilityPolicy.dynamic_tool_allowed?(nil, "linear_graphql")

    assert CapabilityPolicy.mcp_tool_allowed?(policy, "linear", "save ISSUE")
    refute CapabilityPolicy.mcp_tool_allowed?(policy, "linear", "delete issue")
    refute CapabilityPolicy.mcp_tool_allowed?(policy, nil, "save issue")
    refute CapabilityPolicy.mcp_tool_allowed?(nil, "linear", "save issue")

    assert CapabilityPolicy.mcp_elicitation_allowed?(policy, "linear")
    refute CapabilityPolicy.mcp_elicitation_allowed?(policy, "github")
    refute CapabilityPolicy.mcp_elicitation_allowed?(policy, nil)
    refute CapabilityPolicy.mcp_elicitation_allowed?(nil, "linear")
  end

  test "malformed policy inputs fail closed" do
    policy =
      CapabilityPolicy.new(%{
        dynamic_tool_allowlist: :all,
        mcp_tool_auto_approve_allowlist: [nil],
        mcp_elicitation_auto_approve_allowlist: nil
      })

    refute CapabilityPolicy.dynamic_tool_allowed?(policy, "linear_graphql")
    refute CapabilityPolicy.mcp_tool_allowed?(policy, "linear", "Save issue")
    refute CapabilityPolicy.mcp_elicitation_allowed?(policy, "linear")
  end
end
